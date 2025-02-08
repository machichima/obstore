"""Integration with the [fsspec] library.

[fsspec]: https://github.com/fsspec/filesystem_spec

The fsspec integration is **best effort** and not the primary API of `obstore`. This integration may not be as stable and may not provide the same performance as the rest of the library. Changes may be made even in patch releases to align better with fsspec expectations. If you find any bugs, please [file an issue](https://github.com/developmentseed/obstore/issues/new/choose).

The underlying `object_store` Rust crate [cautions](https://docs.rs/object_store/latest/object_store/#why-not-a-filesystem-interface) against relying too strongly on stateful filesystem representations of object stores:

> The ObjectStore interface is designed to mirror the APIs of object stores and not filesystems, and thus has stateless APIs instead of cursor based interfaces such as Read or Seek available in filesystems.
>
> This design provides the following advantages:
>
> - All operations are atomic, and readers cannot observe partial and/or failed writes
> - Methods map directly to object store APIs, providing both efficiency and predictability
> - Abstracts away filesystem and operating system specific quirks, ensuring portability
> - Allows for functionality not native to filesystems, such as operation preconditions and atomic multipart uploads

Where possible, implementations should use the underlying `obstore` APIs
directly. Only where this is not possible should users fall back to this fsspec
integration.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from functools import lru_cache
from typing import (
    TYPE_CHECKING,
    Any,
    Coroutine,
    Dict,
    List,
    Tuple,
)
from urllib.parse import urlparse

import fsspec.asyn
import fsspec.spec

import obstore as obs
from obstore import Bytes
from obstore.store import from_url

if TYPE_CHECKING:
    from obstore.store import (
        AzureConfig,
        AzureConfigInput,
        ClientConfig,
        GCSConfig,
        GCSConfigInput,
        RetryConfig,
        S3Config,
        S3ConfigInput,
    )


class AsyncFsspecStore(fsspec.asyn.AsyncFileSystem):
    """An fsspec implementation based on a obstore Store.

    You should be able to pass an instance of this class into any API that expects an
    fsspec-style object.
    """

    cachable = False
    config: (
        S3Config
        | S3ConfigInput
        | GCSConfig
        | GCSConfigInput
        | AzureConfig
        | AzureConfigInput
        | None
    )
    client_options: ClientConfig | None
    retry_config: RetryConfig | None

    def __init__(
        self,
        *args,
        config: S3Config
        | S3ConfigInput
        | GCSConfig
        | GCSConfigInput
        | AzureConfig
        | AzureConfigInput
        | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        asynchronous: bool = False,
        loop: Any = None,
        batch_size: int | None = None,
    ):
        """Construct a new AsyncFsspecStore

        Args:
            store: a configured instance of one of the store classes in `obstore.store`.
            asynchronous: Set to `True` if this instance is meant to be be called using
                the fsspec async API. This should only be set to true when running
                within a coroutine.
            loop: since both fsspec/python and tokio/rust may be using loops, this should
                be kept `None` for now, and will not be used.
            batch_size: some operations on many files will batch their requests; if you
                are seeing timeouts, you may want to set this number smaller than the
                defaults, which are determined in `fsspec.asyn._get_batch_size`.

        Example:

        ```py
        from obstore.fsspec import AsyncFsspecStore
        from obstore.store import HTTPStore

        store = HTTPStore.from_url("https://example.com")
        fsspec_store = AsyncFsspecStore(store)
        resp = fsspec_store.cat("/")
        assert resp.startswith(b"<!doctype html>")
        ```
        """

        self.config = config
        self.client_options = client_options
        self.retry_config = retry_config

        super().__init__(
            *args, asynchronous=asynchronous, loop=loop, batch_size=batch_size
        )

    def _split_path(self, path: str) -> Tuple[str, str]:
        """
        Split bucket and file path

        Args:
            path  (str): Input path, like `s3://mybucket/path/to/file`

        Examples:
            >>> split_path("s3://mybucket/path/to/file")
            ['mybucket', 'path/to/file']
        """

        protocol_with_bucket = ["s3", "s3a", "gcs", "gs", "abfs"]

        if not self.protocol in protocol_with_bucket:
            # no bucket name in path
            return "", path

        res = urlparse(path)
        if res.scheme:
            if res.scheme != self.protocol:
                raise ValueError(
                    f"Expect protocol to be {self.protocol}. Got {res.scheme}"
                )
            path = res.netloc + res.path

        if "/" not in path:
            return path, ""
        else:
            path_li = path.split("/")
            bucket = path_li[0]
            file_path = "/".join(path_li[1:])
            return (bucket, file_path)

    @lru_cache(maxsize=10)
    def _construct_store(self, bucket: str):
        return from_url(
            url=f"{self.protocol}://{bucket}",
            config=self.config,
            client_options=self.client_options,
            retry_config=self.retry_config if self.retry_config else None,
        )

    async def _rm_file(self, path, **kwargs):
        bucket, path = self._split_path(path)
        store = self._construct_store(bucket)
        return await obs.delete_async(store, path)

    async def _cp_file(self, path1, path2, **kwargs):
        bucket1, path1 = self._split_path(path1)
        bucket2, path2 = self._split_path(path2)

        if bucket1 != bucket2:
            raise ValueError(
                f"Bucket mismatch: Source bucket '{bucket1}' and destination bucket '{bucket2}' must be the same."
            )

        store = self._construct_store(bucket1)
        return await obs.copy_async(store, path1, path2)

    async def _pipe_file(self, path, value, mode="overwrite", **kwargs):
        bucket, path = self._split_path(path)
        store = self._construct_store(bucket)
        return await obs.put_async(store, path, value)

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        bucket, path = self._split_path(path)
        store = self._construct_store(bucket)

        if start is None and end is None:
            resp = await obs.get_async(store, path)
            return (await resp.bytes_async()).to_bytes()

        range_bytes = await obs.get_range_async(store, path, start=start, end=end)
        return range_bytes.to_bytes()

    async def _cat_ranges(
        self,
        paths: List[str],
        starts: List[int] | int,
        ends: List[int] | int,
        max_gap=None,
        batch_size=None,
        on_error="return",
        **kwargs,
    ):
        if isinstance(starts, int):
            starts = [starts] * len(paths)
        if isinstance(ends, int):
            ends = [ends] * len(paths)
        if not len(paths) == len(starts) == len(ends):
            raise ValueError

        per_file_requests: Dict[str, List[Tuple[int, int, int]]] = defaultdict(list)
        for idx, (path, start, end) in enumerate(zip(paths, starts, ends)):
            per_file_requests[path].append((start, end, idx))

        futs: List[Coroutine[Any, Any, List[Bytes]]] = []
        for path, ranges in per_file_requests.items():
            bucket, path = self._split_path(path)
            store = self._construct_store(bucket)

            offsets = [r[0] for r in ranges]
            ends = [r[1] for r in ranges]
            fut = obs.get_ranges_async(store, path, starts=offsets, ends=ends)
            futs.append(fut)

        result = await asyncio.gather(*futs)

        output_buffers: List[bytes] = [b""] * len(paths)
        for per_file_request, buffers in zip(per_file_requests.items(), result):
            path, ranges = per_file_request
            for buffer, ranges_ in zip(buffers, ranges):
                initial_index = ranges_[2]
                output_buffers[initial_index] = buffer.to_bytes()

        return output_buffers

    async def _put_file(self, lpath, rpath, **kwargs):
        lbucket, lpath = self._split_path(lpath)
        rbucket, rpath = self._split_path(rpath)

        if lbucket != rbucket:
            raise ValueError(
                f"Bucket mismatch: Source bucket '{lbucket}' and destination bucket '{rbucket}' must be the same."
            )

        store = self._construct_store(lbucket)

        with open(lpath, "rb") as f:
            await obs.put_async(store, rpath, f)

    async def _get_file(self, rpath, lpath, **kwargs):
        lbucket, lpath = self._split_path(lpath)
        rbucket, rpath = self._split_path(rpath)

        if lbucket != rbucket:
            raise ValueError(
                f"Bucket mismatch: Source bucket '{lbucket}' and destination bucket '{rbucket}' must be the same."
            )

        store = self._construct_store(lbucket)

        with open(lpath, "wb") as f:
            resp = await obs.get_async(store, rpath)
            async for buffer in resp.stream():
                f.write(buffer)

    async def _info(self, path, **kwargs):
        bucket, path = self._split_path(path)
        store = self._construct_store(bucket)

        head = await obs.head_async(store, path)
        return {
            # Required of `info`: (?)
            "name": head["path"],
            "size": head["size"],
            "type": "directory" if head["path"].endswith("/") else "file",
            # Implementation-specific keys
            "e_tag": head["e_tag"],
            "last_modified": head["last_modified"],
            "version": head["version"],
        }

    async def _ls(self, path, detail=True, **kwargs):
        bucket, path = self._split_path(path)
        store = self._construct_store(bucket)

        result = await obs.list_with_delimiter_async(store, path)
        objects = result["objects"]
        prefs = result["common_prefixes"]
        if detail:
            return [
                {
                    "name": object["path"],
                    "size": object["size"],
                    "type": "file",
                    "e_tag": object["e_tag"],
                }
                for object in objects
            ] + [{"name": object, "size": 0, "type": "directory"} for object in prefs]
        else:
            return sorted([object["path"] for object in objects] + prefs)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        """Return raw bytes-mode file-like from the file-system"""

        return BufferedFileSimple(self, path, mode, **kwargs)


class BufferedFileSimple(fsspec.spec.AbstractBufferedFile):
    def __init__(self, fs, path, mode="rb", **kwargs):
        if mode != "rb":
            raise ValueError("Only 'rb' mode is currently supported")
        super().__init__(fs, path, mode, **kwargs)

    def read(self, length: int = -1):
        """Return bytes from the remote file

        Args:
            length: if positive, returns up to this many bytes; if negative, return all
                remaining byets.
        """
        if length < 0:
            data = self.fs.cat_file(self.path, self.loc, self.size)
            self.loc = self.size
        else:
            data = self.fs.cat_file(self.path, self.loc, self.loc + length)
            self.loc += length
        return data


class S3FsspecStore(AsyncFsspecStore):
    protocol = "s3"


class GCSFsspecStore(AsyncFsspecStore):
    protocol = "gs"


class AzureFsspecStore(AsyncFsspecStore):
    protocol = "abfs"
