"""Integration with the [fsspec] library.

This integration is designed for compatibility and may not provide the same performance
as other obstore APIs.

[fsspec]: https://github.com/fsspec/filesystem_spec

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
from typing import Any, Coroutine, Dict, List, Tuple

import fsspec.asyn
import fsspec.spec

import obstore as obs
from obstore import open_reader, open_writer


class AsyncFsspecStore(fsspec.asyn.AsyncFileSystem):
    """An fsspec implementation based on a obstore Store.

    You should be able to pass an instance of this class into any API that expects an
    fsspec-style object.
    """

    cachable = False

    def __init__(
        self,
        store: obs.store.ObjectStore,
        *args,
        asynchronous: bool = False,
        loop=None,
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

        self.store = store
        super().__init__(
            *args, asynchronous=asynchronous, loop=loop, batch_size=batch_size
        )

    async def _rm_file(self, path, **kwargs):
        return await obs.delete_async(self.store, path)

    async def _cp_file(self, path1, path2, **kwargs):
        return await obs.copy_async(self.store, path1, path2)

    async def _pipe_file(self, path, value, **kwargs):
        return await obs.put_async(self.store, path, value)

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        if start is None and end is None:
            resp = await obs.get_async(self.store, path)
            return await resp.bytes_async()

        range_bytes = await obs.get_range_async(self.store, path, start=start, end=end)
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

        futs: List[Coroutine[Any, Any, List[bytes]]] = []
        for path, ranges in per_file_requests.items():
            offsets = [r[0] for r in ranges]
            ends = [r[1] for r in ranges]
            fut = obs.get_ranges_async(self.store, path, starts=offsets, ends=ends)
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
        with open(lpath, "rb") as f:
            await obs.put_async(self.store, rpath, f)

    async def _get_file(self, rpath, lpath, **kwargs):
        with open(lpath, "wb") as f:
            resp = await obs.get_async(self.store, rpath)
            async for buffer in resp.stream():
                f.write(buffer)

    async def _info(self, path, **kwargs):
        try:
            head = await obs.head_async(self.store, path)
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
        except FileNotFoundError:
            # try ls, refer to the info implementation in fsspec
            # https://github.com/fsspec/filesystem_spec/blob/08d1e494db177d90ccc77e5f154d5fbb34657b13/fsspec/spec.py#L643-L675
            parent = self._parent(path)
            out = await self._ls(parent)
            out = [o for o in out if o["name"].rstrip("/") == path]
            if out:
                return out[0]
            out = await self._ls(path)
            out1 = [o for o in out if o["name"].rstrip("/") == path]
            if len(out1) == 1:
                if "size" not in out1[0]:
                    out1[0]["size"] = None
                return out1[0]
            elif len(out1) > 1 or out:
                return {"name": path, "size": 0, "type": "directory"}
            else:
                raise FileNotFoundError(path)

    async def _ls(self, path, detail=True, **kwargs):
        result = await obs.list_with_delimiter_async(self.store, path)
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

    def _open(self, path, mode="rb", **kwargs):
        """Return raw bytes-mode file-like from the file-system"""
        assert mode in ("rb", "wb"), f"Only 'rb' and 'wb' mode is currently supported, got: {mode}"
        if mode == "wb":
            return BufferedFileWrite(self, path, mode, **kwargs)
        if mode == "rb":
            return BufferedFileRead(self, path, mode, **kwargs)


class BufferedFileWrite(fsspec.spec.AbstractBufferedFile):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._writer = open_writer(self.fs.store, self.path)

    def _initiate_upload(self):
        """
        Called by AbstractBufferedFile flusH() on the first flush
        """
        self._writer = open_writer(self.fs.store, self.path)

    def _upload_chunk(self, final=False):
        """
        Called every time fsspec flushes the write buffer
        """
        if self.buffer and len(self.buffer.getbuffer()) > 0:
            self.buffer.seek(0)
            self._writer.write(self.buffer.read())
            # flush all the data in buffer when closing
            if final:
                self._writer.flush()
            return True
        else:
            return False

    def close(self):
        """Close file
        Ensure flushing the buffer
        """
        if self._writer.closed():
            return
        self._upload_chunk(final=True)
        self._writer.close()


class BufferedFileRead(fsspec.spec.AbstractBufferedFile):
    def __init__(self, fs, path, mode="rb", **kwargs):
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
