import os

import fsspec
import pyarrow.parquet as pq
import pytest

import obstore as obs
from obstore.fsspec import AsyncFsspecStore, register
from tests.conftest import TEST_BUCKET_NAME


def test_register():
    """Test that register properly creates and registers a subclass for a given protocol."""
    register("s3")  # Register the "s3" protocol dynamically
    fs_class = fsspec.get_filesystem_class("s3")

    assert issubclass(
        fs_class, AsyncFsspecStore
    ), "Registered class should be a subclass of AsyncFsspecStore"
    assert (
        fs_class.protocol == "s3"
    ), "Registered class should have the correct protocol"

    # Ensure a new instance of the registered store can be created
    fs_instance = fs_class()
    assert isinstance(
        fs_instance, AsyncFsspecStore
    ), "Registered class should be instantiable"

    # test register asynchronous
    register("gcs", asynchronous=True)  # Register the "s3" protocol dynamically
    fs_class = fsspec.get_filesystem_class("gcs")
    assert (
        fs_class.asynchronous == True
    ), "Registered class should be asynchronous"

    # test multiple registrations
    register(["file", "abfs"])
    assert issubclass(fsspec.get_filesystem_class("file"), AsyncFsspecStore)
    assert issubclass(fsspec.get_filesystem_class("abfs"), AsyncFsspecStore)


def test_register_invalid_types():
    """Test that register rejects invalid input types."""
    with pytest.raises(TypeError):
        register(123)  # Not a string or list

    with pytest.raises(TypeError):
        register(["s3", 42])  # List contains a non-string

    with pytest.raises(ValueError):
        register(["s3", ""])  # List contains a non-string

    with pytest.raises(TypeError):
        register(None)  # None is invalid

    with pytest.raises(ValueError):
        register([])  # Empty list is invalid

@pytest.fixture()
def fs(s3_store_config):
    register("s3")
    return fsspec.filesystem("s3", config=s3_store_config)


def test_list(fs):
    out = fs.ls("", detail=False)
    assert out == ["afile"]
    fs.pipe_file("dir/bfile", b"data")
    out = fs.ls("", detail=False)
    assert out == ["afile", "dir"]
    out = fs.ls("", detail=True)
    assert out[0]["type"] == "file"
    assert out[1]["type"] == "directory"


@pytest.mark.asyncio
async def test_list_async(s3_store):
    fs = AsyncFsspecStore(s3_store, asynchronous=True)
    out = await fs._ls("", detail=False)
    assert out == ["afile"]
    await fs._pipe_file("dir/bfile", b"data")
    out = await fs._ls("", detail=False)
    assert out == ["afile", "dir"]
    out = await fs._ls("", detail=True)
    assert out[0]["type"] == "file"
    assert out[1]["type"] == "directory"


@pytest.mark.network
def test_remote_parquet():
    store = obs.store.HTTPStore.from_url("https://github.com")
    fs = AsyncFsspecStore(store)
    url = "opengeospatial/geoparquet/raw/refs/heads/main/examples/example.parquet"
    pq.read_metadata(url, filesystem=fs)


def test_multi_file_ops(fs):
    data = {"dir/test1": b"test data1", "dir/test2": b"test data2"}
    fs.pipe(data)
    out = fs.cat(list(data))
    assert out == data
    out = fs.cat("dir", recursive=True)
    assert out == data
    fs.cp("dir", "dir2", recursive=True)
    out = fs.find("", detail=False)
    assert out == ["afile", "dir/test1", "dir/test2", "dir2/test1", "dir2/test2"]
    fs.rm(["dir", "dir2"], recursive=True)
    out = fs.find("", detail=False)
    assert out == ["afile"]


def test_cat_ranges_one(fs):
    data1 = os.urandom(10000)
    fs.pipe_file("data1", data1)

    # single range
    out = fs.cat_ranges(["data1"], [10], [20])
    assert out == [data1[10:20]]

    # range oob
    out = fs.cat_ranges(["data1"], [0], [11000])
    assert out == [data1]

    # two disjoint ranges, one file
    out = fs.cat_ranges(["data1", "data1"], [10, 40], [20, 60])
    assert out == [data1[10:20], data1[40:60]]

    # two adjoining ranges, one file
    out = fs.cat_ranges(["data1", "data1"], [10, 30], [20, 60])
    assert out == [data1[10:20], data1[30:60]]

    # two overlapping ranges, one file
    out = fs.cat_ranges(["data1", "data1"], [10, 15], [20, 60])
    assert out == [data1[10:20], data1[15:60]]

    # completely overlapping ranges, one file
    out = fs.cat_ranges(["data1", "data1"], [10, 0], [20, 60])
    assert out == [data1[10:20], data1[0:60]]


def test_cat_ranges_two(fs):
    data1 = os.urandom(10000)
    data2 = os.urandom(10000)
    fs.pipe({"data1": data1, "data2": data2})

    # single range in each file
    out = fs.cat_ranges(["data1", "data2"], [10, 10], [20, 20])
    assert out == [data1[10:20], data2[10:20]]


@pytest.mark.xfail(reason="negative and mixed ranges not implemented")
def test_cat_ranges_mixed(fs):
    data1 = os.urandom(10000)
    data2 = os.urandom(10000)
    fs.pipe({"data1": data1, "data2": data2})

    # single range in each file
    out = fs.cat_ranges(["data1", "data1", "data2"], [-10, None, 10], [None, -10, -10])
    assert out == [data1[-10:], data1[:-10], data2[10:-10]]


@pytest.mark.xfail(reason="atomic writes not working on moto")
def test_atomic_write(fs):
    fs.pipe_file("data1", b"data1")
    fs.pipe_file("data1", b"data1", mode="overwrite")
    with pytest.raises(ValueError):
        fs.pipe_file("data1", b"data1", mode="create")


def test_cat_ranges_error(fs):
    with pytest.raises(ValueError):
        fs.cat_ranges(["path"], [], [])
