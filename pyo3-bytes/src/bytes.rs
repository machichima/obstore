//! Support for Python buffer protocol

use std::os::raw::c_int;
use std::ptr::NonNull;

use bytes::{Bytes, BytesMut};

use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyIndexError, PyValueError};
use pyo3::prelude::*;
use pyo3::{ffi, IntoPyObjectExt};

/// A wrapper around a [`bytes::Bytes`][].
///
/// This implements both import and export via the Python buffer protocol.
///
/// ### Buffer protocol import
///
/// This can be very useful as a general way to support ingest of a Python buffer protocol object.
///
/// The underlying [Bytes] manages the external memory, automatically calling the Python
/// buffer's release callback when the internal reference count reaches 0.
///
/// Note that converting this [`Bytes`] into a [BytesMut][::bytes::BytesMut] will always create a
/// deep copy of the buffer into newly allocated memory, since this `Bytes` is constructed from an
/// owner.
///
/// ### Buffer protocol export
///
/// PyBytes implements the Python buffer protocol to enable Python to access the underlying `Bytes`
/// data view without copies. In Python, this `PyBytes` object can be passed to Python `bytes` or
/// `memoryview` constructors, `numpy.frombuffer`, or any other function that supports buffer
/// protocol input.
#[pyclass(name = "Bytes", subclass, frozen, sequence, weakref)]
#[derive(Debug, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub struct PyBytes(Bytes);

impl AsRef<Bytes> for PyBytes {
    fn as_ref(&self) -> &Bytes {
        &self.0
    }
}

impl AsRef<[u8]> for PyBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl PyBytes {
    /// Construct a new [PyBytes]
    pub fn new(buffer: Bytes) -> Self {
        Self(buffer)
    }

    /// Consume and return the [Bytes]
    pub fn into_inner(self) -> Bytes {
        self.0
    }

    /// Access the underlying buffer as a byte slice
    pub fn as_slice(&self) -> &[u8] {
        self.as_ref()
    }
}

impl From<PyBytes> for Bytes {
    fn from(value: PyBytes) -> Self {
        value.0
    }
}

impl From<Vec<u8>> for PyBytes {
    fn from(value: Vec<u8>) -> Self {
        PyBytes(value.into())
    }
}

impl From<Bytes> for PyBytes {
    fn from(value: Bytes) -> Self {
        PyBytes(value)
    }
}

impl From<BytesMut> for PyBytes {
    fn from(value: BytesMut) -> Self {
        PyBytes(value.into())
    }
}

#[pymethods]
impl PyBytes {
    // By setting the argument to PyBytes, this means that any buffer-protocol object is supported
    // here, since it will use the FromPyObject impl.
    #[new]
    fn py_new(buf: PyBytes) -> Self {
        buf
    }

    /// The number of bytes in this Bytes
    fn __len__(&self) -> usize {
        self.0.len()
    }

    fn __repr__(&self) -> String {
        format!("Bytes({})", self.0.len())
    }

    fn __add__(&self, other: PyBytes) -> PyBytes {
        let total_length = self.0.len() + other.0.len();
        let mut new_buffer = BytesMut::with_capacity(total_length);
        new_buffer.extend_from_slice(&self.0);
        new_buffer.extend_from_slice(&other.0);
        new_buffer.into()
    }

    fn __contains__(&self, item: PyBytes) -> bool {
        self.0
            .windows(item.0.len())
            .any(|window| window == item.as_slice())
    }

    fn __eq__(&self, other: PyBytes) -> bool {
        self.0.as_ref() == other.0.as_ref()
    }

    fn __getitem__(&self, py: Python, key: Bound<PyAny>) -> PyResult<PyObject> {
        if let Ok(mut index) = key.extract::<isize>() {
            if index < 0 {
                index += self.0.len() as isize;
            }

            self.0
                .get(index as usize)
                .ok_or(PyIndexError::new_err("Index out of range"))?
                .into_py_any(py)
        } else {
            Err(PyValueError::new_err(
                "Currently, only integer keys are allowed in __getitem__.",
            ))
        }
    }

    fn __mul__(&self, value: usize) -> PyBytes {
        let mut out_buf = BytesMut::with_capacity(self.0.len() * value);
        (0..value).for_each(|_| out_buf.extend_from_slice(self.0.as_ref()));
        out_buf.into()
    }

    /// This is taken from opendal:
    /// https://github.com/apache/opendal/blob/d001321b0f9834bc1e2e7d463bcfdc3683e968c9/bindings/python/src/utils.rs#L51-L72
    unsafe fn __getbuffer__(
        slf: PyRef<Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let bytes = slf.0.as_ref();
        let ret = ffi::PyBuffer_FillInfo(
            view,
            slf.as_ptr() as *mut _,
            bytes.as_ptr() as *mut _,
            bytes.len().try_into().unwrap(),
            1, // read only
            flags,
        );
        if ret == -1 {
            return Err(PyErr::fetch(slf.py()));
        }
        Ok(())
    }

    // Comment from david hewitt on discord:
    // > I think normally `__getbuffer__` takes a pointer to the owning Python object, so you
    // > don't need to treat the allocation as owned separately. It should be good enough to keep
    // > the allocation owned by the object.
    // https://discord.com/channels/1209263839632424990/1324816949464666194/1328299411427557397
    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {}

    /// If the binary data starts with the prefix string, return bytes[len(prefix):]. Otherwise,
    /// return a copy of the original binary data:
    #[pyo3(signature = (prefix, /))]
    fn removeprefix(&self, prefix: PyBytes) -> PyBytes {
        if self.0.starts_with(prefix.as_ref()) {
            self.0.slice(prefix.0.len()..).into()
        } else {
            self.0.clone().into()
        }
    }

    /// If the binary data ends with the suffix string and that suffix is not empty, return
    /// `bytes[:-len(suffix)]`. Otherwise, return the original binary data.
    #[pyo3(signature = (suffix, /))]
    fn removesuffix(&self, suffix: PyBytes) -> PyBytes {
        if self.0.ends_with(suffix.as_ref()) {
            self.0.slice(0..self.0.len() - suffix.0.len()).into()
        } else {
            self.0.clone().into()
        }
    }

    /// Return True if all bytes in the sequence are alphabetical ASCII characters or ASCII decimal
    /// digits and the sequence is not empty, False otherwise. Alphabetic ASCII characters are
    /// those byte values in the sequence b'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'.
    /// ASCII decimal digits are those byte values in the sequence b'0123456789'.
    fn isalnum(&self) -> bool {
        if self.0.is_empty() {
            return false;
        }

        for c in self.0.as_ref() {
            if !c.is_ascii_alphanumeric() {
                return false;
            }
        }
        true
    }

    /// Return True if all bytes in the sequence are alphabetic ASCII characters and the sequence
    /// is not empty, False otherwise. Alphabetic ASCII characters are those byte values in the
    /// sequence b'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'.
    fn isalpha(&self) -> bool {
        if self.0.is_empty() {
            return false;
        }

        for c in self.0.as_ref() {
            if !c.is_ascii_alphabetic() {
                return false;
            }
        }
        true
    }

    /// Return True if the sequence is empty or all bytes in the sequence are ASCII, False
    /// otherwise. ASCII bytes are in the range 0-0x7F.
    fn isascii(&self) -> bool {
        for c in self.0.as_ref() {
            if !c.is_ascii() {
                return false;
            }
        }
        true
    }

    /// Return True if all bytes in the sequence are ASCII decimal digits and the sequence is not
    /// empty, False otherwise. ASCII decimal digits are those byte values in the sequence
    /// b'0123456789'.
    fn isdigit(&self) -> bool {
        if self.0.is_empty() {
            return false;
        }

        for c in self.0.as_ref() {
            if !c.is_ascii_digit() {
                return false;
            }
        }
        true
    }

    /// Return True if there is at least one lowercase ASCII character in the sequence and no
    /// uppercase ASCII characters, False otherwise.
    fn islower(&self) -> bool {
        let mut has_lower = false;
        for c in self.0.as_ref() {
            if c.is_ascii_uppercase() {
                return false;
            }
            if !has_lower && c.is_ascii_lowercase() {
                has_lower = true;
            }
        }

        has_lower
    }

    /// Return True if all bytes in the sequence are ASCII whitespace and the sequence is not
    /// empty, False otherwise. ASCII whitespace characters are those byte values in the sequence
    /// b' \t\n\r\x0b\f' (space, tab, newline, carriage return, vertical tab, form feed).
    fn isspace(&self) -> bool {
        if self.0.is_empty() {
            return false;
        }

        for c in self.0.as_ref() {
            // Also check for vertical tab
            if !(c.is_ascii_whitespace() || *c == b'\x0b') {
                return false;
            }
        }
        true
    }

    /// Return True if there is at least one uppercase alphabetic ASCII character in the sequence
    /// and no lowercase ASCII characters, False otherwise.
    fn isupper(&self) -> bool {
        let mut has_upper = false;
        for c in self.0.as_ref() {
            if c.is_ascii_lowercase() {
                return false;
            }
            if !has_upper && c.is_ascii_uppercase() {
                has_upper = true;
            }
        }

        has_upper
    }

    /// Return a copy of the sequence with all the uppercase ASCII characters converted to their
    /// corresponding lowercase counterpart.
    fn lower(&self) -> PyBytes {
        self.0.to_ascii_lowercase().into()
    }

    /// Return a copy of the sequence with all the lowercase ASCII characters converted to their
    /// corresponding uppercase counterpart.
    fn upper(&self) -> PyBytes {
        self.0.to_ascii_uppercase().into()
    }

    /// Copy this buffer's contents to a Python `bytes` object
    fn to_bytes<'py>(&'py self, py: Python<'py>) -> Bound<'py, pyo3::types::PyBytes> {
        pyo3::types::PyBytes::new(py, &self.0)
    }
}

impl<'py> FromPyObject<'py> for PyBytes {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let buffer = ob.extract::<PyBytesWrapper>()?;
        let bytes = Bytes::from_owner(buffer);
        Ok(Self(bytes))
    }
}

/// A wrapper around a PyBuffer that applies a custom destructor that checks if the Python
/// interpreter is still initialized before freeing the buffer memory.
///
/// This also implements AsRef<[u8]> because that is required for Bytes::from_owner
#[derive(Debug)]
struct PyBytesWrapper(Option<PyBuffer<u8>>);

impl Drop for PyBytesWrapper {
    fn drop(&mut self) {
        // Only call the underlying Drop of PyBuffer if the Python interpreter is still
        // initialized. Sometimes the Drop can attempt to happen after the Python interpreter was
        // already finalized.
        // https://github.com/kylebarron/arro3/issues/230
        let is_initialized = unsafe { ffi::Py_IsInitialized() };
        if let Some(val) = self.0.take() {
            if is_initialized == 0 {
                std::mem::forget(val);
            } else {
                std::mem::drop(val);
            }
        }
    }
}

impl AsRef<[u8]> for PyBytesWrapper {
    fn as_ref(&self) -> &[u8] {
        let buffer = self.0.as_ref().expect("Buffer already disposed");
        let len = buffer.item_count();

        let ptr = NonNull::new(buffer.buf_ptr() as _).expect("Expected buffer ptr to be non null");

        // Safety:
        //
        // This requires that the data will not be mutated from Python. Sadly, the buffer protocol
        // does not uphold this invariant always for us, and the Python user must take care not to
        // mutate the provided buffer.
        unsafe { std::slice::from_raw_parts(ptr.as_ptr() as *const u8, len) }
    }
}

fn validate_buffer(buf: &PyBuffer<u8>) -> PyResult<()> {
    if !buf.is_c_contiguous() {
        return Err(PyValueError::new_err("Buffer is not C contiguous"));
    }

    if buf.shape().iter().any(|s| *s == 0) {
        return Err(PyValueError::new_err("0-length dimension not supported."));
    }

    if buf.strides().iter().any(|s| *s == 0) {
        return Err(PyValueError::new_err("Non-zero strides not supported."));
    }

    Ok(())
}

impl<'py> FromPyObject<'py> for PyBytesWrapper {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let buffer = ob.extract::<PyBuffer<u8>>()?;
        validate_buffer(&buffer)?;
        Ok(Self(Some(buffer)))
    }
}
