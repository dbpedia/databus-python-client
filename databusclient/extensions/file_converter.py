"""File format conversion extension for databus-python-client.

Provides streaming pipeline for file decompression, re-compression,
and checksum validation during download operations.

Supports gzip, bz2, xz formats natively. Optional zstd support is
available when the ``zstandard`` package is installed.

The special format name ``'none'`` represents an uncompressed / raw file.
"""

import bz2
import gzip
import hashlib
import lzma
import os
from typing import BinaryIO, Dict, Optional

# --- Optional zstd support ---------------------------------------------------
try:
    import zstandard as _zstd

    _HAS_ZSTD = True
except ImportError:  # pragma: no cover
    _zstd = None
    _HAS_ZSTD = False


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

COMPRESSION_EXTENSIONS: Dict[str, str] = {
    "bz2": ".bz2",
    "gz": ".gz",
    "xz": ".xz",
}

COMPRESSION_MODULES: Dict[str, object] = {
    "bz2": bz2,
    "gz": gzip,
    "xz": lzma,
}

if _HAS_ZSTD:
    COMPRESSION_EXTENSIONS["zstd"] = ".zst"
    # zstandard doesn't expose a module-level open(); handled specially.
    COMPRESSION_MODULES["zstd"] = _zstd

# Magic-number signatures (first N bytes -> format).
MAGIC_NUMBERS: Dict[bytes, str] = {
    b"\x1f\x8b": "gz",        # gzip
    b"BZ": "bz2",             # bzip2 (BZh...)
    b"\xfd7zXZ\x00": "xz",   # xz / LZMA
}
if _HAS_ZSTD:
    MAGIC_NUMBERS[b"\x28\xb5\x2f\xfd"] = "zstd"


class FileConverter:
    """Handles file format conversion with streaming support.

    All public methods are ``@staticmethod``; instantiation is not required.
    """

    CHUNK_SIZE = 8192  # 8 KiB chunks for streaming

    # ------------------------------------------------------------------
    # Format detection
    # ------------------------------------------------------------------

    @staticmethod
    def detect_format(filename: str, header_bytes: Optional[bytes] = None) -> str:
        """Detect the compression format of a file.

        Checks the file *extension* first.  When *header_bytes* are provided
        the magic-number signature is also inspected and takes precedence if
        the extension is ambiguous.

        Args:
            filename: Name (or path) of the file.
            header_bytes: Optional first bytes of the file content for
                magic-number detection.

        Returns:
            ``'gz'``, ``'bz2'``, ``'xz'``, ``'zstd'`` or ``'none'``.
        """
        # 1) Extension-based detection
        filename_lower = filename.lower()
        for fmt, ext in COMPRESSION_EXTENSIONS.items():
            if filename_lower.endswith(ext):
                return fmt

        # 2) Magic-number fallback
        if header_bytes:
            detected = FileConverter.detect_format_by_magic(header_bytes)
            if detected != "none":
                return detected

        return "none"

    @staticmethod
    def detect_format_by_magic(header_bytes: bytes) -> str:
        """Detect compression format from raw magic bytes.

        Args:
            header_bytes: The first bytes of file content (≥6 bytes
                recommended).

        Returns:
            ``'gz'``, ``'bz2'``, ``'xz'``, ``'zstd'`` or ``'none'``.
        """
        for magic, fmt in MAGIC_NUMBERS.items():
            if header_bytes[: len(magic)] == magic:
                return fmt
        return "none"

    # ------------------------------------------------------------------
    # Individual stream helpers (gzip)
    # ------------------------------------------------------------------

    @staticmethod
    def decompress_gzip_stream(
        input_stream: BinaryIO,
        output_stream: BinaryIO,
        validate_checksum: bool = False,
    ) -> Optional[str]:
        """Decompress gzip stream with optional checksum computation.

        Args:
            input_stream: Input gzip compressed stream.
            output_stream: Output decompressed stream.
            validate_checksum: Whether to compute a SHA-256 checksum of
                the decompressed output.

        Returns:
            Hex-encoded SHA-256 checksum when *validate_checksum* is
            ``True``, otherwise ``None``.
        """
        hasher = hashlib.sha256() if validate_checksum else None

        with gzip.open(input_stream, "rb") as gz:
            while True:
                chunk = gz.read(FileConverter.CHUNK_SIZE)
                if not chunk:
                    break
                output_stream.write(chunk)
                if hasher:
                    hasher.update(chunk)

        return hasher.hexdigest() if hasher else None

    @staticmethod
    def compress_gzip_stream(
        input_stream: BinaryIO, output_stream: BinaryIO
    ) -> None:
        """Compress stream to gzip format."""
        with gzip.open(output_stream, "wb") as gz:
            while True:
                chunk = input_stream.read(FileConverter.CHUNK_SIZE)
                if not chunk:
                    break
                gz.write(chunk)

    # ------------------------------------------------------------------
    # Individual stream helpers (bz2)
    # ------------------------------------------------------------------

    @staticmethod
    def decompress_bz2_stream(
        input_stream: BinaryIO,
        output_stream: BinaryIO,
        validate_checksum: bool = False,
    ) -> Optional[str]:
        """Decompress bz2 stream with optional checksum computation."""
        hasher = hashlib.sha256() if validate_checksum else None

        with bz2.open(input_stream, "rb") as bf:
            while True:
                chunk = bf.read(FileConverter.CHUNK_SIZE)
                if not chunk:
                    break
                output_stream.write(chunk)
                if hasher:
                    hasher.update(chunk)

        return hasher.hexdigest() if hasher else None

    @staticmethod
    def compress_bz2_stream(
        input_stream: BinaryIO, output_stream: BinaryIO
    ) -> None:
        """Compress stream to bz2 format."""
        with bz2.open(output_stream, "wb") as bf:
            while True:
                chunk = input_stream.read(FileConverter.CHUNK_SIZE)
                if not chunk:
                    break
                bf.write(chunk)

    # ------------------------------------------------------------------
    # Individual stream helpers (xz / LZMA)
    # ------------------------------------------------------------------

    @staticmethod
    def decompress_xz_stream(
        input_stream: BinaryIO,
        output_stream: BinaryIO,
        validate_checksum: bool = False,
    ) -> Optional[str]:
        """Decompress xz stream with optional checksum computation."""
        hasher = hashlib.sha256() if validate_checksum else None

        with lzma.open(input_stream, "rb") as xf:
            while True:
                chunk = xf.read(FileConverter.CHUNK_SIZE)
                if not chunk:
                    break
                output_stream.write(chunk)
                if hasher:
                    hasher.update(chunk)

        return hasher.hexdigest() if hasher else None

    @staticmethod
    def compress_xz_stream(
        input_stream: BinaryIO, output_stream: BinaryIO
    ) -> None:
        """Compress stream to xz format."""
        with lzma.open(output_stream, "wb") as xf:
            while True:
                chunk = input_stream.read(FileConverter.CHUNK_SIZE)
                if not chunk:
                    break
                xf.write(chunk)

    # ------------------------------------------------------------------
    # Checksum validation
    # ------------------------------------------------------------------

    @staticmethod
    def validate_checksum_stream(
        input_stream: BinaryIO, expected_checksum: str
    ) -> bool:
        """Validate SHA-256 checksum of a stream.

        The stream is **seeked to position 0** before reading and
        **seeked back to 0** after reading, so the caller can continue
        to use it.  If the checksum does not match, a ``ValueError``
        is raised; otherwise ``True`` is returned.

        Args:
            input_stream: Seekable binary input stream.
            expected_checksum: Expected SHA-256 hex digest (case-insensitive).

        Returns:
            ``True`` when the computed checksum matches *expected_checksum*.

        Raises:
            ValueError: If the computed checksum does not match.
        """
        hasher = hashlib.sha256()
        input_stream.seek(0)

        while True:
            chunk = input_stream.read(FileConverter.CHUNK_SIZE)
            if not chunk:
                break
            hasher.update(chunk)

        computed = hasher.hexdigest()
        input_stream.seek(0)
        if computed.lower() != expected_checksum.lower():
            raise ValueError(
                f"Checksum mismatch: expected {expected_checksum}, got {computed}"
            )
        return True

    # ------------------------------------------------------------------
    # High-level: convert on-disk files
    # ------------------------------------------------------------------

    @staticmethod
    def convert_file(
        source_path: str,
        target_path: str,
        source_format: str,
        target_format: str,
    ) -> None:
        """Convert a file between compression formats.

        ``source_format`` / ``target_format`` may be ``'none'`` to mean
        "raw / uncompressed".  So ``convert_file(f, t, 'gz', 'none')``
        decompresses and ``convert_file(f, t, 'none', 'bz2')`` compresses.

        The *source_path* is removed on success.

        Raises:
            ValueError: If a format is not recognised.
            RuntimeError: If the conversion fails.
        """
        _validate_format(source_format, "source")
        _validate_format(target_format, "target")

        print(
            f"Converting {source_format} → {target_format}: "
            f"{os.path.basename(source_path)}"
        )

        try:
            with _open_reader(source_path, source_format) as reader:
                with _open_writer(target_path, target_format) as writer:
                    while True:
                        chunk = reader.read(FileConverter.CHUNK_SIZE)
                        if not chunk:
                            break
                        writer.write(chunk)
        except Exception as e:
            if os.path.exists(target_path):
                os.remove(target_path)
            raise RuntimeError(f"Compression conversion failed: {e}")

        # Source removal is intentionally outside the try block so that
        # a failure to delete the source does not trigger target cleanup.
        os.remove(source_path)
        print(f"Conversion complete: {os.path.basename(target_path)}")

    # ------------------------------------------------------------------
    # High-level: streaming conversion on file-like objects
    # ------------------------------------------------------------------

    @staticmethod
    def convert_stream(
        input_stream: BinaryIO,
        output_stream: BinaryIO,
        source_format: str,
        target_format: str,
        compute_checksum: bool = False,
    ) -> Optional[str]:
        """Stream conversion between two file-like objects.

        Data is read from *input_stream*, decompressed (if
        ``source_format != 'none'``), recompressed (if
        ``target_format != 'none'``), and written to *output_stream*.

        When *compute_checksum* is ``True`` the SHA-256 digest of the
        **decompressed** (intermediate) bytes is computed and returned
        as a hex string.  The caller can compare this value against a
        known-good digest to verify data integrity.

        Args:
            input_stream: Source file-like object (binary read).
            output_stream: Target file-like object (binary write).
            source_format: Compression format of *input_stream*.
            target_format: Compression format for *output_stream*.
            compute_checksum: If ``True``, compute and return a SHA-256
                hex digest of the decompressed intermediate bytes.

        Returns:
            Hex SHA-256 digest when *compute_checksum* is ``True``,
            otherwise ``None``.
        """
        _validate_format(source_format, "source")
        _validate_format(target_format, "target")

        hasher = hashlib.sha256() if compute_checksum else None

        reader_ctx = _wrap_reader_ctx(input_stream, source_format)
        writer_ctx = _wrap_writer(output_stream, target_format)

        with reader_ctx as reader, writer_ctx as writer:
            while True:
                chunk = reader.read(FileConverter.CHUNK_SIZE)
                if not chunk:
                    break
                if hasher:
                    hasher.update(chunk)
                writer.write(chunk)

        return hasher.hexdigest() if hasher else None

    # ------------------------------------------------------------------
    # Filename helpers
    # ------------------------------------------------------------------

    @staticmethod
    def get_converted_filename(
        filename: str, source_format: str, target_format: str
    ) -> str:
        """Generate the new filename after format conversion.

        Handles ``'none'`` by stripping / adding extensions as needed.
        """
        # Strip existing compression extension (if any)
        if source_format != "none":
            source_ext = COMPRESSION_EXTENSIONS[source_format]
            if filename.lower().endswith(source_ext):
                filename = filename[: -len(source_ext)]

        # Append new compression extension (if any)
        if target_format != "none":
            target_ext = COMPRESSION_EXTENSIONS[target_format]
            filename = filename + target_ext

        return filename


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

_VALID_FORMATS = set(COMPRESSION_MODULES.keys()) | {"none"}


def _validate_format(fmt: str, label: str) -> None:
    if fmt not in _VALID_FORMATS:
        raise ValueError(
            f"Unsupported {label} compression format: {fmt}. "
            f"Supported formats: {sorted(_VALID_FORMATS)}"
        )


def _open_reader(path: str, fmt: str):
    """Return a context-manager file object for *reading*."""
    if fmt == "none":
        return open(path, "rb")
    if fmt == "zstd" and _HAS_ZSTD:
        fh = open(path, "rb")
        dctx = _zstd.ZstdDecompressor()
        return dctx.stream_reader(fh)
    return COMPRESSION_MODULES[fmt].open(path, "rb")


def _open_writer(path: str, fmt: str):
    """Return a context-manager file object for *writing*."""
    if fmt == "none":
        return open(path, "wb")
    if fmt == "zstd" and _HAS_ZSTD:
        fh = open(path, "wb")
        cctx = _zstd.ZstdCompressor()
        return cctx.stream_writer(fh)
    return COMPRESSION_MODULES[fmt].open(path, "wb")


class _NullCtx:
    """Tiny wrapper to turn a plain file-like into a no-op context manager."""

    def __init__(self, obj):
        self._obj = obj

    def __enter__(self):
        return self._obj

    def __exit__(self, *exc):
        return False


def _wrap_reader(stream: BinaryIO, fmt: str):
    """Wrap *stream* so that ``read()`` yields decompressed bytes."""
    if fmt == "none":
        return stream
    if fmt == "zstd" and _HAS_ZSTD:
        dctx = _zstd.ZstdDecompressor()
        return dctx.stream_reader(stream, closefd=False)
    return COMPRESSION_MODULES[fmt].open(stream, "rb")


def _wrap_reader_ctx(stream: BinaryIO, fmt: str):
    """Like :func:`_wrap_reader` but always returns a context manager.

    For ``'none'`` format the original *stream* is returned inside a
    :class:`_NullCtx` so the caller can use ``with`` uniformly.
    """
    if fmt == "none":
        return _NullCtx(stream)
    if fmt == "zstd" and _HAS_ZSTD:
        dctx = _zstd.ZstdDecompressor()
        return dctx.stream_reader(stream, closefd=False)
    return COMPRESSION_MODULES[fmt].open(stream, "rb")


def _wrap_writer(stream: BinaryIO, fmt: str):
    """Return a context-manager wrapping *stream* for compressed writing."""
    if fmt == "none":
        return _NullCtx(stream)
    if fmt == "zstd" and _HAS_ZSTD:
        cctx = _zstd.ZstdCompressor()
        return cctx.stream_writer(stream, closefd=False)
    return COMPRESSION_MODULES[fmt].open(stream, "wb")
