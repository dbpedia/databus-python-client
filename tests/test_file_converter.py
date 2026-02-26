"""Tests for the FileConverter module and new conversion capabilities.

Covers:
- Decompression to raw (convert_to='none')
- Compression of raw files (source_format='none')
- Streaming conversion via convert_stream
- Magic-number format detection
- Filename generation with 'none' format
- Checksum validation during streaming
- --decompress CLI flag
"""

import bz2
import gzip
import hashlib
import io
import lzma
import os
import tempfile

import pytest
from click.testing import CliRunner

from databusclient.extensions.file_converter import FileConverter
from databusclient.api.download import _should_convert_file
from databusclient.cli import download

# Re-usable test payload
_TEST_DATA = b"Hello, streaming converter! " * 200


# ---------------------------------------------------------------------------
# Format detection: detect_format and detect_format_by_magic
# ---------------------------------------------------------------------------

class TestFormatDetection:

    def test_detect_format_by_extension(self):
        assert FileConverter.detect_format("data.csv.gz") == "gz"
        assert FileConverter.detect_format("dump.nt.bz2") == "bz2"
        assert FileConverter.detect_format("file.xz") == "xz"
        assert FileConverter.detect_format("readme.txt") == "none"

    def test_detect_format_case_insensitive(self):
        assert FileConverter.detect_format("FILE.GZ") == "gz"
        assert FileConverter.detect_format("dump.BZ2") == "bz2"

    def test_detect_format_by_magic_gzip(self):
        header = b"\x1f\x8b\x08\x00"
        assert FileConverter.detect_format_by_magic(header) == "gz"

    def test_detect_format_by_magic_bz2(self):
        header = b"BZh91AY&SY"
        assert FileConverter.detect_format_by_magic(header) == "bz2"

    def test_detect_format_by_magic_xz(self):
        header = b"\xfd7zXZ\x00\x00"
        assert FileConverter.detect_format_by_magic(header) == "xz"

    def test_detect_format_by_magic_unknown(self):
        header = b"\x00\x00\x00\x00"
        assert FileConverter.detect_format_by_magic(header) == "none"

    def test_detect_format_extension_wins_but_magic_fallback(self):
        # Extension wins when present
        assert FileConverter.detect_format("file.gz", b"\x1f\x8b") == "gz"
        # Magic fallback when extension is absent
        assert FileConverter.detect_format("file.dat", b"\x1f\x8b") == "gz"


# ---------------------------------------------------------------------------
# Decompression tests (convert_to='none')
# ---------------------------------------------------------------------------

class TestDecompressionToNone:

    def test_decompress_gzip_to_none(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            gz_path = os.path.join(tmpdir, "data.txt.gz")
            raw_path = os.path.join(tmpdir, "data.txt")
            with gzip.open(gz_path, "wb") as f:
                f.write(_TEST_DATA)
            FileConverter.convert_file(gz_path, raw_path, "gz", "none")
            assert not os.path.exists(gz_path)
            with open(raw_path, "rb") as f:
                assert f.read() == _TEST_DATA

    def test_decompress_bz2_to_none(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            bz2_path = os.path.join(tmpdir, "data.txt.bz2")
            raw_path = os.path.join(tmpdir, "data.txt")
            with bz2.open(bz2_path, "wb") as f:
                f.write(_TEST_DATA)
            FileConverter.convert_file(bz2_path, raw_path, "bz2", "none")
            assert not os.path.exists(bz2_path)
            with open(raw_path, "rb") as f:
                assert f.read() == _TEST_DATA

    def test_decompress_xz_to_none(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            xz_path = os.path.join(tmpdir, "data.txt.xz")
            raw_path = os.path.join(tmpdir, "data.txt")
            with lzma.open(xz_path, "wb") as f:
                f.write(_TEST_DATA)
            FileConverter.convert_file(xz_path, raw_path, "xz", "none")
            assert not os.path.exists(xz_path)
            with open(raw_path, "rb") as f:
                assert f.read() == _TEST_DATA


# ---------------------------------------------------------------------------
# Compression of raw files (source_format='none')
# ---------------------------------------------------------------------------

class TestCompressionFromNone:

    def test_compress_none_to_gzip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_path = os.path.join(tmpdir, "data.txt")
            gz_path = os.path.join(tmpdir, "data.txt.gz")
            with open(raw_path, "wb") as f:
                f.write(_TEST_DATA)
            FileConverter.convert_file(raw_path, gz_path, "none", "gz")
            assert not os.path.exists(raw_path)
            with gzip.open(gz_path, "rb") as f:
                assert f.read() == _TEST_DATA

    def test_compress_none_to_bz2(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_path = os.path.join(tmpdir, "data.txt")
            bz2_path = os.path.join(tmpdir, "data.txt.bz2")
            with open(raw_path, "wb") as f:
                f.write(_TEST_DATA)
            FileConverter.convert_file(raw_path, bz2_path, "none", "bz2")
            assert not os.path.exists(raw_path)
            with bz2.open(bz2_path, "rb") as f:
                assert f.read() == _TEST_DATA

    def test_compress_none_to_xz(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_path = os.path.join(tmpdir, "data.txt")
            xz_path = os.path.join(tmpdir, "data.txt.xz")
            with open(raw_path, "wb") as f:
                f.write(_TEST_DATA)
            FileConverter.convert_file(raw_path, xz_path, "none", "xz")
            assert not os.path.exists(raw_path)
            with lzma.open(xz_path, "rb") as f:
                assert f.read() == _TEST_DATA


# ---------------------------------------------------------------------------
# Streaming conversion (convert_stream)
# ---------------------------------------------------------------------------

class TestStreamConversion:

    def test_convert_stream_gz_to_bz2(self):
        # Compress test data into gzip in-memory
        gz_buf = io.BytesIO()
        with gzip.open(gz_buf, "wb") as gz:
            gz.write(_TEST_DATA)
        gz_buf.seek(0)

        bz2_buf = io.BytesIO()
        FileConverter.convert_stream(gz_buf, bz2_buf, "gz", "bz2")

        bz2_buf.seek(0)
        assert bz2.decompress(bz2_buf.read()) == _TEST_DATA

    def test_convert_stream_decompress(self):
        gz_buf = io.BytesIO()
        with gzip.open(gz_buf, "wb") as gz:
            gz.write(_TEST_DATA)
        gz_buf.seek(0)

        raw_buf = io.BytesIO()
        FileConverter.convert_stream(gz_buf, raw_buf, "gz", "none")

        assert raw_buf.getvalue() == _TEST_DATA

    def test_convert_stream_compress(self):
        raw_buf = io.BytesIO(_TEST_DATA)
        gz_buf = io.BytesIO()
        FileConverter.convert_stream(raw_buf, gz_buf, "none", "gz")

        gz_buf.seek(0)
        with gzip.open(gz_buf, "rb") as gz:
            assert gz.read() == _TEST_DATA

    def test_streaming_checksum_validation(self):
        expected_hash = hashlib.sha256(_TEST_DATA).hexdigest()

        gz_buf = io.BytesIO()
        with gzip.open(gz_buf, "wb") as gz:
            gz.write(_TEST_DATA)
        gz_buf.seek(0)

        raw_buf = io.BytesIO()
        result_hash = FileConverter.convert_stream(
            gz_buf, raw_buf, "gz", "none", compute_checksum=True
        )
        assert result_hash == expected_hash


# ---------------------------------------------------------------------------
# Filename generation with 'none'
# ---------------------------------------------------------------------------

class TestConvertedFilename:

    def test_strip_extension_for_none_target(self):
        assert FileConverter.get_converted_filename(
            "data.csv.gz", "gz", "none"
        ) == "data.csv"

    def test_add_extension_for_none_source(self):
        assert FileConverter.get_converted_filename(
            "data.csv", "none", "bz2"
        ) == "data.csv.bz2"

    def test_convert_between_formats(self):
        assert FileConverter.get_converted_filename(
            "dump.nt.bz2", "bz2", "xz"
        ) == "dump.nt.xz"


# ---------------------------------------------------------------------------
# _should_convert_file with 'none'
# ---------------------------------------------------------------------------

class TestShouldConvertFileNone:

    def test_decompress_compressed_file(self):
        ok, src = _should_convert_file("file.txt.gz", "none", None)
        assert ok is True
        assert src == "gz"

    def test_decompress_already_uncompressed(self):
        ok, src = _should_convert_file("file.txt", "none", None)
        assert ok is False

    def test_compress_raw_file(self):
        ok, src = _should_convert_file("file.txt", "gz", "none")
        assert ok is True
        assert src == "none"

    def test_decompress_with_filter_match(self):
        ok, src = _should_convert_file("file.txt.bz2", "none", "bz2")
        assert ok is True
        assert src == "bz2"

    def test_decompress_with_filter_no_match(self):
        ok, src = _should_convert_file("file.txt.bz2", "none", "xz")
        assert ok is False


# ---------------------------------------------------------------------------
# Checksum validation
# ---------------------------------------------------------------------------

class TestChecksumValidation:

    def test_valid_checksum(self):
        data = b"checksum test data"
        expected = hashlib.sha256(data).hexdigest()
        stream = io.BytesIO(data)
        assert FileConverter.validate_checksum_stream(stream, expected) is True

    def test_invalid_checksum_raises(self):
        stream = io.BytesIO(b"some data")
        with pytest.raises(ValueError, match="Checksum mismatch"):
            FileConverter.validate_checksum_stream(stream, "0" * 64)


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestErrorHandling:

    def test_unsupported_source_format(self):
        with pytest.raises(ValueError, match="Unsupported source"):
            FileConverter.convert_file("a", "b", "zip", "gz")

    def test_unsupported_target_format(self):
        with pytest.raises(ValueError, match="Unsupported target"):
            FileConverter.convert_file("a", "b", "gz", "rar")

    def test_corrupted_file_cleanup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            src = os.path.join(tmpdir, "bad.bz2")
            tgt = os.path.join(tmpdir, "out.gz")
            with open(src, "wb") as f:
                f.write(b"not real bz2 data")
            with pytest.raises(RuntimeError, match="Compression conversion failed"):
                FileConverter.convert_file(src, tgt, "bz2", "gz")
            assert not os.path.exists(tgt)


# ---------------------------------------------------------------------------
# CLI --decompress flag
# ---------------------------------------------------------------------------

class TestDecompressCLI:
    """Test the --decompress flag via Click's CliRunner.

    We don't actually download anything; we just verify the flag
    handling logic by patching the download function.
    """

    def test_decompress_flag_sets_convert_to_none(self, monkeypatch):
        """--decompress should result in convert_to='none' reaching api_download."""
        captured = {}

        def fake_download(**kwargs):
            captured.update(kwargs)

        monkeypatch.setattr(
            "databusclient.cli.api_download", fake_download
        )

        runner = CliRunner()
        result = runner.invoke(
            download,
            ["--decompress", "https://example.org/test"],
        )
        assert result.exit_code == 0, result.output
        assert captured.get("convert_to") == "none"

    def test_decompress_and_convert_to_conflict(self):
        runner = CliRunner()
        result = runner.invoke(
            download,
            ["--decompress", "--convert-to", "gz", "https://example.org/test"],
        )
        assert result.exit_code != 0
        assert "Cannot use --decompress together with --convert-to" in result.output

    def test_decompress_and_convert_from_conflict(self):
        runner = CliRunner()
        result = runner.invoke(
            download,
            ["--decompress", "--convert-from", "bz2", "https://example.org/test"],
        )
        assert result.exit_code != 0
        assert "Cannot use --decompress together with --convert-from" in result.output


# ---------------------------------------------------------------------------
# Streaming conversion: no intermediate files
# ---------------------------------------------------------------------------

class TestStreamingNoIntermediateFiles:
    """Verify that streaming conversion doesn't leave temp files behind."""

    def test_stream_decompress_no_temp(self):
        """Stream gz → none in memory, no disk artefacts."""
        gz_buf = io.BytesIO()
        with gzip.open(gz_buf, "wb") as gz:
            gz.write(_TEST_DATA)
        gz_buf.seek(0)

        raw_buf = io.BytesIO()
        FileConverter.convert_stream(gz_buf, raw_buf, "gz", "none")
        assert raw_buf.getvalue() == _TEST_DATA
        # BytesIO objects don't touch the filesystem at all

    def test_stream_compress_no_temp(self):
        """Stream none → bz2 in memory, no disk artefacts."""
        raw_buf = io.BytesIO(_TEST_DATA)
        bz2_buf = io.BytesIO()
        FileConverter.convert_stream(raw_buf, bz2_buf, "none", "bz2")
        bz2_buf.seek(0)
        assert bz2.decompress(bz2_buf.read()) == _TEST_DATA

    def test_stream_recompress_no_temp(self):
        """Stream bz2 → xz in memory, no disk artefacts."""
        bz2_buf = io.BytesIO()
        with bz2.open(bz2_buf, "wb") as bf:
            bf.write(_TEST_DATA)
        bz2_buf.seek(0)

        xz_buf = io.BytesIO()
        FileConverter.convert_stream(bz2_buf, xz_buf, "bz2", "xz")
        xz_buf.seek(0)
        with lzma.open(xz_buf, "rb") as xf:
            assert xf.read() == _TEST_DATA

    def test_file_convert_removes_source_only(self):
        """convert_file removes source but target persists; no temp files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = os.path.join(tmpdir, "data.txt.gz")
            tgt = os.path.join(tmpdir, "data.txt")
            with gzip.open(src, "wb") as f:
                f.write(_TEST_DATA)

            before = set(os.listdir(tmpdir))
            FileConverter.convert_file(src, tgt, "gz", "none")
            after = set(os.listdir(tmpdir))

            # Only the target should remain; source gone, nothing extra
            assert "data.txt.gz" not in after
            assert "data.txt" in after
            assert after - before == {"data.txt"}


# ---------------------------------------------------------------------------
# Checksum: mismatch raises ValueError, stream rewound
# ---------------------------------------------------------------------------

class TestChecksumBehaviour:

    def test_mismatch_raises_valueerror(self):
        stream = io.BytesIO(b"test data for checksum")
        with pytest.raises(ValueError, match="Checksum mismatch"):
            FileConverter.validate_checksum_stream(stream, "0" * 64)

    def test_stream_rewound_after_valid_check(self):
        data = b"rewind check"
        expected = hashlib.sha256(data).hexdigest()
        stream = io.BytesIO(data)
        FileConverter.validate_checksum_stream(stream, expected)
        # Stream should be at position 0 after validation
        assert stream.tell() == 0
        assert stream.read() == data

    def test_stream_rewound_after_invalid_check(self):
        stream = io.BytesIO(b"data")
        try:
            FileConverter.validate_checksum_stream(stream, "0" * 64)
        except ValueError:
            pass
        # Stream should still be at position 0 after failed validation
        assert stream.tell() == 0

    def test_compute_checksum_matches_raw_hash(self):
        """compute_checksum in convert_stream returns digest of decompressed bytes."""
        expected = hashlib.sha256(_TEST_DATA).hexdigest()

        bz2_buf = io.BytesIO()
        with bz2.open(bz2_buf, "wb") as bf:
            bf.write(_TEST_DATA)
        bz2_buf.seek(0)

        raw_buf = io.BytesIO()
        got = FileConverter.convert_stream(
            bz2_buf, raw_buf, "bz2", "none", compute_checksum=True
        )
        assert got == expected


# ---------------------------------------------------------------------------
# zstd (optional)
# ---------------------------------------------------------------------------

try:
    import zstandard  # noqa: F401
    _HAS_ZSTD = True
except ImportError:
    _HAS_ZSTD = False


@pytest.mark.skipif(not _HAS_ZSTD, reason="zstandard package not installed")
class TestZstd:

    def test_compress_none_to_zstd(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            raw = os.path.join(tmpdir, "data.txt")
            zst = os.path.join(tmpdir, "data.txt.zst")
            with open(raw, "wb") as f:
                f.write(_TEST_DATA)
            FileConverter.convert_file(raw, zst, "none", "zstd")
            assert not os.path.exists(raw)
            import zstandard as zd
            dctx = zd.ZstdDecompressor()
            with open(zst, "rb") as f:
                assert dctx.decompress(f.read()) == _TEST_DATA

    def test_decompress_zstd_to_none(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            raw = os.path.join(tmpdir, "data.txt")
            zst = os.path.join(tmpdir, "data.txt.zst")
            import zstandard as zd
            cctx = zd.ZstdCompressor()
            with open(zst, "wb") as f:
                f.write(cctx.compress(_TEST_DATA))
            FileConverter.convert_file(zst, raw, "zstd", "none")
            assert not os.path.exists(zst)
            with open(raw, "rb") as f:
                assert f.read() == _TEST_DATA


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
