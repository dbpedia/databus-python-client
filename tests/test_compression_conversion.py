"""Tests for on-the-fly compression conversion feature"""

import os
import gzip
import bz2
import lzma
import tempfile
import pytest
from databusclient.api.download import (
    _detect_compression_format,
    _should_convert_file,
    _get_converted_filename,
    _convert_compression_format,
)


def test_detect_compression_format():
    """Test compression format detection from filenames"""
    assert _detect_compression_format("file.txt.bz2") == "bz2"
    assert _detect_compression_format("file.txt.gz") == "gz"
    assert _detect_compression_format("file.txt.xz") == "xz"
    assert _detect_compression_format("file.txt") is None
    assert _detect_compression_format("FILE.TXT.GZ") == "gz"  # case insensitive


def test_should_convert_file():
    """Test file conversion decision logic"""
    # No conversion target specified
    should_convert, source = _should_convert_file("file.txt.bz2", None, None)
    assert should_convert is False
    assert source is None

    # Uncompressed file
    should_convert, source = _should_convert_file("file.txt", "gz", None)
    assert should_convert is False
    assert source is None

    # Same source and target
    should_convert, source = _should_convert_file("file.txt.gz", "gz", None)
    assert should_convert is False
    assert source is None

    # Valid conversion
    should_convert, source = _should_convert_file("file.txt.bz2", "gz", None)
    assert should_convert is True
    assert source == "bz2"

    # With convert_from filter matching
    should_convert, source = _should_convert_file("file.txt.bz2", "gz", "bz2")
    assert should_convert is True
    assert source == "bz2"

    # With convert_from filter not matching
    should_convert, source = _should_convert_file("file.txt.bz2", "gz", "xz")
    assert should_convert is False
    assert source is None


def test_get_converted_filename():
    """Test filename conversion"""
    assert _get_converted_filename("data.txt.bz2", "bz2", "gz") == "data.txt.gz"
    assert _get_converted_filename("data.txt.gz", "gz", "xz") == "data.txt.xz"
    assert _get_converted_filename("data.txt.xz", "xz", "bz2") == "data.txt.bz2"


def test_convert_compression_format():
    """Test actual compression format conversion"""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        test_data = b"This is test data for compression conversion " * 100
        
        # Create a bz2 file
        bz2_file = os.path.join(tmpdir, "test.txt.bz2")
        with bz2.open(bz2_file, 'wb') as f:
            f.write(test_data)
        
        # Convert bz2 to gz
        gz_file = os.path.join(tmpdir, "test.txt.gz")
        _convert_compression_format(bz2_file, gz_file, "bz2", "gz")
        
        # Verify the original file was removed
        assert not os.path.exists(bz2_file)
        
        # Verify the new file exists and contains the same data
        assert os.path.exists(gz_file)
        with gzip.open(gz_file, 'rb') as f:
            decompressed = f.read()
        assert decompressed == test_data


def test_convert_gz_to_xz():
    """Test conversion from gzip to xz"""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        test_data = b"Conversion test: gz to xz format" * 50
        
        # Create a gz file
        gz_file = os.path.join(tmpdir, "test.txt.gz")
        with gzip.open(gz_file, 'wb') as f:
            f.write(test_data)
        
        # Convert gz to xz
        xz_file = os.path.join(tmpdir, "test.txt.xz")
        _convert_compression_format(gz_file, xz_file, "gz", "xz")
        
        # Verify conversion
        assert not os.path.exists(gz_file)
        assert os.path.exists(xz_file)
        with lzma.open(xz_file, 'rb') as f:
            decompressed = f.read()
        assert decompressed == test_data


def test_convert_xz_to_bz2():
    """Test conversion from xz to bz2"""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test data
        test_data = b"XZ to BZ2 compression conversion test" * 75
        
        # Create an xz file
        xz_file = os.path.join(tmpdir, "test.txt.xz")
        with lzma.open(xz_file, 'wb') as f:
            f.write(test_data)
        
        # Convert xz to bz2
        bz2_file = os.path.join(tmpdir, "test.txt.bz2")
        _convert_compression_format(xz_file, bz2_file, "xz", "bz2")
        
        # Verify conversion
        assert not os.path.exists(xz_file)
        assert os.path.exists(bz2_file)
        with bz2.open(bz2_file, 'rb') as f:
            decompressed = f.read()
        assert decompressed == test_data


def test_case_insensitive_filename_conversion():
    """Test that uppercase extensions are handled correctly (addresses PR feedback)"""
    # Test uppercase extension matching
    assert _get_converted_filename("FILE.BZ2", "bz2", "gz") == "FILE.gz"
    assert _get_converted_filename("data.GZ", "gz", "xz") == "data.xz"
    assert _get_converted_filename("archive.XZ", "xz", "bz2") == "archive.bz2"
    
    # Test mixed case
    assert _get_converted_filename("File.Bz2", "bz2", "gz") == "File.gz"


def test_invalid_source_format_validation():
    """Test that invalid source format raises ValueError (addresses PR feedback)"""
    with tempfile.TemporaryDirectory() as tmpdir:
        source_file = os.path.join(tmpdir, "test.zip")
        target_file = os.path.join(tmpdir, "test.gz")
        
        # Create a dummy file
        with open(source_file, 'wb') as f:
            f.write(b"test data")
        
        # Should raise ValueError for unsupported format
        with pytest.raises(ValueError, match="Unsupported source compression format"):
            _convert_compression_format(source_file, target_file, "zip", "gz")


def test_invalid_target_format_validation():
    """Test that invalid target format raises ValueError (addresses PR feedback)"""
    with tempfile.TemporaryDirectory() as tmpdir:
        source_file = os.path.join(tmpdir, "test.gz")
        target_file = os.path.join(tmpdir, "test.rar")
        
        # Create a valid gz file
        test_data = b"test data"
        with gzip.open(source_file, 'wb') as f:
            f.write(test_data)
        
        # Should raise ValueError for unsupported format
        with pytest.raises(ValueError, match="Unsupported target compression format"):
            _convert_compression_format(source_file, target_file, "gz", "rar")


def test_corrupted_file_handling():
    """Test that corrupted files are handled gracefully and target file is cleaned up"""
    with tempfile.TemporaryDirectory() as tmpdir:
        source_file = os.path.join(tmpdir, "corrupted.bz2")
        target_file = os.path.join(tmpdir, "target.gz")
        
        # Create a file with .bz2 extension but invalid content
        with open(source_file, 'wb') as f:
            f.write(b"This is not valid bz2 compressed data")
        
        # Should raise RuntimeError
        with pytest.raises(RuntimeError, match="Compression conversion failed"):
            _convert_compression_format(source_file, target_file, "bz2", "gz")
        
        # Verify target file was cleaned up
        assert not os.path.exists(target_file)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
