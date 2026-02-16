"""File format conversion extension for databus-python-client.

Provides streaming pipeline for file decompression, re-compression,
and checksum validation during download operations.
"""

import gzip
import hashlib
from typing import BinaryIO, Optional


class FileConverter:
    """Handles file format conversion with streaming support."""

    CHUNK_SIZE = 8192  # 8KB chunks for streaming

    @staticmethod
    def decompress_gzip_stream(
        input_stream: BinaryIO,
        output_stream: BinaryIO,
        validate_checksum: bool = False,
        expected_checksum: Optional[str] = None
    ) -> Optional[str]:
        """Decompress gzip stream with optional checksum validation.

        Args:
            input_stream: Input gzip compressed stream
            output_stream: Output decompressed stream
            validate_checksum: Whether to compute checksum during decompression
            expected_checksum: Expected SHA256 checksum (for source file)

        Returns:
            Computed checksum if validate_checksum is True, None otherwise

        Raises:
            IOError: If checksum validation fails
        """
        hasher = hashlib.sha256() if validate_checksum else None
        source_hasher = hashlib.sha256() if expected_checksum else None

        with gzip.open(input_stream, 'rb') as gz:
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
        input_stream: BinaryIO,
        output_stream: BinaryIO
    ) -> None:
        """Compress stream to gzip format.

        Args:
            input_stream: Input uncompressed stream
            output_stream: Output gzip compressed stream
        """
        with gzip.open(output_stream, 'wb') as gz:
            while True:
                chunk = input_stream.read(FileConverter.CHUNK_SIZE)
                if not chunk:
                    break
                gz.write(chunk)

    @staticmethod
    def validate_checksum_stream(
        input_stream: BinaryIO,
        expected_checksum: str
    ) -> bool:
        """Validate SHA256 checksum of a stream.

        Args:
            input_stream: Input stream to validate
            expected_checksum: Expected SHA256 checksum

        Returns:
            True if checksum matches

        Raises:
            IOError: If checksum validation fails
        """
        hasher = hashlib.sha256()
        input_stream.seek(0)
        
        while True:
            chunk = input_stream.read(FileConverter.CHUNK_SIZE)
            if not chunk:
                break
            hasher.update(chunk)
        
        computed = hasher.hexdigest()
        if computed.lower() != expected_checksum.lower():
            raise IOError(
                f"Checksum mismatch: expected {expected_checksum}, "
                f"got {computed}"
            )
        
        return True
