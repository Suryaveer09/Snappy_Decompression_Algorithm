from dataclasses import dataclass, field
from typing import Dict, Any, Tuple, BinaryIO, Optional
from io import BytesIO
import os

_version_ = "0.1.0"
_author_ = "Your Name"  # Replace with your name or organization
_license_ = "MIT"  # Or whichever license you prefer

# -------------------------------------------------------------------------
# Class: FileFormatDetection
# -------------------------------------------------------------------------
@dataclass
class FileFormatDetection:
    """
    Represents the result of file format detection.
    
    Attributes:
        format: The detected format (e.g., "gzip", "parquet", "unknown").
        confidence: A float between 0.0 and 1.0 indicating detection confidence.
        evidence: A string explaining why the format was classified as such.
        extra: Optional dictionary for additional metadata.
    """
    format: str
    confidence: float
    evidence: str
    extra: Dict[str, Any] = field(default_factory=dict)

    def is_known(self) -> bool:
        """Return True if the format is identified with nonzero confidence."""
        return self.format.lower() != "unknown" and self.confidence > 0.0

    def is_compressed(self) -> bool:
        """Return True if this format is a compression algorithm."""
        return self.format.lower() in {
            "gzip", "zstd", "bzip2", "lz4-frame",
            "xz", "snappy-framed", "brotli"
        }

    def is_archive(self) -> bool:
        """Return True if this format is an archive container."""
        return self.format.lower() in {"zip", "7z", "tar"}

    def is_columnar(self) -> bool:
        """Return True if this format is a columnar storage format."""
        return self.format.lower() in {"parquet", "orc"}

    def summary(self) -> str:
        """Return a concise human-readable summary of the detection."""
        return f"[{self.format.upper()}] confidence={self.confidence:.2f} – {self.evidence}"

    def metadata(self) -> Dict[str, Any]:
        """
        Return all stored metadata in a clean dict.
        Useful for logging, JSON export, etc.
        """
        return {
            "format": self.format,
            "confidence": self.confidence,
            "evidence": self.evidence,
            "extra": self.extra,
            "is_compressed": self.is_compressed(),
            "is_archive": self.is_archive(),
            "is_columnar": self.is_columnar(),
        }


# -------------------------------------------------------------------------
# Shared signature table (used by both sniff_format and sniff_stream)
# -------------------------------------------------------------------------
SIGNATURES = {
    "gzip": {
        "start": [b"\x1f\x8b"],  # 1f 8b
        "evidence": "Starts with 1f 8b (gzip).",
    },
    "zstd": {
        "start": [b"\x28\xb5\x2f\xfd"],
        "evidence": "Starts with 28 b5 2f fd (zstd).",
    },
    "bzip2": {
        "start": [b"BZh"],
        "evidence": "Starts with 'BZh' (bzip2).",
    },
    "lz4-frame": {
        "start": [b"\x04\x22\x4d\x18"],
        "evidence": "Starts with 04 22 4d 18 (LZ4 frame).",
    },
    "xz": {
        "start": [b"\xfd7zXZ\x00"],
        "evidence": "Starts with fd 37 7a 58 5a 00 (XZ).",
    },
    "zip": {
        "start": [b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"],
        "evidence": "Starts with PK (ZIP container).",
    },
    "7z": {
        "start": [b"\x37\x7a\xbc\xaf\x27\x1c"],
        "evidence": "Starts with 37 7a bc af 27 1c (7z archive).",
    },
    "snappy-framed": {
        "start": [b"\xff\x06\x00\x00sNaPpY"],
        "evidence": "Starts with ff 06 00 00 'sNaPpY' (Snappy framed).",
    },
    "brotli": {
        # Brotli doesn't have a fixed magic number, but we can check for common patterns.
        # This is a heuristic: first byte often 0x91-0x9F for compressed data.
        "heuristic": lambda head: head and 0x91 <= head[0] <= 0x9F,
        "evidence": "First byte in range 0x91-0x9F (Brotli heuristic).",
        "confidence": 0.5,  # Lower confidence since it's heuristic
    },
    "parquet": {
        "start": [b"PAR1"],
        "end": [b"PAR1"],
        "evidence": "Has 'PAR1' at start and end (Parquet).",
    },
    "orc": {
        "end_contains": [b"ORC"],
        "evidence": "Tail contains 'ORC' in postscript (ORC).",
    },
    "tar": {
        "tar_magic": [b"ustar\x00", b"ustar  "],
        "evidence": "Has 'ustar' at offset 257 (TAR).",
    },
}


# -------------------------------------------------------------------------
# Helpers for signature sniffing
# -------------------------------------------------------------------------
def _read_ranges(fp: BinaryIO, head_n: int = 64, tail_n: int = 64,
                 tar_probe_offset: int = 257, tar_probe_len: int = 8) -> Tuple[bytes, bytes, bytes]:
    """
    Read head, tail, and a slice around TAR's magic position from a seekable stream.
    
    Args:
        fp: A seekable binary file-like object.
        head_n: Number of bytes to read from the start.
        tail_n: Number of bytes to read from the end.
        tar_probe_offset: Offset for TAR magic check.
        tar_probe_len: Length of TAR magic slice.
    
    Returns:
        Tuple of (head bytes, tail bytes, tar_slice bytes).
    
    Raises:
        ValueError: If the stream is not seekable.
    """
    if not (hasattr(fp, "seekable") and fp.seekable()):
        raise ValueError("Stream must be seekable for _read_ranges.")

    # Get file size
    fp.seek(0, os.SEEK_END)
    size = fp.tell()
    fp.seek(0, os.SEEK_SET)

    # Read head
    head = fp.read(head_n)

    # Read tail
    tail_len = min(tail_n, size)
    fp.seek(max(0, size - tail_len), os.SEEK_SET)
    tail = fp.read(tail_len)

    # Read TAR slice
    tar_slice = b""
    if size >= tar_probe_offset + tar_probe_len:
        fp.seek(tar_probe_offset, os.SEEK_SET)
        tar_slice = fp.read(tar_probe_len)

    return head, tail, tar_slice


def _detect_from_ranges(head: bytes, tail: bytes, tar_slice: bytes) -> FileFormatDetection:
    """Apply signature checks to the extracted byte ranges."""
    def starts_with_any(buf: bytes, patterns: list) -> bool:
        return any(buf.startswith(p) for p in patterns if p)

    def ends_with_any(buf: bytes, patterns: list) -> bool:
        return any(buf.endswith(p) for p in patterns if p)

    def contains_any(buf: bytes, patterns: list) -> bool:
        return any(p in buf for p in patterns if p)

    # Check in priority order (more specific first)
    for fmt, sig in SIGNATURES.items():
        confidence = sig.get("confidence", 1.0)

        if "heuristic" in sig:
            if sig["heuristic"](head):
                return FileFormatDetection(fmt, confidence, sig["evidence"], {})
            continue

        match = True

        if "start" in sig and not starts_with_any(head, sig["start"]):
            match = False
        if "end" in sig and not ends_with_any(tail, sig["end"]):
            match = False
        if "end_contains" in sig and not contains_any(tail, sig["end_contains"]):
            match = False
        if "tar_magic" in sig and not starts_with_any(tar_slice, sig["tar_magic"]):
            match = False

        if match:
            return FileFormatDetection(fmt, confidence, sig["evidence"], {})

    # Unknown
    return FileFormatDetection("unknown", 0.0, "No decisive signature found.", {})


# -------------------------------------------------------------------------
# Main Function: sniff_format (filepath-based)
# -------------------------------------------------------------------------
def sniff_format(file_path: str, head_n: int = 64, tail_n: int = 64) -> FileFormatDetection:
    """
    Identify common compression/archival/columnar formats by signatures from a file path.
    
    Args:
        file_path: Path to the file.
        head_n: Bytes to read from the start (default: 64).
        tail_n: Bytes to read from the end (default: 64).
    
    Returns:
        FileFormatDetection object.
    """
    if not os.path.exists(file_path):
        return FileFormatDetection("unknown", 0.0, "File not found.", {})
    
    try:
        with open(file_path, "rb") as fp:
            head, tail, tar_slice = _read_ranges(fp, head_n, tail_n)
            return _detect_from_ranges(head, tail, tar_slice)
    except Exception as e:
        return FileFormatDetection("unknown", 0.0, f"I/O error: {str(e)}", {})


# -------------------------------------------------------------------------
# Function: sniff_stream (file-like object)
# -------------------------------------------------------------------------
def sniff_stream(stream: BinaryIO, head_n: int = 64, tail_n: int = 64,
                 buffer_non_seekable: bool = True) -> FileFormatDetection:
    """
    Identify format from a file-like object.
    
    - If seekable, reads ranges in-place without consuming the stream.
    - If not seekable and buffer_non_seekable=True, buffers the entire stream into memory.
    - If not seekable and buffer_non_seekable=False, attempts partial detection (head only).
    
    Args:
        stream: Binary file-like object.
        head_n: Bytes to read from the start.
        tail_n: Bytes to read from the end.
        buffer_non_seekable: Whether to buffer non-seekable streams (may consume memory).
    
    Returns:
        FileFormatDetection object.
    
    Warning:
        For large non-seekable streams, buffering may use significant memory.
    """
    try:
        if hasattr(stream, "seekable") and stream.seekable():
            original_pos = stream.tell()
            try:
                head, tail, tar_slice = _read_ranges(stream, head_n, tail_n)
                return _detect_from_ranges(head, tail, tar_slice)
            finally:
                stream.seek(original_pos, os.SEEK_SET)
        else:
            if not buffer_non_seekable:
                # Partial detection: only head-based formats
                head = stream.read(head_n)
                # Mock tail and tar_slice as empty (limits detection)
                partial_detection = _detect_from_ranges(head, b"", b"")
                if partial_detection.is_known():
                    partial_detection.confidence *= 0.8  # Reduce confidence for partial
                    partial_detection.evidence += " (partial detection from head only)."
                return partial_detection
            
            # Buffer entire stream
            data = stream.read()
            with BytesIO(data) as fp:
                head, tail, tar_slice = _read_ranges(fp, head_n, tail_n)
                return _detect_from_ranges(head, tail, tar_slice)
    except Exception as e:
        return FileFormatDetection("unknown", 0.0, f"Stream read error: {str(e)}", {})


# -------------------------------------------------------------------------
# Extension API: Add custom signature
# -------------------------------------------------------------------------
def add_signature(format_name: str, signature: Dict[str, Any], overwrite: bool = False) -> None:
    """
    Add or update a custom signature to the SIGNATURES table.
    
    Args:
        format_name: Name of the format (e.g., "custom").
        signature: Dictionary with keys like 'start', 'end', etc., matching SIGNATURES structure.
        overwrite: If True, overwrite existing signature.
    
    Raises:
        ValueError: If format_name exists and overwrite=False.
    """
    if format_name in SIGNATURES and not overwrite:
        raise ValueError(f"Signature for '{format_name}' already exists. Use overwrite=True to update.")
    SIGNATURES[format_name] = signature