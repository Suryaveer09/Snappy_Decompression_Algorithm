from dataclasses import dataclass, field
from typing import Dict, Any, Tuple, BinaryIO
from io import BytesIO


# -------------------------------------------------------------------------
# Class: FileFormatDetection
# -------------------------------------------------------------------------
@dataclass
class FileFormatDetection:
    format: str                 # e.g., "gzip", "parquet", "orc", "unknown"
    confidence: float           # 0.0..1.0
    evidence: str               # why it was classified
    extra: Dict[str, Any] = field(default_factory=dict)

    # ---------- Introspection Helpers ----------
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
def _read_ranges(fp: BinaryIO, head_n=64, tail_n=64,
                 tar_probe_offset=257, tar_probe_len=8) -> Tuple[bytes, bytes, bytes]:
    """
    Read head, tail, and a slice around TAR's magic position.
    Works for files and file-like objects supporting seek().
    """
    # head
    fp.seek(0, 0)
    head = fp.read(head_n)

    # tail
    fp.seek(0, 2)
    size = fp.tell()
    tail_len = min(tail_n, size)
    fp.seek(size - tail_len, 0)
    tail = fp.read(tail_len)

    # tar magic at offset 257
    tar_slice = b""
    if size >= tar_probe_offset + tar_probe_len:
        fp.seek(tar_probe_offset, 0)
        tar_slice = fp.read(tar_probe_len)

    return head, tail, tar_slice


def _detect_from_ranges(head: bytes, tail: bytes, tar_slice: bytes) -> FileFormatDetection:
    """Apply signature checks to the extracted byte ranges."""
    def starts_with_any(buf: bytes, patterns) -> bool:
        return any(buf.startswith(p) for p in patterns)

    def ends_with_any(buf: bytes, patterns) -> bool:
        return any(buf.endswith(p) for p in patterns)

    # 1) Parquet
    s = SIGNATURES["parquet"]
    if starts_with_any(head, s["start"]) and ends_with_any(tail, s["end"]):
        return FileFormatDetection("parquet", 1.0, s["evidence"], {})

    # 2) Start-only signatures
    for name in ["gzip", "zstd", "bzip2", "lz4-frame", "xz", "zip", "7z", "snappy-framed"]:
        s = SIGNATURES[name]
        if starts_with_any(head, s["start"]):
            return FileFormatDetection(name, 1.0, s["evidence"], {})

    # 3) TAR (magic at offset 257)
    s = SIGNATURES["tar"]
    if any(tar_slice.startswith(p) for p in s["tar_magic"]):
        return FileFormatDetection("tar", 0.95, s["evidence"], {})

    # 4) ORC (postscript marker near tail)
    s = SIGNATURES["orc"]
    if any(p in tail[-16:] for p in s["end_contains"]):
        return FileFormatDetection("orc", 0.9, s["evidence"], {})

    # 5) Unknown
    return FileFormatDetection("unknown", 0.0, "No decisive signature found.", {})


# -------------------------------------------------------------------------
# Main Function: sniff_format (filepath-based)
# -------------------------------------------------------------------------
def sniff_format(file_path: str) -> FileFormatDetection:
    """
    Identify common compression/archival/columnar formats by signatures.
    Returns a FileFormatDetection object.
    """
    try:
        with open(file_path, "rb") as fp:
            head, tail, tar_slice = _read_ranges(fp)
    except FileNotFoundError:
        return FileFormatDetection("unknown", 0.0, "File not found.", {})
    except Exception as e:
        return FileFormatDetection("unknown", 0.0, f"I/O error: {e}", {})

    return _detect_from_ranges(head, tail, tar_slice)


# -------------------------------------------------------------------------
# New Function: sniff_stream (file-like object)
# -------------------------------------------------------------------------
def sniff_stream(stream: BinaryIO) -> FileFormatDetection:
    """
    Identify format from a file-like object.
    - If the stream is seekable, read ranges in-place.
    - If not seekable, buffer into a BytesIO and then sniff.
    """
    try:
        # Prefer in-place if seekable
        if hasattr(stream, "seekable") and stream.seekable():
            try:
                pos = stream.tell()
            except Exception:
                pos = None

            head, tail, tar_slice = _read_ranges(stream)

            # restore original position best-effort
            if pos is not None:
                try:
                    stream.seek(pos, 0)
                except Exception:
                    pass
        else:
            # Non-seekable: buffer the content
            data = stream.read()
            fp = BytesIO(data)
            head, tail, tar_slice = _read_ranges(fp)

    except Exception as e:
        return FileFormatDetection("unknown", 0.0, f"Stream read error: {e}", {})

    return _detect_from_ranges(head, tail, tar_slice)
