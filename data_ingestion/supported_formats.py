"""
Registry of supported file types and their configurations.
"""

from enum import Enum
from typing import Dict, List

class FileFormat(Enum):
    """Enumeration of supported file formats."""
    CSV = "csv"
    JSON = "json"
    XLSX = "xlsx"
    XLS = "xls"
    TXT = "txt"
    TSV = "tsv"

class SupportedFormats:
    """Registry of supported file formats and their properties."""
    
    # Map of file extensions to FileFormat
    EXTENSION_MAP = {
        '.csv': FileFormat.CSV,
        '.json': FileFormat.JSON,
        '.jsonl': FileFormat.JSON,
        '.xlsx': FileFormat.XLSX,
        '.xls': FileFormat.XLS,
        '.txt': FileFormat.TXT,
        '.tsv': FileFormat.TSV,
    }
    
    # MIME type mappings
    MIME_TYPE_MAP = {
        'text/csv': FileFormat.CSV,
        'application/json': FileFormat.JSON,
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': FileFormat.XLSX,
        'application/vnd.ms-excel': FileFormat.XLS,
        'text/plain': FileFormat.TXT,
        'text/tab-separated-values': FileFormat.TSV,
    }
    
    # Maximum file sizes (in bytes)
    MAX_FILE_SIZES = {
        FileFormat.CSV: 100 * 1024 * 1024,  # 100 MB
        FileFormat.JSON: 100 * 1024 * 1024,  # 100 MB
        FileFormat.XLSX: 50 * 1024 * 1024,   # 50 MB
        FileFormat.XLS: 50 * 1024 * 1024,    # 50 MB
        FileFormat.TXT: 100 * 1024 * 1024,   # 100 MB
        FileFormat.TSV: 100 * 1024 * 1024,   # 100 MB
    }
    
    @classmethod
    def is_supported(cls, extension: str) -> bool:
        """Check if a file extension is supported."""
        ext = extension.lower()
        if not ext.startswith('.'):
            ext = f'.{ext}'
        return ext in cls.EXTENSION_MAP
    
    @classmethod
    def get_format(cls, extension: str) -> FileFormat:
        """Get FileFormat for a given extension."""
        ext = extension.lower()
        if not ext.startswith('.'):
            ext = f'.{ext}'
        return cls.EXTENSION_MAP.get(ext)
    
    @classmethod
    def get_supported_extensions(cls) -> List[str]:
        """Get list of all supported file extensions."""
        return list(cls.EXTENSION_MAP.keys())
    
    @classmethod
    def get_max_file_size(cls, file_format: FileFormat) -> int:
        """Get maximum allowed file size for a format."""
        return cls.MAX_FILE_SIZES.get(file_format, 10 * 1024 * 1024)  # Default 10MB
