"""
Data Ingestion Module

This module handles file uploads, parsing multiple data formats,
and analyzing data structure for automated database creation.
"""

from .file_parser import FileParser
from .file_analyzer import FileAnalyzer
from .supported_formats import SupportedFormats

__all__ = ['FileParser', 'FileAnalyzer', 'SupportedFormats']
