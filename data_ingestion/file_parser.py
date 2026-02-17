"""
File Parser Module

Handles parsing of multiple file formats (CSV, JSON, XLSX, TXT)
and returns data in a standardized format.
"""

import pandas as pd
import json
import os
import chardet
from typing import Dict, List, Any, Optional
from pathlib import Path
from .supported_formats import FileFormat, SupportedFormats

class FileParser:
    """Main file parser that delegates to format-specific parsers."""
    
    def __init__(self):
        """Initialize the file parser."""
        self.supported_formats = SupportedFormats()
    
    def parse_file(self, file_path: str) -> Dict[str, Any]:
        """
        Parse a file and return standardized data structure.
        
        Args:
            file_path: Path to the file to parse
            
        Returns:
            Dictionary with standardized format:
            {
                "filename": str,
                "format": str,
                "tables": [
                    {
                        "name": str,
                        "columns": [{"name": str, "type": str, "sample_values": list}],
                        "row_count": int,
                        "data": list[dict]
                    }
                ],
                "metadata": dict
            }
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Determine file format
        extension = file_path.suffix.lower()
        file_format = self.supported_formats.get_format(extension)
        
        if not file_format:
            raise ValueError(f"Unsupported file format: {extension}")
        
        # Check file size
        file_size = file_path.stat().st_size
        max_size = self.supported_formats.get_max_file_size(file_format)
        if file_size > max_size:
            raise ValueError(f"File size ({file_size} bytes) exceeds maximum allowed ({max_size} bytes)")
        
        # Parse based on format
        if file_format in [FileFormat.CSV, FileFormat.TSV]:
            return self._parse_csv(file_path, file_format)
        elif file_format == FileFormat.JSON:
            return self._parse_json(file_path)
        elif file_format in [FileFormat.XLSX, FileFormat.XLS]:
            return self._parse_excel(file_path)
        elif file_format == FileFormat.TXT:
            return self._parse_txt(file_path)
        else:
            raise ValueError(f"Parser not implemented for format: {file_format}")
    
    def _detect_encoding(self, file_path: Path) -> str:
        """Detect file encoding using chardet."""
        with open(file_path, 'rb') as f:
            raw_data = f.read(10000)  # Read first 10KB
            result = chardet.detect(raw_data)
            return result['encoding'] or 'utf-8'
    
    def _infer_column_type(self, series: pd.Series) -> str:
        """Infer data type for a pandas Series."""
        if pd.api.types.is_integer_dtype(series):
            return "integer"
        elif pd.api.types.is_float_dtype(series):
            return "float"
        elif pd.api.types.is_bool_dtype(series):
            return "boolean"
        elif pd.api.types.is_datetime64_any_dtype(series):
            return "datetime"
        else:
            return "string"
    
    def _get_sample_values(self, series: pd.Series, max_samples: int = 5) -> List[Any]:
        """Get sample values from a series, excluding nulls."""
        non_null = series.dropna()
        samples = non_null.head(max_samples).tolist()
        # Convert numpy types to Python types
        return [x.item() if hasattr(x, 'item') else x for x in samples]
    
    def _df_to_table_dict(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """Convert pandas DataFrame to standardized table dictionary."""
        columns = []
        for col in df.columns:
            columns.append({
                "name": col,
                "type": self._infer_column_type(df[col]),
                "sample_values": self._get_sample_values(df[col]),
                "null_count": int(df[col].isna().sum()),
                "unique_count": int(df[col].nunique())
            })
        
        # Convert DataFrame to list of dicts, handling NaN values
        data = df.where(pd.notna(df), None).to_dict('records')
        
        return {
            "name": table_name,
            "columns": columns,
            "row_count": len(df),
            "data": data
        }
    
    def _parse_csv(self, file_path: Path, file_format: FileFormat) -> Dict[str, Any]:
        """Parse CSV/TSV file."""
        encoding = self._detect_encoding(file_path)
        delimiter = '\t' if file_format == FileFormat.TSV else None  # Let pandas auto-detect for CSV
        
        try:
            df = pd.read_csv(
                file_path,
                encoding=encoding,
                delimiter=delimiter,
                low_memory=False,
                parse_dates=True,
                
            )
        except Exception as e:
            raise ValueError(f"Error parsing CSV file: {str(e)}")
        
        table_name = file_path.stem  # Use filename without extension
        
        return {
            "filename": file_path.name,
            "format": file_format.value,
            "tables": [self._df_to_table_dict(df, table_name)],
            "metadata": {
                "encoding": encoding,
                "delimiter": delimiter or "auto-detected",
                "file_size_bytes": file_path.stat().st_size
            }
        }
    
    def _parse_json(self, file_path: Path) -> Dict[str, Any]:
        """Parse JSON file."""
        encoding = self._detect_encoding(file_path)
        
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            # Try JSON Lines format
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    data = [json.loads(line) for line in f]
            except Exception:
                raise ValueError(f"Error parsing JSON file: {str(e)}")
        
        # Convert to DataFrame
        if isinstance(data, list):
            # List of objects
            df = pd.json_normalize(data)
            table_name = file_path.stem
            tables = [self._df_to_table_dict(df, table_name)]
        elif isinstance(data, dict):
            # Check if it contains multiple tables
            tables = []
            for key, value in data.items():
                if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                    # This looks like a table
                    df = pd.json_normalize(value)
                    tables.append(self._df_to_table_dict(df, key))
            
            # If no tables found, treat entire dict as single record
            if not tables:
                df = pd.json_normalize([data])
                tables = [self._df_to_table_dict(df, file_path.stem)]
        else:
            raise ValueError("JSON must be a list or object")
        
        return {
            "filename": file_path.name,
            "format": FileFormat.JSON.value,
            "tables": tables,
            "metadata": {
                "encoding": encoding,
                "file_size_bytes": file_path.stat().st_size
            }
        }
    
    def _parse_excel(self, file_path: Path) -> Dict[str, Any]:
        """Parse Excel file (XLSX/XLS)."""
        try:
            # Read all sheets
            excel_file = pd.ExcelFile(file_path)
            tables = []
            
            for sheet_name in excel_file.sheet_names:
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                # Skip empty sheets
                if not df.empty:
                    tables.append(self._df_to_table_dict(df, sheet_name))
            
            if not tables:
                raise ValueError("No data found in Excel file")
            
        except Exception as e:
            raise ValueError(f"Error parsing Excel file: {str(e)}")
        
        return {
            "filename": file_path.name,
            "format": FileFormat.XLSX.value,
            "tables": tables,
            "metadata": {
                "sheet_count": len(excel_file.sheet_names),
                "sheet_names": excel_file.sheet_names,
                "file_size_bytes": file_path.stat().st_size
            }
        }
    
    def _parse_txt(self, file_path: Path) -> Dict[str, Any]:
        """Parse TXT file by attempting to detect structure."""
        encoding = self._detect_encoding(file_path)
        
        # Try to determine if it's delimited
        with open(file_path, 'r', encoding=encoding) as f:
            first_lines = [f.readline() for _ in range(5)]
        
        # Try common delimiters
        for delimiter in [',', '\t', '|', ';', ' ']:
            if all(delimiter in line for line in first_lines if line.strip()):
                # Looks delimited, try to parse as CSV
                try:
                    df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding)
                    if df.shape[1] > 1:  # More than one column
                        return {
                            "filename": file_path.name,
                            "format": FileFormat.TXT.value,
                            "tables": [self._df_to_table_dict(df, file_path.stem)],
                            "metadata": {
                                "encoding": encoding,
                                "detected_delimiter": delimiter,
                                "file_size_bytes": file_path.stat().st_size
                            }
                        }
                except Exception:
                    continue
        
        # If no delimiter detected, read as single column of text
        with open(file_path, 'r', encoding=encoding) as f:
            lines = [line.strip() for line in f if line.strip()]
        
        df = pd.DataFrame({'text': lines})
        
        return {
            "filename": file_path.name,
            "format": FileFormat.TXT.value,
            "tables": [self._df_to_table_dict(df, file_path.stem)],
            "metadata": {
                "encoding": encoding,
                "detected_delimiter": "none",
                "file_size_bytes": file_path.stat().st_size
            }
        }
