"""
Simple unit tests for the data ingestion module.
Run with: pytest tests/test_data_ingestion.py -v
"""

import pytest
import json
from pathlib import Path
import tempfile
import shutil

from data_ingestion.file_parser import FileParser
from data_ingestion.file_analyzer import FileAnalyzer
from data_ingestion.supported_formats import SupportedFormats, FileFormat

class TestSupportedFormats:
    """Test the supported formats registry."""
    
    def test_csv_is_supported(self):
        assert SupportedFormats.is_supported('.csv')
        assert SupportedFormats.is_supported('csv')
    
    def test_json_is_supported(self):
        assert SupportedFormats.is_supported('.json')
        assert SupportedFormats.get_format('.json') == FileFormat.JSON
    
    def test_unsupported_format(self):
        assert not SupportedFormats.is_supported('.pdf')
        assert SupportedFormats.get_format('.pdf') is None
    
    def test_get_supported_extensions(self):
        extensions = SupportedFormats.get_supported_extensions()
        assert '.csv' in extensions
        assert '.json' in extensions
        assert '.xlsx' in extensions

class TestFileParser:
    """Test file parsing functionality."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def parser(self):
        return FileParser()
    
    def test_parse_simple_csv(self, parser, temp_dir):
        """Test parsing a simple CSV file."""
        csv_file = temp_dir / "test.csv"
        csv_file.write_text("name,age,city\nAlice,30,Boston\nBob,25,NYC\n")
        
        result = parser.parse_file(str(csv_file))
        
        assert result['filename'] == 'test.csv'
        assert result['format'] == 'csv'
        assert len(result['tables']) == 1
        
        table = result['tables'][0]
        assert table['name'] == 'test'
        assert table['row_count'] == 2
        assert len(table['columns']) == 3
        assert table['data'][0]['name'] == 'Alice'
    
    def test_parse_json_array(self, parser, temp_dir):
        """Test parsing JSON array."""
        json_file = temp_dir / "test.json"
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]
        json_file.write_text(json.dumps(data))
        
        result = parser.parse_file(str(json_file))
        
        assert result['format'] == 'json'
        assert len(result['tables']) == 1
        assert result['tables'][0]['row_count'] == 2
    
    def test_parse_json_object_with_tables(self, parser, temp_dir):
        """Test parsing JSON object with multiple tables."""
        json_file = temp_dir / "test.json"
        data = {
            "users": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"}
            ],
            "posts": [
                {"id": 1, "user_id": 1, "title": "Hello"}
            ]
        }
        json_file.write_text(json.dumps(data))
        
        result = parser.parse_file(str(json_file))
        
        assert len(result['tables']) == 2
        table_names = [t['name'] for t in result['tables']]
        assert 'users' in table_names
        assert 'posts' in table_names
    
    def test_invalid_file(self, parser):
        """Test error handling for non-existent file."""
        with pytest.raises(FileNotFoundError):
            parser.parse_file("/nonexistent/file.csv")
    
    def test_unsupported_format(self, parser, temp_dir):
        """Test error handling for unsupported format."""
        unsupported_file = temp_dir / "test.pdf"
        unsupported_file.write_text("fake pdf content")
        
        with pytest.raises(ValueError, match="Unsupported file format"):
            parser.parse_file(str(unsupported_file))

class TestFileAnalyzer:
    """Test file analysis functionality."""
    
    @pytest.fixture
    def analyzer(self):
        return FileAnalyzer()
    
    def test_analyze_single_file(self, analyzer):
        """Test analyzing a single file."""
        parsed_files = [{
            "filename": "test.csv",
            "format": "csv",
            "tables": [{
                "name": "users",
                "row_count": 3,
                "columns": [
                    {"name": "id", "type": "integer", "null_count": 0, "unique_count": 3},
                    {"name": "name", "type": "string", "null_count": 0, "unique_count": 3},
                    {"name": "age", "type": "integer", "null_count": 1, "unique_count": 2}
                ],
                "data": []
            }],
            "metadata": {}
        }]
        
        analysis = analyzer.analyze_files(parsed_files)
        
        assert analysis['total_files'] == 1
        assert analysis['total_tables'] == 1
        assert analysis['total_rows'] == 3
        assert len(analysis['file_summaries']) == 1
    
    def test_suggest_primary_key(self, analyzer):
        """Test primary key suggestion."""
        table = {
            "name": "users",
            "row_count": 3,
            "columns": [
                {"name": "id", "type": "integer", "null_count": 0, "unique_count": 3},
                {"name": "name", "type": "string", "null_count": 0, "unique_count": 3},
                {"name": "email", "type": "string", "null_count": 1, "unique_count": 2}
            ],
            "data": []
        }
        
        pk_suggestion = analyzer._suggest_primary_key(table)
        
        assert pk_suggestion is not None
        assert pk_suggestion['column'] == 'id'
        assert pk_suggestion['confidence'] in ['high', 'medium']
    
    def test_detect_relationships(self, analyzer):
        """Test relationship detection between tables."""
        all_tables = [
            {
                "source_file": "users.csv",
                "table": {
                    "name": "users",
                    "row_count": 3,
                    "columns": [
                        {"name": "id", "type": "integer", "null_count": 0, "unique_count": 3},
                        {"name": "name", "type": "string", "null_count": 0, "unique_count": 3}
                    ],
                    "data": []
                }
            },
            {
                "source_file": "orders.csv",
                "table": {
                    "name": "orders",
                    "row_count": 5,
                    "columns": [
                        {"name": "order_id", "type": "integer", "null_count": 0, "unique_count": 5},
                        {"name": "user_id", "type": "integer", "null_count": 0, "unique_count": 3}
                    ],
                    "data": []
                }
            }
        ]
        
        relationships = analyzer._detect_relationships(all_tables)
        
        # Should detect user_id -> id relationship
        assert len(relationships) > 0
        found_relationship = any(
            rel['from_column'] == 'user_id' or rel['to_column'] == 'user_id'
            for rel in relationships
        )
        assert found_relationship
    
    def test_detect_data_quality_issues(self, analyzer):
        """Test data quality issue detection."""
        table = {
            "name": "test",
            "row_count": 10,
            "columns": [
                {"name": "col1", "type": "string", "null_count": 8, "unique_count": 2},  # High nulls
                {"name": "col2", "type": "integer", "null_count": 0, "unique_count": 10}
            ],
            "data": []
        }
        
        issues = analyzer._detect_data_quality_issues(table)
        
        assert len(issues) > 0
        # Should detect high null percentage in col1
        assert any("null" in issue.lower() for issue in issues)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
