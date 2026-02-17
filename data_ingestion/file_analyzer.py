"""
File Analyzer Module

Analyzes parsed file data to extract structural information,
detect relationships, and generate natural language summaries.
Implements DS-STAR's Data File Analyzer concept.
"""

from typing import Dict, List, Any, Tuple, Set
import pandas as pd
from collections import Counter

class FileAnalyzer:
    """Analyzes parsed file data and generates contextual summaries."""
    
    def __init__(self):
        """Initialize the file analyzer."""
        pass
    
    def analyze_files(self, parsed_files: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze multiple parsed files and generate comprehensive summary.
        
        Args:
            parsed_files: List of parsed file dictionaries from FileParser
            
        Returns:
            Dictionary with analysis results and natural language summary
        """
        analysis = {
            "total_files": len(parsed_files),
            "total_tables": 0,
            "total_rows": 0,
            "file_summaries": [],
            "relationships": [],
            "suggested_primary_keys": {},
            "data_quality_issues": [],
            "natural_language_summary": ""
        }
        
        all_tables = []
        
        # Analyze each file
        for parsed_file in parsed_files:
            file_summary = self._analyze_single_file(parsed_file)
            analysis["file_summaries"].append(file_summary)
            
            for table in parsed_file["tables"]:
                all_tables.append({
                    "source_file": parsed_file["filename"],
                    "table": table
                })
                analysis["total_rows"] += table["row_count"]
            
            analysis["total_tables"] += len(parsed_file["tables"])
        
        # Detect relationships between tables
        if len(all_tables) > 1:
            analysis["relationships"] = self._detect_relationships(all_tables)
        
        # Suggest primary keys
        for table_info in all_tables:
            table = table_info["table"]
            pk_suggestion = self._suggest_primary_key(table)
            if pk_suggestion:
                key = f"{table_info['source_file']}.{table['name']}"
                analysis["suggested_primary_keys"][key] = pk_suggestion
        
        # Detect data quality issues
        for table_info in all_tables:
            issues = self._detect_data_quality_issues(table_info["table"])
            for issue in issues:
                analysis["data_quality_issues"].append({
                    "file": table_info["source_file"],
                    "table": table_info["table"]["name"],
                    "issue": issue
                })
        
        # Generate natural language summary
        analysis["natural_language_summary"] = self._generate_nl_summary(analysis, all_tables)
        
        return analysis
    
    def _analyze_single_file(self, parsed_file: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze a single parsed file."""
        summary = {
            "filename": parsed_file["filename"],
            "format": parsed_file["format"],
            "table_count": len(parsed_file["tables"]),
            "tables": []
        }
        
        for table in parsed_file["tables"]:
            table_summary = {
                "name": table["name"],
                "row_count": table["row_count"],
                "column_count": len(table["columns"]),
                "columns": [
                    {
                        "name": col["name"],
                        "type": col["type"],
                        "null_percentage": (col["null_count"] / table["row_count"] * 100) if table["row_count"] > 0 else 0,
                        "unique_percentage": (col["unique_count"] / table["row_count"] * 100) if table["row_count"] > 0 else 0
                    }
                    for col in table["columns"]
                ]
            }
            summary["tables"].append(table_summary)
        
        return summary
    
    def _suggest_primary_key(self, table: Dict[str, Any]) -> Dict[str, Any]:
        """Suggest potential primary key column(s) for a table."""
        if table["row_count"] == 0:
            return None
        
        candidates = []
        
        for col in table["columns"]:
            # Check if column is unique and has no nulls
            is_unique = col["unique_count"] == table["row_count"]
            has_no_nulls = col["null_count"] == 0
            
            if is_unique and has_no_nulls:
                score = 100
                
                # Bonus for common PK naming patterns
                col_name_lower = col["name"].lower()
                if 'id' in col_name_lower:
                    score += 50
                if col_name_lower.endswith('_id') or col_name_lower == 'id':
                    score += 25
                
                # Bonus for integer types (common for auto-increment)
                if col["type"] == "integer":
                    score += 25
                
                candidates.append({
                    "column": col["name"],
                    "score": score,
                    "reason": f"Unique values, no nulls, type: {col['type']}"
                })
        
        if candidates:
            # Return highest scoring candidate
            best = max(candidates, key=lambda x: x["score"])
            return {
                "column": best["column"],
                "confidence": "high" if best["score"] >= 150 else "medium",
                "reason": best["reason"]
            }
        
        return None
    
    def _detect_relationships(self, all_tables: List[Dict]) -> List[Dict[str, Any]]:
        """Detect potential foreign key relationships between tables."""
        relationships = []
        
        for i, table1_info in enumerate(all_tables):
            for table2_info in all_tables[i+1:]:
                table1 = table1_info["table"]
                table2 = table2_info["table"]
                
                # Compare column names and types
                for col1 in table1["columns"]:
                    for col2 in table2["columns"]:
                        # Check if columns have similar names
                        similarity_score = self._calculate_column_similarity(col1, col2)
                        
                        if similarity_score > 0.5:  # Threshold for potential relationship
                            relationships.append({
                                "from_file": table1_info["source_file"],
                                "from_table": table1["name"],
                                "from_column": col1["name"],
                                "to_file": table2_info["source_file"],
                                "to_table": table2["name"],
                                "to_column": col2["name"],
                                "confidence": "high" if similarity_score > 0.8 else "medium",
                                "reason": self._get_relationship_reason(col1, col2, similarity_score)
                            })
        
        return relationships
    
    def _calculate_column_similarity(self, col1: Dict, col2: Dict) -> float:
        """Calculate similarity score between two columns (0-1)."""
        score = 0.0
        
        # Same name
        if col1["name"] == col2["name"]:
            score += 0.5
        
        # Similar name (e.g., "id" vs "patient_id")
        name1_lower = col1["name"].lower()
        name2_lower = col2["name"].lower()
        
        if name1_lower in name2_lower or name2_lower in name1_lower:
            score += 0.3
        
        # Same type
        if col1["type"] == col2["type"]:
            score += 0.2
        
        # Check if one looks like a foreign key
        if ('_id' in name1_lower and name2_lower == 'id') or \
           ('_id' in name2_lower and name1_lower == 'id'):
            score += 0.4
        
        return min(score, 1.0)
    
    def _get_relationship_reason(self, col1: Dict, col2: Dict, similarity_score: float) -> str:
        """Generate human-readable reason for detected relationship."""
        reasons = []
        
        if col1["name"] == col2["name"]:
            reasons.append("identical column names")
        elif col1["name"].lower() in col2["name"].lower() or col2["name"].lower() in col1["name"].lower():
            reasons.append("similar column names")
        
        if col1["type"] == col2["type"]:
            reasons.append(f"matching types ({col1['type']})")
        
        if '_id' in col1["name"].lower() or '_id' in col2["name"].lower():
            reasons.append("foreign key naming pattern")
        
        return ", ".join(reasons) if reasons else "column similarity detected"
    
    def _detect_data_quality_issues(self, table: Dict[str, Any]) -> List[str]:
        """Detect data quality issues in a table."""
        issues = []
        
        # Check for high null percentage
        for col in table["columns"]:
            null_pct = (col["null_count"] / table["row_count"] * 100) if table["row_count"] > 0 else 0
            if null_pct > 50:
                issues.append(f"Column '{col['name']}' has {null_pct:.1f}% null values")
        
        # Check for potential duplicate rows (all columns have low unique count)
        if table["row_count"] > 0:
            avg_unique_pct = sum(
                (col["unique_count"] / table["row_count"] * 100) for col in table["columns"]
            ) / len(table["columns"])
            
            if avg_unique_pct < 10 and table["row_count"] > 10:
                issues.append("Potential duplicate rows detected (low unique value percentage)")
        
        # Check for no potential primary key
        has_unique_col = any(
            col["unique_count"] == table["row_count"] and col["null_count"] == 0
            for col in table["columns"]
        )
        if not has_unique_col and table["row_count"] > 0:
            issues.append("No obvious primary key candidate found")
        
        return issues
    
    def _generate_nl_summary(self, analysis: Dict, all_tables: List[Dict]) -> str:
        """Generate natural language summary of the analysis."""
        summary_parts = []
        
        # Overall summary
        summary_parts.append(
            f"Dataset contains {analysis['total_files']} file(s) with {analysis['total_tables']} table(s) "
            f"and a total of {analysis['total_rows']:,} rows."
        )
        
        # File summaries
        summary_parts.append("\n\nFile Details:")
        for file_summary in analysis["file_summaries"]:
            for table_summary in file_summary["tables"]:
                summary_parts.append(
                    f"\n- {file_summary['filename']} ({file_summary['format']}) → Table '{table_summary['name']}': "
                    f"{table_summary['row_count']:,} rows, {table_summary['column_count']} columns"
                )
                
                # List columns with types
                col_list = ", ".join([
                    f"{col['name']} ({col['type']})" for col in table_summary["columns"][:5]
                ])
                if table_summary['column_count'] > 5:
                    col_list += f", ... and {table_summary['column_count'] - 5} more"
                summary_parts.append(f"  Columns: {col_list}")
        
        # Primary key suggestions
        if analysis["suggested_primary_keys"]:
            summary_parts.append("\n\nSuggested Primary Keys:")
            for table_key, pk_info in analysis["suggested_primary_keys"].items():
                summary_parts.append(
                    f"- {table_key}: '{pk_info['column']}' (confidence: {pk_info['confidence']})"
                )
        
        # Relationships
        if analysis["relationships"]:
            summary_parts.append(f"\n\nDetected {len(analysis['relationships'])} potential relationship(s):")
            for rel in analysis["relationships"][:5]:  # Show first 5
                summary_parts.append(
                    f"- {rel['from_table']}.{rel['from_column']} → {rel['to_table']}.{rel['to_column']} "
                    f"({rel['confidence']} confidence: {rel['reason']})"
                )
        
        # Data quality issues
        if analysis["data_quality_issues"]:
            summary_parts.append(f"\n\nData Quality Warnings ({len(analysis['data_quality_issues'])} issue(s)):")
            for issue_info in analysis["data_quality_issues"][:5]:  # Show first 5
                summary_parts.append(f"- {issue_info['table']}: {issue_info['issue']}")
        
        return "".join(summary_parts)
