"""
Ingestion Orchestrator

Coordinates the multi-step data ingestion process using DS-STAR's
iterative refinement approach.
"""

import os
import uuid
import json
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime

from data_ingestion.file_parser import FileParser
from data_ingestion.file_analyzer import FileAnalyzer
from agents.schema_agent import SchemaAgent
from database_builder import DatabaseBuilder

class IngestionState:
    """Represents the state of an ingestion process."""
    
    STAGES = [
        "uploaded",
        "parsing",
        "analyzing",
        "generating_schema",
        "awaiting_approval",
        "creating_database",
        "completed",
        "failed"
    ]
    
    def __init__(self, upload_id: str):
        self.upload_id = upload_id
        self.stage = "uploaded"
        self.created_at = datetime.now().isoformat()
        self.updated_at = datetime.now().isoformat()
        self.files = []
        self.requirements = ""
        self.parsed_data = None
        self.file_analysis = None
        self.schema_result = None
        self.schema_progress = None
        self.database_result = None
        self.errors = []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert state to dictionary."""
        return {
            "upload_id": self.upload_id,
            "stage": self.stage,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "files": self.files,
            "requirements": self.requirements,
            "has_parsed_data": self.parsed_data is not None,
            "has_analysis": self.file_analysis is not None,
            "has_schema": self.schema_result is not None,
            "has_database": self.database_result is not None,
            "schema_progress": self.schema_progress,
            "errors": self.errors
        }


class IngestionOrchestrator:
    """
    Orchestrates the end-to-end data ingestion workflow.
    Implements DS-STAR's iterative refinement loop.
    """
    
    def __init__(self, upload_dir: str = "./uploads", state_dir: str = "./instance/ingestion_states"):
        """
        Initialize the orchestrator.
        
        Args:
            upload_dir: Directory for uploaded files
            state_dir: Directory to store ingestion states
        """
        self.upload_dir = Path(upload_dir)
        self.state_dir = Path(state_dir)
        
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        
        self.file_parser = FileParser()
        self.file_analyzer = FileAnalyzer()
        self.schema_agent = SchemaAgent()
        self.db_builder = DatabaseBuilder()
        
        self.states: Dict[str, IngestionState] = {}
    
    def start_ingestion(
        self,
        files: List[Dict[str, Any]],
        requirements: str
    ) -> str:
        """
        Initiate a new ingestion workflow.
        
        Args:
            files: List of uploaded files with 'name' and 'path' keys
            requirements: User's natural language requirements
            
        Returns:
            Upload ID (UUID)
        """
        upload_id = str(uuid.uuid4())
        state = IngestionState(upload_id)
        state.files = files
        state.requirements = requirements
        
        # Create upload directory
        upload_path = self.upload_dir / upload_id
        upload_path.mkdir(parents=True, exist_ok=True)
        
        self.states[upload_id] = state
        self._save_state(state)
        
        return upload_id
    
    def process_files(self, upload_id: str) -> Dict[str, Any]:
        """
        Process uploaded files: parse and analyze.
        
        Args:
            upload_id: The ingestion ID
            
        Returns:
            Processing result with status and analysis
        """
        state = self._get_state(upload_id)
        
        if not state:
            return {"error": "Invalid upload ID"}
        
        try:
            # Stage 1: Parse files
            state.stage = "parsing"
            self._save_state(state)
            
            parsed_files = []
            for file_info in state.files:
                file_path = file_info["path"]
                print(f"Parsing {file_path}...")
                
                try:
                    parsed = self.file_parser.parse_file(file_path)
                    parsed_files.append(parsed)
                except Exception as e:
                    error_msg = f"Error parsing {file_info['name']}: {str(e)}"
                    state.errors.append(error_msg)
                    print(f"✗ {error_msg}")
            
            if not parsed_files:
                state.stage = "failed"
                state.errors.append("No files successfully parsed")
                self._save_state(state)
                return {"error": "No files successfully parsed", "details": state.errors}
            
            state.parsed_data = parsed_files
            
            # Stage 2: Analyze files
            state.stage = "analyzing"
            self._save_state(state)
            
            print("Analyzing files...")
            analysis = self.file_analyzer.analyze_files(parsed_files)
            state.file_analysis = analysis
            
            state.stage = "analyzing"
            state.updated_at = datetime.now().isoformat()
            self._save_state(state)
            
            return {
                "success": True,
                "upload_id": upload_id,
                "files_parsed": len(parsed_files),
                "analysis": analysis
            }
        
        except Exception as e:
            state.stage = "failed"
            error_msg = f"Processing failed: {str(e)}"
            state.errors.append(error_msg)
            self._save_state(state)
            return {"error": error_msg}
    
    def generate_schema(self, upload_id: str) -> Dict[str, Any]:
        """
        Generate database schema using iterative refinement.
        
        Args:
            upload_id: The ingestion ID
            
        Returns:
            Schema generation result
        """
        state = self._get_state(upload_id)
        
        if not state:
            return {"error": "Invalid upload ID"}
        
        if not state.file_analysis:
            return {"error": "Files must be analyzed first"}
        
        try:
            state.stage = "generating_schema"
            self._save_state(state)
            
            print("Generating database schema...")
            
            def progress_callback(info: Dict[str, Any]):
                state.schema_progress = info
                self._save_state(state)
                
            schema_result = self.schema_agent.generate_schema(
                file_analysis=state.file_analysis,
                requirements=state.requirements,
                progress_callback=progress_callback
            )
            
            state.schema_result = schema_result
            state.stage = "awaiting_approval"
            state.updated_at = datetime.now().isoformat()
            self._save_state(state)
            
            return {
                "success": True,
                "upload_id": upload_id,
                "schema_description": schema_result["schema_description"],
                "verification_status": schema_result["verification_status"],
                "warnings": schema_result["warnings"],
                "rounds_taken": schema_result["rounds_taken"]
            }
        
        except Exception as e:
            state.stage = "failed"
            error_msg = f"Schema generation failed: {str(e)}"
            state.errors.append(error_msg)
            self._save_state(state)
            return {"error": error_msg}
    
    def finalize_ingestion(
        self,
        upload_id: str,
        db_name: Optional[str] = None,
        approved: bool = True
    ) -> Dict[str, Any]:
        """
        Create database and populate with data.
        
        Args:
            upload_id: The ingestion ID
            db_name: Optional custom database name
            approved: Whether the schema is approved
            
        Returns:
            Finalization result with database info
        """
        state = self._get_state(upload_id)
        
        if not state:
            return {"error": "Invalid upload ID"}
        
        if not approved:
            state.stage = "awaiting_approval"
            self._save_state(state)
            return {"message": "Schema not approved, awaiting edits"}
        
        if not state.schema_result:
            return {"error": "Schema must be generated first"}
        
        try:
            state.stage = "creating_database"
            self._save_state(state)
            
            # Use upload_id as db_name if not provided
            if not db_name:
                db_name = f"ingestion_{upload_id[:8]}"
            
            print(f"Creating database: {db_name}")
            db_result = self.db_builder.create_database_from_schema(
                schema_code=state.schema_result["schema_code"],
                db_name=db_name,
                parsed_data=state.parsed_data
            )
            
            state.database_result = db_result
            
            if db_result["success"]:
                state.stage = "completed"
            else:
                state.stage = "failed"
                state.errors.extend(db_result["errors"])
            
            state.updated_at = datetime.now().isoformat()
            self._save_state(state)
            
            return {
                "success": db_result["success"],
                "upload_id": upload_id,
                "database_name": db_name,
                "database_path": db_result["db_path"],
                "tables_created": db_result["tables_created"],
                "rows_inserted": db_result["rows_inserted"],
                "errors": db_result["errors"],
                "warnings": db_result["warnings"]
            }
        
        except Exception as e:
            state.stage = "failed"
            error_msg = f"Database creation failed: {str(e)}"
            state.errors.append(error_msg)
            self._save_state(state)
            return {"error": error_msg}
    
    def get_status(self, upload_id: str) -> Dict[str, Any]:
        """
        Get current status of an ingestion process.
        
        Args:
            upload_id: The ingestion ID
            
        Returns:
            Status information
        """
        state = self._get_state(upload_id)
        
        if not state:
            return {"error": "Invalid upload ID"}
        
        return state.to_dict()
    
    def get_schema(self, upload_id: str) -> Dict[str, Any]:
        """
        Get generated schema for review.
        
        Args:
            upload_id: The ingestion ID
            
        Returns:
            Schema information
        """
        state = self._get_state(upload_id)
        
        if not state:
            return {"error": "Invalid upload ID"}
        
        if not state.schema_result:
            return {"error": "Schema not yet generated"}
        
        return {
            "upload_id": upload_id,
            "schema_code": state.schema_result["schema_code"],
            "schema_description": state.schema_result["schema_description"],
            "verification_status": state.schema_result["verification_status"],
            "warnings": state.schema_result["warnings"],
            "relationships": state.schema_result["relationships_detected"],
            "refinement_history": state.schema_result["refinement_history"],
            "rounds_taken": state.schema_result.get("rounds_taken", len(state.schema_result.get("refinement_history", [])))
        }
    
    def _get_state(self, upload_id: str) -> Optional[IngestionState]:
        """Retrieve state from memory or disk."""
        if upload_id in self.states:
            return self.states[upload_id]
        
        # Try loading from disk
        state_file = self.state_dir / f"{upload_id}.json"
        if state_file.exists():
            with open(state_file, 'r') as f:
                data = json.load(f)
                state = IngestionState(upload_id)
                state.__dict__.update(data)
                self.states[upload_id] = state
                return state
        
        return None
    
    def _save_state(self, state: IngestionState):
        """Save state to disk."""
        state.updated_at = datetime.now().isoformat()
        state_file = self.state_dir / f"{state.upload_id}.json"
        
        with open(state_file, 'w') as f:
            # Convert state to JSON-serializable format
            state_dict = state.__dict__.copy()
            json.dump(state_dict, f, indent=2)
