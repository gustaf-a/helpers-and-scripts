import json
import os
import logging
from dataclasses import dataclass
from typing import Dict, Optional

@dataclass
class MigrationProgress:
    table_name: str
    total_rows: int
    migrated_rows: int
    last_primary_key: Optional[str] = None
    completed: bool = False

class ProgressTracker:
    def __init__(self, progress_file: str = "migration_progress.json", logger: logging.Logger = None):
        # Ensure progress directory exists
        self.progress_dir = "progress"
        os.makedirs(self.progress_dir, exist_ok=True)
        
        # Update progress file path to include directory
        if not os.path.dirname(progress_file):
            self.progress_file = os.path.join(self.progress_dir, progress_file)
        else:
            self.progress_file = progress_file
            
        self.logger = logger
        self.progress_data = self._load_progress()
    
    def _load_progress(self) -> Dict[str, MigrationProgress]:
        """Load migration progress from file"""
        if not os.path.exists(self.progress_file):
            return {}
        
        try:
            with open(self.progress_file, 'r') as f:
                data = json.load(f)
                return {
                    table: MigrationProgress(**progress) 
                    for table, progress in data.items()
                }
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Could not load progress file: {e}. Starting fresh.")
            return {}
    
    def save_progress(self):
        """Save migration progress to file"""
        try:
            data = {
                table: {
                    'table_name': progress.table_name,
                    'total_rows': progress.total_rows,
                    'migrated_rows': progress.migrated_rows,
                    'last_primary_key': progress.last_primary_key,
                    'completed': progress.completed
                }
                for table, progress in self.progress_data.items()
            }
            with open(self.progress_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to save progress: {e}")
    
    def get_progress(self, table_name: str) -> MigrationProgress:
        """Get progress for a specific table"""
        return self.progress_data.get(table_name)
    
    def update_progress(self, table_name: str, progress: MigrationProgress):
        """Update progress for a specific table"""
        self.progress_data[table_name] = progress
        self.save_progress()
