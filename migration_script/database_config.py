from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class DatabaseConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    schema: str

def load_config_from_file(file_path: str) -> DatabaseConfig:
    """Load database configuration from ini file"""
    config_dict = {}
    
    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if not line or line.startswith(';') or line.startswith('#'):
                    continue
                
                # Parse key=value pairs
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip('"')  # Remove surrounding quotes if present
                    config_dict[key] = value
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {file_path}")
    except Exception as e:
        raise Exception(f"Error reading configuration file {file_path}: {e}")
    
    # Extract database configuration
    try:
        return DatabaseConfig(
            host=config_dict.get('DatabaseConfig_host'),
            port=int(config_dict.get('DatabaseConfig_port', 5432)),
            user=config_dict.get('DatabaseConfig_user'),
            password=config_dict.get('DatabaseConfig_password'),
            database=config_dict.get('DatabaseConfig_database'),
            schema=config_dict.get('DatabaseConfig_schema', 'public')
        )
    except (KeyError, TypeError, ValueError) as e:
        raise Exception(f"Invalid or missing database configuration in {file_path}: {e}")
