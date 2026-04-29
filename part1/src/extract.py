import json
from pathlib import Path
from typing import Any


def read_json(path: str | Path) -> dict[str, Any] | list[dict[str, Any]]:
    """
    Read JSON file from local filesystem.

    Args: 
        path: Path to JSON file.
        
    Returns:
        Parsed JSON payload.
    """
    file_path = Path(path)

    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found at {file_path}")
    
    with file_path.open("r", encoding="utf-8") as file:
        return json.load(file)
    

def extract_records(payload: dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Extract records from JSON mock payload structure.
    
    The expected structure is:
    {
        "data": [
            {...},
            {...}
        ]
    }

    Args:
        payload: Parsed JSON payload.

    Returns:
        List of source records.
    """
    if isinstance(payload, dict):
        records = payload.get("data", [])

        if records is None:
            return []
        
        if not isinstance(records, list):
            raise ValueError("Expected 'data' field to be a list")
        
        return records
    
    if isinstance(payload, list):
        return payload
    
    raise ValueError("Unexpected JSON structure. Expected dict or list")
