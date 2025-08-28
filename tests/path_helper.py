"""Helper to ensure correct Python path for tests."""
import sys
import os
from pathlib import Path

def setup_test_path():
    """Add the project root to Python path."""
    # Get the absolute path to the tests directory
    tests_dir = Path(__file__).parent.absolute()
    
    # Get the project root (parent of tests)
    project_root = tests_dir.parent
    
    # Add to Python path if not already there
    project_root_str = str(project_root)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)
    
    # Also try to add src directly if it exists
    src_path = project_root / "src"
    if src_path.exists() and str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    
    # Set PYTHONPATH environment variable
    os.environ["PYTHONPATH"] = project_root_str
    
    return project_root

# Call this when the module is imported
setup_test_path()