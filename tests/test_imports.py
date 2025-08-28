"""Test to verify imports work correctly."""
import sys
import os
from pathlib import Path

def test_imports():
    """Test that we can import all necessary modules."""
    # Ensure parent directory is in path
    parent_dir = Path(__file__).parent.parent
    if str(parent_dir) not in sys.path:
        sys.path.insert(0, str(parent_dir))
    
    # Try imports
    try:
        from src.data.loader import DataLoader
        assert DataLoader is not None
        print("✓ Successfully imported DataLoader")
    except ImportError as e:
        print(f"✗ Failed to import DataLoader: {e}")
        print(f"  Current directory: {os.getcwd()}")
        print(f"  Python path: {sys.path}")
        print(f"  Directory contents: {os.listdir('.')}")
        raise
    
    try:
        from src.analysis.tasks import AnalysisTasks
        assert AnalysisTasks is not None
        print("✓ Successfully imported AnalysisTasks")
    except ImportError as e:
        print(f"✗ Failed to import AnalysisTasks: {e}")
        raise

if __name__ == "__main__":
    test_imports()