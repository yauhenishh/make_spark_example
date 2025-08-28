"""Test to verify imports work correctly."""

import os


def test_imports():
    """Test that we can import all necessary modules."""
    # Try imports
    try:
        from src.data.loader import DataLoader

        assert DataLoader is not None
        print("✓ Successfully imported DataLoader")
    except ImportError as e:
        print(f"✗ Failed to import DataLoader: {e}")
        print(f"  Current directory: {os.getcwd()}")
        print(f"  Directory contents: {os.listdir('.')}")
        if 'src' in os.listdir('.'):
            print(f"  src/ contents: {os.listdir('src')}")
        raise

    try:
        from src.analysis.tasks import MerchantAnalysis

        assert MerchantAnalysis is not None
        print("✓ Successfully imported MerchantAnalysis")
    except ImportError as e:
        print(f"✗ Failed to import MerchantAnalysis: {e}")
        raise


if __name__ == "__main__":
    test_imports()
