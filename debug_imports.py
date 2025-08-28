#!/usr/bin/env python3
"""Debug script to check Python imports in CI environment."""

import sys
import os
from pathlib import Path

print("Python version:", sys.version)
print("Python executable:", sys.executable)
print("\nPython path:")
for p in sys.path:
    print(f"  {p}")

print("\nCurrent directory:", os.getcwd())
print("\nDirectory contents:")
for item in sorted(os.listdir(".")):
    print(f"  {item}")

print("\nChecking src directory:")
src_path = Path("src")
if src_path.exists():
    print("  src/ exists")
    print("  src/ contents:")
    for item in sorted(src_path.iterdir()):
        print(f"    {item.name}")
else:
    print("  src/ does NOT exist!")

print("\nTrying to import src:")
try:
    import src
    print("  SUCCESS: src imported")
    print(f"  src location: {src.__file__}")
except ImportError as e:
    print(f"  FAILED: {e}")

print("\nTrying to import src.data:")
try:
    import src.data
    print("  SUCCESS: src.data imported")
except ImportError as e:
    print(f"  FAILED: {e}")

print("\nTrying to import src.data.loader:")
try:
    from src.data.loader import DataLoader
    print("  SUCCESS: DataLoader imported")
except ImportError as e:
    print(f"  FAILED: {e}")

print("\nChecking installed packages:")
import subprocess
result = subprocess.run([sys.executable, "-m", "pip", "list"], capture_output=True, text=True)
if "billups-data-analysis" in result.stdout:
    print("  billups-data-analysis is installed")
else:
    print("  billups-data-analysis is NOT installed")