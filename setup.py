from setuptools import find_packages, setup

setup(
    name="billups-data-analysis",
    version="1.0.0",
    packages=find_packages(where=".", include=["src", "src.*"]),
    package_dir={"": "."},
    py_modules=["src"],
    install_requires=[
        "pyspark>=3.0.0",
        "pandas>=1.3.0",
        "numpy>=1.21.0",
        "click>=8.0.0",
        "pyarrow>=5.0.0",
        "matplotlib>=3.4.0",
        "seaborn>=0.11.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.2.0",
            "pytest-cov>=2.12.0",
            "ruff>=0.1.0",
            "mypy>=0.910",
        ]
    },
    entry_points={
        "console_scripts": [
            "billups-analysis=src.cli:main",
        ],
    },
    python_requires=">=3.10",
)
