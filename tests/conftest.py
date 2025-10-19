import pytest
import pandas as pd
import numpy as np


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing"""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'salary': [50000.0, 60000.0, 70000.0, 55000.0, 65000.0],
        'active': [True, False, True, True, False],
        'category': ['A', 'B', 'A', 'C', 'B']
    })


@pytest.fixture
def inconsistent_dataframe():
    """Create a DataFrame with various inconsistencies for testing"""
    return pd.DataFrame({
        'id': [1, 2, 'invalid', 4, 5],  # Non-numeric in numeric column
        'name': ['Alice', 123, 'Charlie', 'Diana', ''],  # Numeric and empty in text column
        'age': [25, 'thirty', 35, None, 32],  # Text and null in numeric column
        'salary': [50000.0, 60000.0, 'N/A', 55000.0, np.nan],  # Inconsistent values
        'active': [True, 'Yes', True, 'False', False],  # Mixed boolean representations
        'date': ['2023-01-01', 'invalid_date', '2023-03-15', '2023-04-20', '']  # Invalid dates
    })


@pytest.fixture
def schema_analyzer():
    """Create a SchemaAnalyzer instance for testing"""
    from dataproc_utility_tools.schema_analyzer import SchemaAnalyzer
    return SchemaAnalyzer(
        chunksize=1000,
        sample_size=5,
        inconsistency_sample_size=3,
        numeric_threshold=0.9,
        mixed_threshold=0.6,
        high_null_threshold=0.1
    )