import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from dataproc_utility_tools.utils import schema_from_analysis_json, _map_inferred_type_to_dtype


class TestUtils:
    """Test class for utility functions"""
    
    def test_map_inferred_type_to_dtype_numeric(self):
        """Test mapping of 'numeric' inferred type to pandas dtype"""
        result = _map_inferred_type_to_dtype("numeric")
        assert result == "float64"
        
        # Test case variations
        result = _map_inferred_type_to_dtype("NUMERIC")
        assert result == "float64"
        
        result = _map_inferred_type_to_dtype("Numeric")
        assert result == "float64"
    
    def test_map_inferred_type_to_dtype_boolean(self):
        """Test mapping of 'boolean' inferred type to pandas dtype"""
        result = _map_inferred_type_to_dtype("boolean")
        assert result == "boolean"
        
        # Test case variations
        result = _map_inferred_type_to_dtype("BOOLEAN")
        assert result == "boolean"
        
        result = _map_inferred_type_to_dtype("Boolean")
        assert result == "boolean"
    
    def test_map_inferred_type_to_dtype_categorical(self):
        """Test mapping of 'categorical' inferred type to pandas dtype"""
        result = _map_inferred_type_to_dtype("categorical")
        assert result == "string"
        
        # Test other types that should map to string
        other_types = ["mixed", "unknown", "text", "categ", ""]
        for dtype in other_types:
            result = _map_inferred_type_to_dtype(dtype)
            assert result == "string", f"Type '{dtype}' should map to 'string'"
    
    def test_map_inferred_type_to_dtype_none_handling(self):
        """Test handling of None and empty values"""
        result = _map_inferred_type_to_dtype(None)
        assert result == "string"
        
        result = _map_inferred_type_to_dtype("")
        assert result == "string"
    
    def test_schema_from_analysis_json_file_not_found(self):
        """Test schema_from_analysis_json with non-existent file"""
        with pytest.raises(FileNotFoundError):
            schema_from_analysis_json("non_existent_file.json")
    
    def test_schema_from_analysis_json_invalid_json(self):
        """Test schema_from_analysis_json with invalid JSON"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
            f.write("this is not valid json")
            temp_file = f.name
        
        try:
            with pytest.raises(ValueError):
                schema_from_analysis_json(temp_file)
        finally:
            Path(temp_file).unlink()
    
    def test_schema_from_analysis_json_valid_file(self):
        """Test schema_from_analysis_json with valid JSON containing schema data"""
        schema_data = {
            "schema": {
                "id": {"inferred_type": "numeric"},
                "name": {"inferred_type": "categorical"},
                "age": {"inferred_type": "numeric"},
                "active": {"inferred_type": "boolean"},
                "score": {"inferred_type": "mixed"}
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
            json.dump(schema_data, f)
            temp_file = f.name
        
        try:
            result = schema_from_analysis_json(temp_file)
            
            # Verify the result structure
            assert isinstance(result, dict)
            assert set(result.keys()) == {"id", "name", "age", "active", "score"}
            
            # Verify the type mappings
            assert result["id"] == "float64"      # numeric -> float64
            assert result["name"] == "string"     # categorical -> string
            assert result["age"] == "float64"     # numeric -> float64
            assert result["active"] == "boolean"  # boolean -> boolean
            assert result["score"] == "string"    # mixed -> string
            
        finally:
            Path(temp_file).unlink()
    
    def test_schema_from_analysis_json_no_schema_key(self):
        """Test schema_from_analysis_json with JSON that doesn't have 'schema' key"""
        # Test with direct column mappings (not nested in 'schema')
        direct_schema = {
            "id": {"inferred_type": "numeric"},
            "name": {"inferred_type": "categorical"}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
            json.dump(direct_schema, f)
            temp_file = f.name
        
        try:
            result = schema_from_analysis_json(temp_file)
            
            assert set(result.keys()) == {"id", "name"}
            assert result["id"] == "float64"
            assert result["name"] == "string"
            
        finally:
            Path(temp_file).unlink()
    
    def test_schema_from_analysis_json_empty_schema(self):
        """Test schema_from_analysis_json with empty schema"""
        empty_schema = {"schema": {}}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
            json.dump(empty_schema, f)
            temp_file = f.name
        
        try:
            with pytest.raises(ValueError, match="Estrutura do JSON de análise não contém informações de schema"):
                schema_from_analysis_json(temp_file)
                
        finally:
            Path(temp_file).unlink()
    
    def test_schema_from_analysis_json_no_inferred_type(self):
        """Test schema_from_analysis_json with columns that have no inferred_type"""
        schema_data = {
            "schema": {
                "id": {"inferred_type": "numeric"},
                "name": {"other_key": "some_value"},  # No inferred_type
                "age": {}  # Empty object
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
            json.dump(schema_data, f)
            temp_file = f.name
        
        try:
            result = schema_from_analysis_json(temp_file)
            
            # Columns without inferred_type should default to 'string'
            assert result["id"] == "float64"      # Has inferred_type = numeric
            assert result["name"] == "string"     # No inferred_type, defaults to string
            assert result["age"] == "string"      # No inferred_type, defaults to string
            
        finally:
            Path(temp_file).unlink()
    
    def test_schema_from_analysis_json_complex_schema(self):
        """Test schema_from_analysis_json with a complex schema structure similar to actual output"""
        complex_schema = {
            "schema": {
                "id": {
                    "inferred_type": "numeric",
                    "samples": [1, 2, 3],
                    "null_count": 0,
                    "inconsistencies": []
                },
                "nome": {
                    "inferred_type": "categorical", 
                    "samples": ["Alice", "Bob"],
                    "null_count": 1,
                    "inconsistencies": []
                },
                "ativo": {
                    "inferred_type": "boolean",
                    "samples": [True, False],
                    "null_count": 0,
                    "inconsistencies": []
                },
                "score": {
                    "inferred_type": "mixed",
                    "samples": [85.5, "N/A"],
                    "null_count": 2,
                    "inconsistencies": [
                        {
                            "type": "non_numeric_in_numeric_column",
                            "message": "Encontrados 1 valores não numéricos",
                            "count": 1,
                            "samples": ["N/A"]
                        }
                    ]
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
            json.dump(complex_schema, f)
            temp_file = f.name
        
        try:
            result = schema_from_analysis_json(temp_file)
            
            # Verify type mappings
            assert result["id"] == "float64"      # numeric -> float64
            assert result["nome"] == "string"     # categorical -> string
            assert result["ativo"] == "boolean"   # boolean -> boolean
            assert result["score"] == "string"    # mixed -> string
            
        finally:
            Path(temp_file).unlink()
    
    def test_schema_from_analysis_json_with_special_values(self):
        """Test schema_from_analysis_json with various edge cases in inferred types"""
        schema_data = {
            "schema": {
                "col1": {"inferred_type": "NUMERIC"},  # Uppercase
                "col2": {"inferred_type": "categorical"},  # Mixed case
                "col3": {"inferred_type": "BOOLEAN"},  # Uppercase boolean
                "col4": {"inferred_type": "MIXED"},  # Uppercase mixed
                "col5": {"inferred_type": "unknown"},  # Unknown type
                "col6": {"inferred_type": ""},  # Empty string
                "col7": {"inferred_type": None}  # None value
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
            json.dump(schema_data, f)
            temp_file = f.name
        
        try:
            result = schema_from_analysis_json(temp_file)
            
            # Verify case-insensitive handling and defaults
            assert result["col1"] == "float64"  # 'NUMERIC' -> 'numeric' -> float64
            assert result["col2"] == "string"   # 'categorical' -> string
            assert result["col3"] == "boolean"  # 'BOOLEAN' -> 'boolean' -> boolean
            assert result["col4"] == "string"   # 'MIXED' -> 'mixed' -> string
            assert result["col5"] == "string"   # 'unknown' -> string
            assert result["col6"] == "string"   # '' -> string
            assert result["col7"] == "string"   # None -> string
            
        finally:
            Path(temp_file).unlink()