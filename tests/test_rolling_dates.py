"""
Tests for the rolling dates module.
"""
import pytest
import json
import os
import tempfile
from src.rolling_dates import RollingDateConfig, create_rolling_date_config


class TestRollingDateConfig:
    """Test suite for RollingDateConfig class."""
    
    def test_empty_config(self):
        """Test creating a config without a file path."""
        config = RollingDateConfig()
        assert not config.has_mappings()
        assert config.get_all_mappings() == {}
        assert config.get_source_date("2025-09-30") is None
        assert not config.is_rolling_date("2025-09-30")
    
    def test_load_valid_config(self, tmp_path):
        """Test loading a valid configuration file."""
        config_data = {
            "2025-09-30": "2025-09-24",
            "2025-10-01": "2025-09-25"
        }
        config_file = tmp_path / "rolling_dates.json"
        config_file.write_text(json.dumps(config_data))
        
        config = RollingDateConfig(str(config_file))
        assert config.has_mappings()
        assert len(config.get_all_mappings()) == 2
        assert config.get_source_date("2025-09-30") == "2025-09-24"
        assert config.get_source_date("2025-10-01") == "2025-09-25"
        assert config.is_rolling_date("2025-09-30")
        assert config.is_rolling_date("2025-10-01")
        assert not config.is_rolling_date("2025-09-29")
    
    def test_get_mapping_info(self, tmp_path):
        """Test getting complete mapping information."""
        config_data = {
            "2025-09-30": "2025-09-24"
        }
        config_file = tmp_path / "rolling_dates.json"
        config_file.write_text(json.dumps(config_data))
        
        config = RollingDateConfig(str(config_file))
        mapping_info = config.get_mapping_info("2025-09-30")
        assert mapping_info == ("2025-09-24", "2025-09-30")
        assert config.get_mapping_info("2025-09-29") is None
    
    def test_file_not_found(self):
        """Test error handling when config file doesn't exist."""
        with pytest.raises(FileNotFoundError):
            RollingDateConfig("nonexistent_file.json")
    
    def test_invalid_json(self, tmp_path):
        """Test error handling for invalid JSON."""
        config_file = tmp_path / "invalid.json"
        config_file.write_text("{ invalid json }")
        
        with pytest.raises(json.JSONDecodeError):
            RollingDateConfig(str(config_file))
    
    def test_invalid_data_structure(self, tmp_path):
        """Test error handling for non-dictionary JSON."""
        config_file = tmp_path / "invalid_structure.json"
        config_file.write_text(json.dumps(["not", "a", "dict"]))
        
        with pytest.raises(ValueError, match="must be a JSON object"):
            RollingDateConfig(str(config_file))
    
    def test_invalid_date_format_target(self, tmp_path):
        """Test error handling for invalid target date format."""
        config_data = {
            "2025/09/30": "2025-09-24"  # Invalid format (slashes instead of dashes)
        }
        config_file = tmp_path / "invalid_date.json"
        config_file.write_text(json.dumps(config_data))
        
        with pytest.raises(ValueError, match="Invalid target date format"):
            RollingDateConfig(str(config_file))
    
    def test_invalid_date_format_source(self, tmp_path):
        """Test error handling for invalid source date format."""
        config_data = {
            "2025-09-30": "2025/09/24"  # Invalid format
        }
        config_file = tmp_path / "invalid_date.json"
        config_file.write_text(json.dumps(config_data))
        
        with pytest.raises(ValueError, match="Invalid source date format"):
            RollingDateConfig(str(config_file))
    
    def test_invalid_date_values(self, tmp_path):
        """Test error handling for invalid date values."""
        config_data = {
            "2025-13-45": "2025-09-24"  # Invalid month and day
        }
        config_file = tmp_path / "invalid_date.json"
        config_file.write_text(json.dumps(config_data))
        
        with pytest.raises(ValueError):
            RollingDateConfig(str(config_file))
    
    def test_get_all_mappings_returns_copy(self, tmp_path):
        """Test that get_all_mappings returns a copy, not the original dict."""
        config_data = {
            "2025-09-30": "2025-09-24"
        }
        config_file = tmp_path / "rolling_dates.json"
        config_file.write_text(json.dumps(config_data))
        
        config = RollingDateConfig(str(config_file))
        mappings = config.get_all_mappings()
        mappings["2025-10-01"] = "2025-09-25"  # Modify the returned dict
        
        # Original config should not be affected
        assert "2025-10-01" not in config.get_all_mappings()
        assert len(config.get_all_mappings()) == 1
    
    def test_factory_function(self, tmp_path):
        """Test the create_rolling_date_config factory function."""
        config_data = {
            "2025-09-30": "2025-09-24"
        }
        config_file = tmp_path / "rolling_dates.json"
        config_file.write_text(json.dumps(config_data))
        
        config = create_rolling_date_config(str(config_file))
        assert isinstance(config, RollingDateConfig)
        assert config.has_mappings()
        
        # Test with None
        empty_config = create_rolling_date_config(None)
        assert isinstance(empty_config, RollingDateConfig)
        assert not empty_config.has_mappings()
    
    def test_multiple_mappings(self, tmp_path):
        """Test configuration with multiple date mappings."""
        config_data = {
            "2025-09-30": "2025-09-24",
            "2025-10-01": "2025-09-25",
            "2025-10-02": "2025-09-26",
            "2025-10-03": "2025-09-27",
            "2025-10-04": "2025-09-28",
            "2025-10-05": "2025-09-29",
            "2025-10-06": "2025-09-23"
        }
        config_file = tmp_path / "rolling_dates.json"
        config_file.write_text(json.dumps(config_data))
        
        config = RollingDateConfig(str(config_file))
        assert len(config.get_all_mappings()) == 7
        
        # Verify all mappings
        for target, source in config_data.items():
            assert config.get_source_date(target) == source
            assert config.is_rolling_date(target)
    
    def test_edge_case_empty_file(self, tmp_path):
        """Test handling of empty JSON object."""
        config_file = tmp_path / "empty.json"
        config_file.write_text("{}")
        
        config = RollingDateConfig(str(config_file))
        assert not config.has_mappings()
        assert config.get_all_mappings() == {}
    
    def test_date_format_with_leading_zeros(self, tmp_path):
        """Test that dates with leading zeros are handled correctly."""
        config_data = {
            "2025-09-05": "2025-09-01"
        }
        config_file = tmp_path / "rolling_dates.json"
        config_file.write_text(json.dumps(config_data))
        
        config = RollingDateConfig(str(config_file))
        assert config.get_source_date("2025-09-05") == "2025-09-01"
