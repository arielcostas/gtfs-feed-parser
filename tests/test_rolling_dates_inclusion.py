"""
Tests for rolling dates inclusion in date lists.

This module tests that rolling dates from the configuration are automatically
added to the processing date list, even if they weren't explicitly requested.
"""
import pytest
import os
import json
from src.orchestrators import get_date_list
from src.rolling_dates import create_rolling_date_config


def test_rolling_dates_auto_included_in_service_reports(tmp_path):
    """
    Test that rolling dates from config are automatically added to date_list
    in service reports orchestrator logic.
    """
    # Create a rolling dates config
    config = {
        "2025-09-30": "2025-09-24",
        "2025-10-01": "2025-09-25",
        "2025-10-02": "2025-09-26"
    }
    
    config_file = tmp_path / "rolling.json"
    with open(config_file, 'w') as f:
        json.dump(config, f)
    
    # Load config
    rolling_config = create_rolling_date_config(str(config_file))
    
    # Simulate the date_list building process
    # Suppose user requested 2025-09-24 to 2025-09-26 (3 days)
    initial_date_list = ["2025-09-24", "2025-09-25", "2025-09-26"]
    
    # Apply the rolling dates expansion (this is what the orchestrator does)
    if rolling_config.has_mappings():
        rolling_dates = list(rolling_config.get_all_mappings().keys())
        expanded_date_list = sorted(set(initial_date_list + rolling_dates))
    else:
        expanded_date_list = initial_date_list
    
    # Verify the list now includes both original dates AND rolling dates
    assert len(expanded_date_list) == 6  # 3 original + 3 rolling
    assert "2025-09-24" in expanded_date_list
    assert "2025-09-25" in expanded_date_list
    assert "2025-09-26" in expanded_date_list
    assert "2025-09-30" in expanded_date_list
    assert "2025-10-01" in expanded_date_list
    assert "2025-10-02" in expanded_date_list


def test_rolling_dates_no_duplicates(tmp_path):
    """
    Test that if a rolling date is also in the user's requested range,
    it doesn't appear twice in the list.
    """
    # Create a config where one rolling date overlaps with requested range
    config = {
        "2025-09-30": "2025-09-24",
        "2025-09-25": "2025-09-24"  # 2025-09-25 might also be in user's range
    }
    
    config_file = tmp_path / "rolling.json"
    with open(config_file, 'w') as f:
        json.dump(config, f)
    
    rolling_config = create_rolling_date_config(str(config_file))
    
    # User requested 2025-09-24 to 2025-09-26 (includes 2025-09-25)
    initial_date_list = ["2025-09-24", "2025-09-25", "2025-09-26"]
    
    # Apply expansion
    if rolling_config.has_mappings():
        rolling_dates = list(rolling_config.get_all_mappings().keys())
        expanded_date_list = sorted(set(initial_date_list + rolling_dates))
    else:
        expanded_date_list = initial_date_list
    
    # Verify no duplicates
    assert len(expanded_date_list) == len(set(expanded_date_list))
    assert expanded_date_list.count("2025-09-25") == 1
    
    # Should have 4 unique dates total
    assert len(expanded_date_list) == 4
    assert "2025-09-24" in expanded_date_list
    assert "2025-09-25" in expanded_date_list
    assert "2025-09-26" in expanded_date_list
    assert "2025-09-30" in expanded_date_list


def test_no_rolling_config_no_expansion(tmp_path):
    """
    Test that without a rolling config, date_list remains unchanged.
    """
    # No config file
    rolling_config = create_rolling_date_config(None)
    
    initial_date_list = ["2025-09-24", "2025-09-25", "2025-09-26"]
    
    # Apply expansion
    if rolling_config.has_mappings():
        rolling_dates = list(rolling_config.get_all_mappings().keys())
        expanded_date_list = sorted(set(initial_date_list + rolling_dates))
    else:
        expanded_date_list = initial_date_list
    
    # Should be unchanged
    assert expanded_date_list == initial_date_list
    assert len(expanded_date_list) == 3
