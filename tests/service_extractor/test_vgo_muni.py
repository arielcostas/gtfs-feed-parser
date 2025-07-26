import pytest
from src.service_extractor.vgo_muni import VgoMunicipalServiceExtractor

@pytest.mark.parametrize(
    "service_identifier,expected",
    [
        ("C1 01LPV00_001001", "L1 S1º")
    ]
)
def test_extract_service_name_from_identifier(service_identifier, expected):
    assert VgoMunicipalServiceExtractor.extract_service_name_from_identifier(service_identifier) == expected

def test_invalid_identifier_too_short():
    with pytest.raises(ValueError):
        VgoMunicipalServiceExtractor.extract_service_name_from_identifier("12345")
