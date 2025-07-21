import pytest
from src.service_extractor.lcg_muni import LcgMunicipalServiceExtractor

@pytest.mark.parametrize(
    "service_identifier,expected",
    [
        ("100010731", "100"),
        ("1501081246", "1501"),
        ("2900070804", "2900"),
        ("2201101700", "2201"),
        ("2001031940", "2001"),
    ]
)
def test_extract_service_name_from_identifier(service_identifier, expected):
    assert LcgMunicipalServiceExtractor.extract_service_name_from_identifier(service_identifier) == expected

def test_invalid_identifier_too_short():
    with pytest.raises(ValueError):
        LcgMunicipalServiceExtractor.extract_service_name_from_identifier("12345")
