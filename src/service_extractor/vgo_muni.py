import re
from src.service_extractor.default import AbstractServiceExtractor

# Compile regex patterns once for better performance
_SERVICE_ID_PATTERN = re.compile(r".*_([0-9]{6})$")
_SERVICE_NAME_PATTERN = re.compile(r"^.+_([0-9]{6})$")

# Pre-define line name mapping for O(1) lookup
_LINE_NAME_MAP = {
    1: "C1",
    3: "C3", 
    30: "N1",
    33: "N4",
    8: "A",
    101: "H",
    150: "REF",
    500: "TUR"
}


class VgoMunicipalServiceExtractor(AbstractServiceExtractor):
    @staticmethod
    def extract_actual_service_id_from_identifier(service_identifier: str) -> str:
        # Extract the numeric part after the underscore, e.g. 'C1 01LPV00_001001' -> '001001'
        match = _SERVICE_ID_PATTERN.match(service_identifier)
        if match:
            return match.group(1)
        return service_identifier

    @staticmethod
    def extract_service_name_from_identifier(service_identifier: str) -> str:
        """
        Extracts the actual service code from the service identifier. In Vigo, the service
        identifier contains a variable-length indicator with letters and stuff, an underscore and 
        6 digits indicating the "main" line and the shift code.

        For example:
            "C1 01LPV00_001001" -> Line 001 (C1) shift 001
            "C1 02LP100_001002" -> Line 001 (C1) shift 002
        
        Args:
            service_identifier (str): The service identifier from which to extract the code.

        Returns:
            str: The code of the service.
        """
        matches = _SERVICE_NAME_PATTERN.match(service_identifier)
        if not matches:
            print(f"Invalid service identifier format: {service_identifier} - Returning as is")
            return service_identifier
        
        # Take the second part only (first capture group)
        service_code = matches.group(1)
        if len(service_code) < 6:  # Should be exactly 6 digits
            print(f"Service code too short: {service_code} - Returning as is")
            return service_identifier

        line_number = int(service_code[:3])  # First three digits are the line number
        shift_code = int(service_code[3:])   # Last three digits are the shift code

        line_number_name = VgoMunicipalServiceExtractor.get_actual_line_name(line_number)
        return f"{line_number_name}-{shift_code}º ({service_code})"
        
    @staticmethod
    def get_actual_line_name(line_number: int) -> str:
        # Use dictionary lookup for O(1) performance instead of match statement
        return _LINE_NAME_MAP.get(line_number, f"L{line_number}")