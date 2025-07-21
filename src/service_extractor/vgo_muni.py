import re
from src.service_extractor.default import AbstractServiceExtractor


class VgoMunicipalServiceExtractor(AbstractServiceExtractor):
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
        matches = re.match(r"^[A-Z0-9]+_([0-9]{6})$", service_identifier)
        if not matches:
            print(f"Invalid service identifier format: {service_identifier} - Returning as is")
            return service_identifier
        
        # Take the second part only (first capture group)
        service_code = matches.group(1)
        if len(service_code) < 2:
            print(f"Service code too short: {service_code} - Returning as is")
            return service_identifier

        line_number = int(service_code[:3]) # First three digits are the line number, let's parse it to integer to remove leading zeros
        shift_code = int(service_code[3:])
        return f"L{line_number} S{shift_code}º"
        