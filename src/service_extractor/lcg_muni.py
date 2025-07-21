from src.service_extractor.default import AbstractServiceExtractor


class LcgMunicipalServiceExtractor(AbstractServiceExtractor):
    @staticmethod
    def extract_service_name_from_identifier(service_identifier: str) -> str:
        """
        Extracts the actual service code from the service identifier for A Coruña GTFS.
        The service identifier is composed as follows:
        - [service_code][calendar_type][departure_time]
        - service_code: variable length (at least 2 digits, can be more)
        - calendar_type: always 2 digits
        - departure_time: always 4 digits

        Args:
            service_identifier (str): The service identifier from which to extract the code.

        Returns:
            str: The code of the service (service_code part).
        """
        if not service_identifier or len(service_identifier) < 7:
            raise ValueError("Invalid service identifier: must be at least 7 characters long")
        # Remove last 6 characters (2 for calendar type, 4 for departure time)
        return service_identifier[:-6]