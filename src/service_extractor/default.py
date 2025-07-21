from abc import abstractmethod


class AbstractServiceExtractor:
    """
    Abstract base class for service extractors.
    """
    @abstractmethod
    def extract_service_name_from_identifier(self, service_identifier: str) -> str:
        pass

class DefaultServiceExtractor(AbstractServiceExtractor):
    @staticmethod
    def extract_service_name_from_identifier(service_identifier: str) -> str:
        """
        Extracts the actual service code from the service identifier. The default
        implementation simply returns the service identifier as is.
        
        Args:
            service_identifier (str): The service identifier from which to extract the code.

        Returns:
            str: The code of the service.
        """
        return service_identifier