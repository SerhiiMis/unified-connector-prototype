import abc

class Connector(abc.ABC):
    """Base class for all connectors."""

    @abc.abstractmethod
    def fetch(self):
        """Fetch raw data from the source. """
        pass

    @abc.abstractmethod
    def normalize(self, raw_data):
        """Map raw data into our unified schema."""
        pass

