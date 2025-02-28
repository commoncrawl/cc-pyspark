
class JsonExtractor:
    """Provides the best available JSON module"""

    def __init__(self):
        try:
            import simdjson
            self.json = simdjson.Parser()
            self.parse = self.json.parse
        except ImportError:
            import json
            self.json = json
            self.parse = self.json.loads
    def parse(self, s, *args, **kwargs):
        """High-performance parsing alternate to `loads` creating only necessary python objects when simdjson is installed.
         Falls back to `loads` otherwise."""
        raise NotImplementedError("JSON parser not initialized")

