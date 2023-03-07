"""Import JSON parser trying faster parsers first: simdjson, ujson, json"""

try:
    import simdjson as json
except ImportError:
    try:
        import ujson as json
    except ImportError:
        import json
