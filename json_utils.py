# -*- coding: utf-8 -*-
import json
import logging

logger = logging.getLogger("json_utils")

# Tentative d'utiliser orjson (le plus rapide) ou ujson
try:
    import orjson
    def loads(s):
        return orjson.loads(s)
    def dumps(obj, **kwargs):
        # orjson.dumps retourne des bytes
        return orjson.dumps(obj).decode('utf-8')
    FAST_JSON = "orjson"
except ImportError:
    try:
        import ujson
        def loads(s):
            return ujson.loads(s)
        def dumps(obj, **kwargs):
            return ujson.dumps(obj, **kwargs)
        FAST_JSON = "ujson"
    except ImportError:
        loads = json.loads
        dumps = json.dumps
        FAST_JSON = "standard"

logger.info("JSON engine: %s", FAST_JSON)
