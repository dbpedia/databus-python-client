import sys
import types

# Provide a lightweight fake SPARQLWrapper module for tests when not installed.
if "SPARQLWrapper" not in sys.modules:
    mod = types.ModuleType("SPARQLWrapper")
    mod.JSON = None

    class DummySPARQL:
        def __init__(self, *args, **kwargs):
            pass

        def setQuery(self, q):
            self._q = q

        def setReturnFormat(self, f):
            self._fmt = f

        def setCustomHttpHeaders(self, h):
            self._headers = h

        def query(self):
            class R:
                def convert(self):
                    return {"results": {"bindings": []}}

            return R()

    mod.SPARQLWrapper = DummySPARQL
    sys.modules["SPARQLWrapper"] = mod
