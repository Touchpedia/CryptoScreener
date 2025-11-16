import importlib
m = importlib.import_module("main")
from fastapi.routing import APIRoute
for r in m.app.routes:
    if isinstance(r, APIRoute):
        mod = getattr(r.endpoint, "__module__", "")
        print(f"{r.path} -> {mod}")