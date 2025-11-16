from fastapi import FastAPI
from api.router import router

app = FastAPI(title="Crypto Screener")
app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000)

from api.routers.ingestion import router as ingestion_router
app.include_router(ingestion_router)

from api.ingestion_scan_shim import router as ingestion_scan_router
app.include_router(ingestion_scan_router)

from api.ingestion_symbols_shim import router as ingestion_symbols_router
app.include_router(ingestion_symbols_router)
\nfrom api.ingestion_fix import router as ingestion_fix_router\napp.include_router(ingestion_fix_router)\n