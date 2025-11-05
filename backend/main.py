from fastapi import FastAPI
from api.router import router

app = FastAPI(title="Crypto Screener")
app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000)

from api.routers.ingestion import router as ingestion_router
app.include_router(ingestion_router)
