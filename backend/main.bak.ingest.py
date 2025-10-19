from fastapi import FastAPI

app = FastAPI(title="Health Minimal")

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/api/status")
def status():
    # deliberately always 200
    return {"ok": True, "redis": None, "note": "minimal"}
