from fastapi import FastAPI
from backend.api.router import router

app = FastAPI(title='Crypto Screener')
app.include_router(router)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run('main:app', host='0.0.0.0', port=8000)
