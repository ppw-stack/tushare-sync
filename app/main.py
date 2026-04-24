from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.api.routes import router as api_router
from app.config import settings
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Tushare Sync服务启动")
    yield
    logger.info("Tushare Sync服务关闭")

app = FastAPI(title="Tushare Sync API", version="1.0.0", lifespan=lifespan)
app.include_router(api_router, prefix="/api")

@app.get("/health")
async def health():
    return {"status": "ok"}
