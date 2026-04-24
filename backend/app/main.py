"""FastAPI application entry point."""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path

from .api.routes import router
from .core.logging import logger
from .core.config import PROJECT_ROOT

app = FastAPI(
    title="DocFusion - 文档理解与多源数据融合系统",
    description="基于大语言模型的文档理解与多源数据融合系统",
    version="1.0.0",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routes
app.include_router(router)

# Serve frontend static files
frontend_dir = PROJECT_ROOT / "frontend"
if frontend_dir.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")


@app.get("/")
async def root():
    """Serve front-end index page."""
    index_path = frontend_dir / "index.html"
    if index_path.exists():
        return FileResponse(str(index_path))
    return {"message": "DocFusion API is running. Visit /docs for API documentation."}


@app.on_event("startup")
async def startup_event():
    logger.info("DocFusion server starting...")
    logger.info(f"Project root: {PROJECT_ROOT}")
    logger.info(f"Frontend dir: {frontend_dir}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
