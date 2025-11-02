from fastapi import FastAPI
from contextlib import asynccontextmanager
import uvicorn
from app.database import Base, engine
from app.scheduler import start_scheduler
import app.config as config
from app.routes import mgnrega


Base.metadata.create_all(bind=engine)


@asynccontextmanager
async def lifespan(app: FastAPI):
    start_scheduler()
    print("Scheduler started from lifespan")
    yield
    print("App shutting down...")

app = FastAPI(
    title="MGNREGA Data Backend",
    description="Backend service that fetches, caches, and stores MGNREGA data for Streamlit visualization.",
    version="1.0.0",
    lifespan=lifespan,
)

# register routes
app.include_router(mgnrega.router)

@app.get("/")
def root():
    return {
        "message": "Welcome to the MGNREGA Backend API ",
        "target_state": config.TARGET_STATE,
        "financial_year": config.FIN_YEAR,
        "data_source": config.MGNREGA_API_URL
    }

# app.include_router(mgnrega.router)

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
