from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routers import indicators, pipeline_runs

app = FastAPI(title="ZA Economic Tracker API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(indicators.router, prefix="/api/indicators")
app.include_router(pipeline_runs.router, prefix="/api/pipeline-runs")


@app.get("/health")
def health():
    return {"status": "ok"}