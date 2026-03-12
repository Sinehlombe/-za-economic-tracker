import os
from datetime import datetime, timezone
from prefect import flow, task
from prefect.logging import get_run_logger
from sqlalchemy.orm import Session

from pipeline.extract import extract_all, INDICATORS
from pipeline.transform import transform_all
from pipeline.load import (
    get_engine,
    load_indicators,
    create_pipeline_run,
    complete_pipeline_run,
)
from pipeline.models import PipelineRun


@task(name="Extract World Bank Data", retries=3, retry_delay_seconds=30)
def extract_task() -> dict:
    logger = get_run_logger()
    logger.info("Starting extraction from World Bank API")
    raw = extract_all()
    total = sum(len(v) for v in raw.values())
    logger.info(f"Extracted {total} total raw records")
    return raw


@task(name="Transform Records")
def transform_task(raw_data: dict) -> tuple:
    logger = get_run_logger()
    valid, failed = transform_all(raw_data, INDICATORS)
    logger.info(f"Transform result: {len(valid)} valid, {len(failed)} failed")
    return valid, failed


@task(name="Load to PostgreSQL")
def load_task(valid_records: list, source_id: int, database_url: str) -> int:
    engine = get_engine(database_url)
    with Session(engine) as session:
        count = load_indicators(session, valid_records, source_id)
    return count


@flow(name="world-bank-etl", log_prints=True)
def world_bank_pipeline():
    logger = get_run_logger()
    database_url = "postgresql://etl_user:etl_pass@localhost:5432/etl_db"
    engine = get_engine(database_url)

    with Session(engine) as session:
        run = create_pipeline_run(session, "world-bank-etl")
        run_id = run.id

    try:
        raw_data = extract_task()
        total_extracted = sum(len(v) for v in raw_data.values())

        valid_records, failed_records = transform_task(raw_data)
        rows_loaded = load_task(valid_records, source_id=1, database_url=database_url)

        with Session(engine) as session:
            run = session.get(PipelineRun, run_id)
            complete_pipeline_run(
                session, run,
                status="success",
                rows_extracted=total_extracted,
                rows_transformed=len(valid_records),
                rows_loaded=rows_loaded,
                rows_failed=len(failed_records),
            )

        logger.info(f"Pipeline complete. Loaded {rows_loaded} rows.")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        with Session(engine) as session:
            run = session.get(PipelineRun, run_id)
            complete_pipeline_run(session, run, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    world_bank_pipeline()