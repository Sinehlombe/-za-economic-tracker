from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from pipeline.models import EconomicIndicator, PipelineRun
from pipeline.utils import get_logger
from datetime import datetime, timezone

logger = get_logger(__name__)


def get_engine(database_url: str):
    return create_engine(database_url, pool_pre_ping=True)


def load_indicators(
    session: Session,
    records: list,
    source_id: int,
    batch_size: int = 500,
) -> int:
    if not records:
        return 0

    rows = [
        {
            "country_code": r.country_code,
            "country_name": r.country_name,
            "indicator_code": r.indicator_code,
            "indicator_name": r.indicator_name,
            "year": r.year,
            "value": r.value,
            "unit": r.unit,
            "source_id": source_id,
        }
        for r in records
    ]

    total_loaded = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]

        stmt = insert(EconomicIndicator).values(batch)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_indicator_country_year",
            set_={
                "value": stmt.excluded.value,
                "updated_at": datetime.now(timezone.utc),
            },
        )

        session.execute(stmt)
        session.commit()
        total_loaded += len(batch)
        logger.info(f"Loaded batch {i // batch_size + 1}: {len(batch)} rows")

    return total_loaded


def create_pipeline_run(session: Session, flow_name: str) -> PipelineRun:
    run = PipelineRun(flow_name=flow_name, status="running")
    session.add(run)
    session.commit()
    session.refresh(run)
    return run


def complete_pipeline_run(
    session: Session,
    run: PipelineRun,
    status: str,
    rows_extracted: int = 0,
    rows_transformed: int = 0,
    rows_loaded: int = 0,
    rows_failed: int = 0,
    error_message: str = None,
):
    run.status = status
    run.rows_extracted = rows_extracted
    run.rows_transformed = rows_transformed
    run.rows_loaded = rows_loaded
    run.rows_failed = rows_failed
    run.error_message = error_message
    run.completed_at = datetime.now(timezone.utc)

    if run.started_at:
        delta = run.completed_at - run.started_at
        run.duration_seconds = delta.total_seconds()

    session.commit()