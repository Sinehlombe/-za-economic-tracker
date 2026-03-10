from sqlalchemy import (
    Column, Integer, String, Float, Date,
    DateTime, Boolean, Text, ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class DataSource(Base):
    __tablename__ = "data_sources"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, unique=True)
    base_url = Column(String(500), nullable=False)
    description = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class EconomicIndicator(Base):
    __tablename__ = "economic_indicators"

    id = Column(Integer, primary_key=True)
    country_code = Column(String(10), nullable=False)
    country_name = Column(String(100), nullable=False)
    indicator_code = Column(String(50), nullable=False)
    indicator_name = Column(String(200), nullable=False)
    year = Column(Integer, nullable=False)
    value = Column(Float)
    unit = Column(String(50))
    source_id = Column(Integer, ForeignKey("data_sources.id"))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint(
            "country_code", "indicator_code", "year",
            name="uq_indicator_country_year"
        ),
    )


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True)
    flow_name = Column(String(100), nullable=False)
    status = Column(String(20), nullable=False)
    rows_extracted = Column(Integer, default=0)
    rows_transformed = Column(Integer, default=0)
    rows_loaded = Column(Integer, default=0)
    rows_failed = Column(Integer, default=0)
    error_message = Column(Text)
    started_at = Column(DateTime(timezone=True), server_default=func.now())
    completed_at = Column(DateTime(timezone=True))
    duration_seconds = Column(Float)