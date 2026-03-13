from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class IndicatorResponse(BaseModel):
    id: int
    country_code: str
    country_name: str
    indicator_code: str
    indicator_name: str
    year: int
    value: Optional[float]
    unit: Optional[str]

    class Config:
        from_attributes = True


class IndicatorCodeResponse(BaseModel):
    indicator_code: str
    indicator_name: str


class PipelineRunResponse(BaseModel):
    id: int
    flow_name: str
    status: str
    rows_extracted: Optional[int]
    rows_transformed: Optional[int]
    rows_loaded: Optional[int]
    rows_failed: Optional[int]
    error_message: Optional[str]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    duration_seconds: Optional[float]

    class Config:
        from_attributes = True