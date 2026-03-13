from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from typing import Optional
from api.schemas import IndicatorResponse, IndicatorCodeResponse
from pipeline.models import EconomicIndicator

DATABASE_URL = "postgresql://etl_user:etl_pass@localhost:5432/etl_db"


def get_db():
    engine = create_engine(DATABASE_URL)
    db = Session(engine)
    try:
        yield db
    finally:
        db.close()


router = APIRouter()


@router.get("/", response_model=list[IndicatorResponse])
def list_indicators(
    indicator_code: Optional[str] = Query(None),
    country_code: str = Query("ZA"),
    start_year: Optional[int] = Query(None),
    end_year: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    query = db.query(EconomicIndicator).filter(
        EconomicIndicator.country_code.ilike(f"%{country_code}%")
    )
    if indicator_code:
        query = query.filter(EconomicIndicator.indicator_code == indicator_code)
    if start_year:
        query = query.filter(EconomicIndicator.year >= start_year)
    if end_year:
        query = query.filter(EconomicIndicator.year <= end_year)
    return query.order_by(EconomicIndicator.year).all()


@router.get("/codes", response_model=list[IndicatorCodeResponse])
def list_indicator_codes(db: Session = Depends(get_db)):
    results = (
        db.query(
            EconomicIndicator.indicator_code,
            EconomicIndicator.indicator_name,
        )
        .distinct()
        .all()
    )
    return [
        {"indicator_code": r.indicator_code, "indicator_name": r.indicator_name}
        for r in results
    ]