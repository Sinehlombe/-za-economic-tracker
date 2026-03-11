from pydantic import BaseModel, field_validator
from typing import Optional
from pipeline.utils import get_logger

logger = get_logger(__name__)


class IndicatorRecord(BaseModel):
    country_code: str
    country_name: str
    indicator_code: str
    indicator_name: str
    year: int
    value: Optional[float]
    unit: str = ""

    @field_validator("year")
    @classmethod
    def year_must_be_reasonable(cls, v):
        if not (1960 <= v <= 2030):
            raise ValueError(f"Year {v} out of expected range")
        return v

    @field_validator("country_code")
    @classmethod
    def country_code_uppercase(cls, v):
        return v.upper().strip()


def transform_indicator_records(
    raw_records: list[dict],
    indicator_code: str,
    indicator_name: str,
) -> tuple[list[IndicatorRecord], list[dict]]:
    valid = []
    failed = []

    for record in raw_records:
        raw_value = record.get("value")
        value = float(raw_value) if raw_value is not None else None

        try:
            validated = IndicatorRecord(
                country_code=record.get("countryiso3code") or record["country"]["id"],
                country_name=record["country"]["value"],
                indicator_code=indicator_code,
                indicator_name=indicator_name,
                year=int(record["date"]),
                value=value,
            )
            valid.append(validated)
        except Exception as e:
            failed.append({"raw": record, "error": str(e)})
            logger.warning(f"Validation failed for record {record.get('date')}: {e}")

    logger.info(
        f"Transform complete for {indicator_code}: "
        f"{len(valid)} valid, {len(failed)} failed"
    )
    return valid, failed


def transform_all(
    raw_data: dict[str, list[dict]],
    indicator_map: dict[str, str],
) -> tuple[list[IndicatorRecord], list[dict]]:
    all_valid = []
    all_failed = []

    for code, records in raw_data.items():
        name = indicator_map.get(code, code)
        valid, failed = transform_indicator_records(records, code, name)
        all_valid.extend(valid)
        all_failed.extend(failed)

    return all_valid, all_failed