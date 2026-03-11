import httpx
from pipeline.utils import get_logger

logger = get_logger(__name__)

WORLD_BANK_BASE = "https://api.worldbank.org/v2"

INDICATORS = {
    "NY.GDP.MKTP.CD": "GDP (current US$)",
    "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",
    "SL.UEM.TOTL.ZS": "Unemployment, total (% of total labor force)",
    "EG.USE.ELEC.KH.PC": "Electric power consumption (kWh per capita)",
}


def fetch_indicator(
    indicator_code: str,
    country: str = "ZA",
    start_year: int = 2000,
    end_year: int = 2023,
) -> list[dict]:
    all_records = []
    page = 1

    while True:
        url = f"{WORLD_BANK_BASE}/country/{country}/indicator/{indicator_code}"
        params = {
            "format": "json",
            "date": f"{start_year}:{end_year}",
            "per_page": 100,
            "page": page,
        }

        try:
            response = httpx.get(url, params=params, timeout=30)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching {indicator_code}: {e}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error fetching {indicator_code}: {e}")
            raise

        data = response.json()

        if not data or len(data) < 2 or data[1] is None:
            logger.warning(f"No data returned for {indicator_code}")
            break

        metadata = data[0]
        records = data[1]
        all_records.extend(records)

        total_pages = metadata.get("pages", 1)
        if page >= total_pages:
            break
        page += 1

    logger.info(f"Extracted {len(all_records)} records for {indicator_code}")
    return all_records


def extract_all() -> dict[str, list[dict]]:
    results = {}
    for code in INDICATORS:
        results[code] = fetch_indicator(code)
    return results