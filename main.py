from pathlib import Path

import polars as pl
from loguru import logger

from src.tbills import AWSSettings, Stats, TreasuryBillAnalytics, TreasuryBillScraper

PROJECT_ROOT_DIR: Path = Path(__file__).parent
APP_DATA_DIR: Path = PROJECT_ROOT_DIR / "app" / "data"


def main() -> int:
    aws_settings: AWSSettings = AWSSettings()
    tbills_scraper: TreasuryBillScraper = TreasuryBillScraper(settings=aws_settings)
    data: pl.DataFrame = tbills_scraper.scrape_treasury_data()
    logger.info("Saving scraped data to local app data directory")
    data.select(["maturity", "yield_pct"]).write_csv(APP_DATA_DIR / "daily_yields.csv")
    _: Stats = tbills_scraper.upsert_data(data)

    tbills_analytics: TreasuryBillAnalytics = TreasuryBillAnalytics(
        input_table=data,
        day_count_base=365,
        days_per_week=7,
    )
    break_even_table: pl.DataFrame = tbills_analytics.compute_break_even_rates(
        decimals=4
    )
    logger.info("Saving break-even table to local app data directory")
    break_even_table.write_csv(APP_DATA_DIR / "break_even_yields.csv")

    return 0


if __name__ == "__main__":
    main()
