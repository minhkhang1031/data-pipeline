from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import dagster as dg
import dlt

from ingestion.vnstock_src import stock_source


VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

SYMBOLS = [
    "VCB",
    "ACB",
    "HPG",
    "FPT",
    "MWG",
]


class StockIngestionConfig(dg.Config):
    start_date: str | None = None
    end_date: str | None = None
    lookback_days: int = 10


@dg.asset(
    key=dg.AssetKey(["data_raw", "stock_raw"]),
)
def stock_raw(
    context: dg.AssetExecutionContext,
    config: StockIngestionConfig,
) -> dg.MaterializeResult:
    today = datetime.now(VN_TZ).date()

    end_date = (
        date.fromisoformat(config.end_date)
        if config.end_date
        else today
    )

    start_date = (
        date.fromisoformat(config.start_date)
        if config.start_date
        else end_date - timedelta(
            days=config.lookback_days
        )
    )

    if start_date > end_date:
        raise ValueError(
            "start_date must be <= end_date"
        )

    pipeline = dlt.pipeline(
        pipeline_name="vnstock_pipeline",
        destination="filesystem",
        dataset_name="raw-data",
    )

    load_info = pipeline.run(
        stock_source(
            symbols=SYMBOLS,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        ),
        loader_file_format="parquet",
    )

    context.log.info(
        "Raw ingestion: %s -> %s",
        start_date,
        end_date,
    )

    return dg.MaterializeResult(
        metadata={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "symbols": ", ".join(SYMBOLS),
            "lookback_days": config.lookback_days,
            "load_info": str(load_info),
        }
    )