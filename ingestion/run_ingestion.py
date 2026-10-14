from datetime import datetime
from zoneinfo import ZoneInfo

import dlt

from vnstock_src import stock_source


VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

SYMBOLS = [
    "VCB",
    "ACB",
    "HPG",
    "FPT",
    "MWG",
]


def run_pipeline(
    symbols: list[str],
    start_date: str,
    end_date: str,
):
    pipeline = dlt.pipeline(
        pipeline_name="vnstock_pipeline",
        destination="filesystem",
        dataset_name="raw-data",
    )

    source = stock_source(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )

    return pipeline.run(
        source,
        loader_file_format="parquet",
    )


if __name__ == "__main__":
    load_info = run_pipeline(
        symbols=SYMBOLS,
        start_date="2020-01-01",
        end_date=datetime.now(VN_TZ).date().isoformat(),
    )

    print(load_info)