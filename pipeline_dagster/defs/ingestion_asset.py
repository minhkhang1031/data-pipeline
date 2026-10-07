import dagster as dg
import dlt

from ingestion.vnstock_src import stock_source


@dg.asset(
    key_prefix=["data_raw"],
)
def stock_raw() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="vnstock_pipeline",
        destination="filesystem",
        dataset_name="raw-data",
    )

    source = stock_source(
        symbols=["VCB", "ACB", "HPG", "FPT", "MWG"],
        start_date="2023-01-01",
        end_date="2026-01-01",
    )

    pipeline.run(
        source,
        loader_file_format="parquet",
    )