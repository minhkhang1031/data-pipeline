from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets


DBT_PROJECT_DIR = (
    Path(__file__)
    .joinpath("..", "..", "..", "transformation")
    .resolve()
)


@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
)
def dbt_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
):
    yield from dbt.cli(
        ["build"],
        context=context,
    ).stream()