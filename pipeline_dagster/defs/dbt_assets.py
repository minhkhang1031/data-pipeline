from pathlib import Path

import dagster as dg
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)


DBT_PROJECT_DIR = (
    Path(__file__)
    .joinpath("..", "..", "..", "transformation")
    .resolve()
)


dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
)

dbt_project.prepare_if_dev()


dbt_translator = DagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(
        enable_source_tests_as_checks=True,
    )
)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    dagster_dbt_translator=dbt_translator,
)
def stock_dbt_assets(
    context: dg.AssetExecutionContext,
    dbt: DbtCliResource,
):
    yield from dbt.cli(
        ["build"],
        context=context,
    ).stream()
