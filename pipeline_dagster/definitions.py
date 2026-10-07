from pathlib import Path
import dagster as dg
from .defs.resources import database_resource, dbt_resource

# Load definitions and immediately bind resources into a single global variable
defs = dg.load_from_defs_folder(
    path_within_project=Path(__file__).parent,
).with_resources(
    {
        "database": database_resource,
        "dbt": dbt_resource,
    }
)
