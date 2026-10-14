import dagster as dg

vnexpress_job = dg.define_asset_job(
    name="stock_schedule",
    selection=dg.AssetSelection.all(),
)

vnexpress_schedule = dg.ScheduleDefinition(
    job=vnexpress_job,
    cron_schedule="0 17 * * *",
    execution_timezone="Asia/Ho_Chi_Minh",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)