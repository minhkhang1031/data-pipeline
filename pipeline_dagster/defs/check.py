import dagster as dg
from dagster_duckdb import DuckDBResource


EXPECTED_SYMBOL_COUNT = 5


@dg.asset_check(
    asset=dg.AssetKey(["mart_market_trend"]),
    blocking=True,
    description=(
        "Kiểm tra dataset cuối cùng có thể dùng cho ML."
    ),
)
def ml_dataset_is_ready(
    database: DuckDBResource,
) -> dg.AssetCheckResult:
    with database.get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS row_count,

                COUNT(DISTINCT stock_symbol)
                    AS symbol_count,

                COUNT(*) FILTER (
                    WHERE
                        stock_symbol IS NULL
                        OR trading_date IS NULL
                        OR feature_daily_return_pct IS NULL
                        OR feature_intraday_volatility_pct IS NULL
                        OR feature_distance_to_ma20_pct IS NULL
                        OR feature_volume_shock_ratio IS NULL
                        OR feature_rsi_14 IS NULL
                        OR feature_sma_10 IS NULL
                        OR feature_sma_20 IS NULL
                        OR feature_sma_50 IS NULL
                        OR target_next_day_up IS NULL
                        OR target_next_day_up NOT IN (0, 1)
                ) AS invalid_row_count

            FROM main.mart_market_trend
            """
        ).fetchone()

    row_count = row[0]
    symbol_count = row[1]
    invalid_row_count = row[2]

    passed = (
        row_count > 5
        and symbol_count == EXPECTED_SYMBOL_COUNT
        and invalid_row_count <= 5  
    )

    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "row_count": row_count,
            "symbol_count": symbol_count,
            "invalid_row_count": invalid_row_count,
        },
    )
