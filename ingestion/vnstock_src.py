import dlt
from vnstock import Market


@dlt.source(name="vnstock_source")
def stock_source(
    symbols: list[str],
    start_date: str,
    end_date: str,
):
    market = Market()

    @dlt.resource(
        name="stock_history",
        primary_key=("symbol", "time"),
        write_disposition="append",
    )
    def stock_history_resource():
        errors: list[str] = []

        for symbol in symbols:
            try:
                df = market.equity(symbol).ohlcv(
                    start=start_date,
                    end=end_date,
                )

                if df is None or df.empty:
                    errors.append(f"{symbol}: no data returned")
                    continue

                df.columns = [
                    str(column).lower()
                    for column in df.columns
                ]

                df["symbol"] = symbol.upper()

                yield df[
                    [
                        "time",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "symbol",
                    ]
                ].to_dict(orient="records")

            except Exception as exc:
                errors.append(f"{symbol}: {exc}")

        if errors:
            raise RuntimeError(
                "vnstock extraction failed:\n"
                + "\n".join(errors)
            )

    return stock_history_resource