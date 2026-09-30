import dlt
from vnstock import Market, Fundamental

@dlt.source(name="vnstock_source")
def stock_source(symbols: list[str], start_date: str, end_date: str):
    mkt = Market()

    # write_disposition có thể chọn append để giữ lại dữ liệu cũ hoặc replace để thay thế dữ liệu cũ
    @dlt.resource(name="stock_history", write_disposition="append") 
    def stock_history_resource():
        for symbol in symbols:
            try:
                print(f"Đang extract dữ liệu mã: {symbol}")
                
                
                df = mkt.equity(symbol).ohlcv(start=start_date, end=end_date)
                
                if df is not None and not df.empty:

                    df.columns = [col.lower() for col in df.columns]
                    df["symbol"] = symbol

                    yield df.to_dict(orient="records")

            except Exception as e:
                print(f"Đã xảy ra lỗi khi extract {symbol}: {e}")
                continue

    return stock_history_resource
