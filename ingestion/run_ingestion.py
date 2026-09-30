import dlt
from data_pipeline_stock.ingestion.vnstock_src import stock_source

def run_pipeline(symbols: list[str], start_date: str, end_date: str):
    
    # Khởi tạo pipeline lưu trữ dạng tập tin
    pipeline = dlt.pipeline(
        pipeline_name="vnstock_pipeline",
        destination="filesystem", 
        dataset_name="raw-data"
    )
    
    # Gọi Source
    source = stock_source(
        symbols=symbols, 
        start_date=start_date, 
        end_date=end_date
    )

    print("Extracting data from vnstock")
    
    load_info = pipeline.run(source, loader_file_format="parquet")
    
    print("\nDone!")
    print(load_info)

if __name__ == "__main__":

    symbols = ["VCB", "ACB", "HPG", "FPT", "MWG"]
    start_date = "2023-01-01"
    end_date = "2026-01-01"

    run_pipeline(symbols, start_date, end_date)

