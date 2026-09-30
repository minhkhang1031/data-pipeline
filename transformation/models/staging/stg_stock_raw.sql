with source_data as (
    select * from {{ source('data_raw', 'stock_raw') }}
)

select 
    cast(_dlt_id as varchar) as dlt_record_id,
    cast(_dlt_load_id as varchar) as dlt_batch_id,
    upper(cast(symbol as varchar)) as stock_symbol,
    -- Dùng 2 cast lồng nhau để convert time -> timestamptz -> date, vì nếu chỉ cast 1 lần thì sẽ bị lỗi Conversion Error
    cast( cast(time as timestamptz) as date ) as trading_date,
    cast(open as double) as price_open,
    cast(close as double) as price_close,
    cast(high as double) as price_high,
    cast(low as double) as price_low,
    cast(volume as bigint) as trading_volume

from source_data