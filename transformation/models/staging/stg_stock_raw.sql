
with source_data as (

    select *
    from {{ source('data_raw', 'stock_raw') }}

),

normalized_data as (

    select
        cast(_dlt_id as varchar) as dlt_record_id,

        cast(_dlt_load_id as varchar) as dlt_batch_id,

        upper(
            cast(symbol as varchar)
        ) as stock_symbol,

        cast(
            cast(time as timestamptz)
            as date
        ) as trading_date,

        cast(open as double) as price_open,

        cast(high as double) as price_high,

        cast(low as double) as price_low,

        cast(close as double) as price_close,

        cast(volume as bigint) as trading_volume

    from source_data

)

select
    dlt_record_id,
    dlt_batch_id,
    stock_symbol,
    trading_date,
    price_open,
    price_high,
    price_low,
    price_close,
    trading_volume

from normalized_data

qualify row_number() over (
    partition by
        stock_symbol,
        trading_date

    order by
        dlt_batch_id desc
) = 1