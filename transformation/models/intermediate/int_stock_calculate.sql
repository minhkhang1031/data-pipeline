with staging_data as (
    select * from {{ ref('stg_stock_raw') }}
),


indicators_base as(
    select
        dlt_record_id,
        dlt_batch_id,
        stock_symbol,
        trading_date,
        price_open,
        price_high,
        price_low,
        price_close,
        trading_volume,

        -- Tính SMA (đường trung bình động) ==> Tính mean close_price trong n phiên.
        -- Đường SMA có thể được sử dụng để xác định xu hướng giá và hỗ trợ trong việc ra quyết định giao dịch.

        -- SMA10 - 20 ==> Dùng cho giao dịch trading lướt sóng (ngắn hạn)
        avg(price_close) over (
            partition by
                stock_symbol
            order by
                trading_date rows between 9 preceding
                and current row
        ) as sma_10,

        avg(price_close) over (
            partition by
                stock_symbol
            order by
                trading_date rows between 19 preceding
                and current row
        ) as sma_20,

        -- SMA 50 ==> Đại diện cho xu hướng của thị trường trong vài tháng (trung hạn)

        avg(price_close) over (
            partition by
                stock_symbol
            order by
                trading_date rows between 49 preceding
                and current row
        ) as sma_50,

        -- Daily return ==> Tỷ lệ phần trăm thay đổi giá so với phiên trước
        (
        price_close - lag(price_close, 1) over (
            partition by 
                stock_symbol
            order by 
                trading_date
        )
        ) / nullif(
            lag(price_close, 1) over (
                partition by
                    stock_symbol    
                order by trading_date
            ),
            0
        ) * 100 as daily_return,

        -- Tỉ lệ giá dao động trong ngày
        (
            (price_high - price_low) / nullif(price_close, 0)
        ) as price_range_ratio,

        -- Tỉ lệ volumn so với trung bình
        trading_volume / nullif(
            avg(trading_volume) over (
                partition by
                    stock_symbol
                order by
                    trading_date rows between 9 preceding
                    and current row
            ),
            0
        ) as volume_ratio_sma10,

        -- Tính giá tăng/giảm so với phiên trước (dùng cho mô hình dự đoán xu hướng giá)
        greatest(
            price_close - lag(price_close, 1) over (
                partition by
                    stock_symbol
                order by trading_date
            ),
            0
        ) as price_gain,

        greatest(
            lag(price_close, 1) over (
                partition by
                    stock_symbol
                order by trading_date
            ) - price_close,
            0
        ) as price_loss

        from staging_data
),

-- Tính RSI
rsi_calculation as (
    select *,
        avg(price_gain) over (
            partition by
                stock_symbol
            order by
                trading_date rows between 13 preceding
                and current row
        ) as avg_gain_14,

        avg(price_loss) over (
            partition by
                stock_symbol
            order by
                trading_date rows between 13 preceding
                and current row
        ) as avg_loss_14

    from indicators_base
)

select
    dlt_record_id,
    dlt_batch_id,
    stock_symbol,
    trading_date,
    price_open,
    price_close,
    price_high,
    price_low,
    trading_volume,
    daily_return,
    price_range_ratio,
    sma_10,
    sma_20,
    sma_50,
    volume_ratio_sma10,

    -- Tỉ lệ price_close so với đường sma20
    (price_close - sma_20) / nullif(sma_20, 0) as price_to_sma20_ratio,
    
    -- RSI 14 ngày
    case
        when avg_loss_14 = 0 then 100
        else 100 - (100 / (1 + (avg_gain_14 / avg_loss_14)))
    end as rsi_14

from rsi_calculation
