with int_data as (
    select * from {{ref('int_stock_calculate')}}
),

ml_dataset as (
    select
        dlt_record_id,
        stock_symbol,
        trading_date,
        price_open,
        price_high,
        price_low,
        price_close,
        trading_volume,

        -- Round các feature để tránh overfitting khi giá trị feature quá chi tiết
        round(daily_return, 4) as feature_daily_return_pct,
        round(price_range_ratio, 4) as feature_intraday_volatility_pct,
        round(price_to_sma20_ratio, 4) as feature_distance_to_ma20_pct,
        round(volume_ratio_sma10, 4) as feature_volume_shock_ratio,
        round(rsi_14, 4) as feature_rsi_14,
        round(sma_10, 2) as feature_sma_10,
        round(sma_20, 2) as feature_sma_20,
        round(sma_50, 2) as feature_sma_50,

        -- Chuẩn bị label
        -- Giá đóng cửa ngày mai
        lead(price_close, 1) over (
            partition by
                stock_symbol
            order by trading_date
        ) as next_day_close,

        -- Giá đóng cửa 7 ngày (1 tuần)
        lead(price_close, 7) over (partition by stock_symbol order by trading_date) as next_7d_close,
                
                price_close as current_close

            from int_data
        )

select
    dlt_record_id,
    stock_symbol,
    trading_date,
    price_open,
    price_high,
    price_low,
    price_close,
    trading_volume,
    feature_daily_return_pct,
    feature_intraday_volatility_pct,
    feature_distance_to_ma20_pct,
    feature_volume_shock_ratio,
    feature_rsi_14,
    feature_sma_10,
    feature_sma_20,
    feature_sma_50,

    -- Label Y: Giá ngày mai tăng (1) hay giảm (0)
    case
        when next_day_close > current_close then 1
        else 0
    end as target_next_day_up,

    -- 1 tuần sau giá tăng hay giảm --> AI làm chưa hiểu rõ
    case
        when next_7d_close > current_close then 1
        else 0
    end as target_7d_up
    from ml_dataset
    where
        -- Tiêu chuẩn 1: Loại bỏ thời gian khởi động (Khiến các chỉ báo MA, RSI bị NULL ở những ngày đầu)
        feature_sma_50 is not null
        and feature_rsi_14 is not null

    -- Tiêu chuẩn 2: Loại bỏ dữ liệu rác (Những ngày lễ sàn đóng cửa, volume = 0)
    and trading_volume > 0

    -- Tiêu chuẩn 3: Chặn đứng rò rỉ dữ liệu (Data Leakage)
    -- Loại bỏ những ngày cuối cùng của chuỗi dữ liệu vì chưa biết tương lai thực tế diễn ra thế nào
    and next_day_close is not null and next_7d_close is not null