
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

        round(daily_return, 4) as feature_daily_return_pct,
        round(price_range_ratio, 4) as feature_intraday_volatility_pct,
        round(price_to_sma20_ratio, 4) as feature_distance_to_ma20_pct,
        round(volume_ratio_sma10, 4) as feature_volume_shock_ratio,
        round(rsi_14, 4) as feature_rsi_14,
        round(sma_10, 2) as feature_sma_10,
        round(sma_20, 2) as feature_sma_20,
        round(sma_50, 2) as feature_sma_50,
        round(macd_line, 4) as feature_macd_line,
        round(macd_signal, 4) as feature_macd_signal,
        round(macd_histogram, 4) as feature_macd_histogram,

        lead(price_close, 1) over (partition by stock_symbol order by trading_date) as next_day_close,
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
    feature_macd_line,
    feature_macd_signal,
    feature_macd_histogram,
    case
        when next_day_close > current_close then 1
        else 0
    end as target_next_day_up,
    case
        when next_7d_close > current_close then 1
        else 0
    end as target_7d_up
    from ml_dataset
    where

    feature_sma_50 is not null
    and feature_rsi_14 is not null
    and feature_macd_signal is not null
    and trading_volume > 0
    and next_day_close is not null
    and next_7d_close is not null
    and feature_daily_return_pct is not null