-- ==================================================================================
-- View for first trade date
-- ==================================================================================

CREATE VIEW vw_first_trade_date AS
SELECT sub.symbol, 
	sub.first_trade_date, 
    p.open AS first_day_open, 
    p.close AS first_day_close, 
    p.high AS first_day_high, 
    p.low AS first_day_low, 
    p.volume AS first_day_volume
FROM
	(
	SELECT symbol, MIN(date) as first_trade_date
	FROM performance 
	GROUP BY symbol
	) sub

INNER JOIN performance p 
	ON p.symbol = sub.symbol AND p.date = sub.first_trade_date

ORDER BY sub.symbol;

SELECT * FROM vw_first_trade_date;

-- ==================================================================================
-- View for milestone prices (7-day, 30-day, 60-day, 90-day, 180-day, 1-year, 2-year)
-- ==================================================================================

-- CREATE VIEW vw_milestone_prices AS
-- SELECT sub_first_day.symbol, 
-- 	sub_first_day.first_trade_date, 
--     p.open AS first_day_open, 
--     p.close AS first_day_close, 
--     p.high AS first_day_high, 
--     p.low AS first_day_low, 
--     p.volume AS first_day_volume
-- FROM
-- 	(
-- 	SELECT symbol, MIN(date) as first_trade_date
-- 	FROM performance 
-- 	GROUP BY symbol
-- 	) sub_first_day ,
--     
--     (
--     SELECT symbol, MIN(date) as seven_day_date
--     FROM performance
--     WHERE date >= first_trade_date + 7
--     GROUP BY symbol
--     ) sub_seven_day

-- INNER JOIN performance p 
-- 	ON p.symbol = sub_first_day.symbol AND p.date = sub_first_day.first_trade_date

-- INNER JOIN performance p 
-- 	ON p.symbol = sub_first_day.symbol AND p.date = sub_seven_day.seven_day_date


-- ORDER BY sub.symbol;

-- SELECT * FROM vw_milestone_prices