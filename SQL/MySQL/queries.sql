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

-- 7-days
CREATE VIEW vw_7_days_trading AS
SELECT sub.symbol, 
	sub.date_7_days, 
    p.open AS open_7_days, 
    p.close AS close_7_days
FROM
	(
	SELECT symbol, MIN(date) as date_7_days
	FROM performance 
    WHERE days_trading >= 7
	GROUP BY symbol
	) sub

INNER JOIN performance p 
	ON p.symbol = sub.symbol AND p.date = sub.date_7_days

ORDER BY sub.symbol;

SELECT * FROM vw_7_days_trading;

-- 30-days
CREATE VIEW vw_30_days_trading AS
SELECT sub.symbol, 
	sub.date_30_days, 
    p.open AS open_30_days, 
    p.close AS close_30_days
FROM
	(
	SELECT symbol, MIN(date) as date_30_days
	FROM performance 
    WHERE days_trading >= 30
	GROUP BY symbol
	) sub

INNER JOIN performance p 
	ON p.symbol = sub.symbol AND p.date = sub.date_30_days

ORDER BY sub.symbol;

SELECT * FROM vw_30_days_trading;


-- 60-days
CREATE VIEW vw_60_days_trading AS
SELECT sub.symbol, 
	sub.date_60_days, 
    p.open AS open_60_days, 
    p.close AS close_60_days
FROM
	(
	SELECT symbol, MIN(date) as date_60_days
	FROM performance 
    WHERE days_trading >= 60
	GROUP BY symbol
	) sub

INNER JOIN performance p 
	ON p.symbol = sub.symbol AND p.date = sub.date_60_days

ORDER BY sub.symbol;

SELECT * FROM vw_60_days_trading;


-- 90-days
CREATE VIEW vw_90_days_trading AS
SELECT sub.symbol, 
	sub.date_90_days, 
    p.open AS open_90_days, 
    p.close AS close_90_days
FROM
	(
	SELECT symbol, MIN(date) as date_90_days
	FROM performance 
    WHERE days_trading >= 90
	GROUP BY symbol
	) sub

INNER JOIN performance p 
	ON p.symbol = sub.symbol AND p.date = sub.date_90_days

ORDER BY sub.symbol;

SELECT * FROM vw_90_days_trading;



-- 180-days
CREATE VIEW vw_180_days_trading AS
SELECT sub.symbol, 
	sub.date_180_days, 
    p.open AS open_180_days, 
    p.close AS close_180_days
FROM
	(
	SELECT symbol, MIN(date) as date_180_days
	FROM performance 
    WHERE days_trading >= 180
	GROUP BY symbol
	) sub

INNER JOIN performance p 
	ON p.symbol = sub.symbol AND p.date = sub.date_180_days

ORDER BY sub.symbol;

SELECT * FROM vw_180_days_trading;


-- 365-days
CREATE VIEW vw_365_days_trading AS
SELECT sub.symbol, 
	sub.date_365_days, 
    p.open AS open_365_days, 
    p.close AS close_365_days
FROM
	(
	SELECT symbol, MIN(date) as date_365_days
	FROM performance 
    WHERE days_trading >= 365
	GROUP BY symbol
	) sub

INNER JOIN performance p 
	ON p.symbol = sub.symbol AND p.date = sub.date_365_days

ORDER BY sub.symbol;

SELECT * FROM vw_365_days_trading;


-- ==================================================================================
-- vw_milestone_trading
-- ==================================================================================

USE ipo_tracker;
ALTER VIEW vw_stocks_performance AS
SELECT S.symbol, S.company, S.exchange, S.proposed_share_price, S.shares_offered, S.dollar_val_shares, 
	CI.city, CI.state, CI.country, CI.zip_code, CI.industry, CI.sector,
    RMC.date date_of_market_cap, RMC.market_cap, RMC.market_cap_formatted, RMC.market_cap_classification,
	F.first_trade_date, YEAR(F.first_trade_date) AS first_trade_year, MONTH(F.first_trade_date) AS first_trade_month, DATE_FORMAT(F.first_trade_date,'%Y-%m') AS first_trade_year_month,
		F.first_day_open, F.first_day_close, F.first_day_high, F.first_day_low, F.first_day_volume,
    MRP.date AS date_most_recent, MRP.days_trading, MRP.open AS most_recent_open, MRP.close AS most_recent_close,
	SEV.date_7_days, SEV.open_7_days, SEV.close_7_days, 
    THI.date_30_days, THI.open_30_days, THI.close_30_days, 
    SIX.date_60_days, SIX.open_60_days, SIX.close_60_days, 
    NIN.date_90_days, NIN.open_90_days, NIN.close_90_days, 
    OE.date_180_days, OE.open_180_days, OE.close_180_days,
	TSF.date_365_days, TSF.open_365_days, TSF.close_365_days
FROM vw_first_trade_date F

INNER JOIN stocks S
	ON F.symbol = S.symbol
    
LEFT JOIN company_info CI
	ON F.symbol = CI.symbol

-- create and join the most recent market cap information
LEFT JOIN 
	(
    SELECT MC.*
	FROM market_cap MC
		INNER JOIN
			(
			SELECT symbol, max(date) as most_recent_date
			FROM market_cap 
			GROUP BY symbol
			) sub
		ON MC.symbol = sub.symbol AND MC.date = sub.most_recent_date 
	) AS RMC
    ON F.symbol = RMC.symbol
    
-- get mileston performance numbers
LEFT JOIN vw_7_days_trading SEV
	ON F.symbol = SEV.symbol
LEFT JOIN vw_30_days_trading THI
	ON F.symbol = THI.symbol
LEFT JOIN vw_60_days_trading SIX
	ON F.symbol = SIX.symbol
LEFT JOIN vw_90_days_trading NIN
	ON F.symbol = NIN.symbol
LEFT JOIN vw_180_days_trading OE
	ON F.symbol = OE.symbol
LEFT JOIN vw_365_days_trading TSF
	ON F.symbol = TSF.symbol

-- most recent performance numbers
LEFT JOIN 
	(
	SELECT P.*
	FROM performance P
		INNER JOIN
			(
			SELECT symbol, max(date) as most_recent_date
			FROM performance
            WHERE open IS NOT NULL and open <> 0
			GROUP BY symbol
			) sub
		ON P.symbol = sub.symbol AND P.date = sub.most_recent_date 
	) AS MRP
    ON F.symbol = MRP.symbol

WHERE F.first_day_open IS NOT NULL;

-- ==================================================================================
-- most recent queries (market cap & performance)
-- ==================================================================================
SELECT MC.*
FROM market_cap MC
	INNER JOIN
		(
		SELECT symbol, max(date) as most_recent_date
		FROM market_cap 
		GROUP BY symbol
		) sub
	ON MC.symbol = sub.symbol AND MC.date = sub.most_recent_date ;


    
SELECT P.*
FROM performance P
	INNER JOIN
		(
		SELECT symbol, max(date) as most_recent_date
		FROM performance 
		GROUP BY symbol
		) sub
	ON P.symbol = sub.symbol AND P.date = sub.most_recent_date ;


-- ==================================================================================
-- update all stock prices in performance table to NULL where symbol has issues
-- ==================================================================================

SET SQL_SAFE_UPDATES = 0;
UPDATE performance
SET open = NULL, close = NULL, high = NULL, low = NULL 
WHERE symbol IN
	(SELECT symbol FROM stock_issues GROUP BY symbol);
SET SQL_SAFE_UPDATES = 1;


-- ==================================================================================
-- duplicate performance rows
-- ==================================================================================
SELECT p.*
FROM performance p
INNER JOIN 
	(
	SELECT symbol, date, count(*) as records
	FROM performance
	GROUP BY symbol, date
	HAVING records > 1
	) as dupes
    ON p.symbol = dupes.symbol AND p.date = dupes.date
ORDER BY p.symbol, p.date
