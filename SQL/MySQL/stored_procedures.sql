-- ==========================================================================
-- SP for populated days trading since first date
-- ==========================================================================

USE ipo_tracker;

DELIMITER $$
CREATE PROCEDURE sp_update_days_trading()

BEGIN
	SET SQL_SAFE_UPDATES = 0;
	-- update the days trading to 0 for the initial date
	UPDATE performance p
	INNER JOIN
		(
		SELECT symbol, MIN(date) as first_trade_day
		FROM performance 
		GROUP BY symbol
		) sub
		ON sub.symbol = p.symbol AND sub.first_trade_day = p.date
    SET days_trading = 0;	
	
	-- update the days trading for all other dates for a stock past the initial date
	UPDATE performance p
	INNER JOIN
		(
		SELECT symbol, MIN(date) as first_trade_day
		FROM performance 
		GROUP BY symbol
		) sub
        ON sub.symbol = p.symbol 
	SET days_trading = DATEDIFF(p.date, sub.first_trade_day);
    
    SET SQL_SAFE_UPDATES = 1;
END$$

DELIMITER ; 



-- ==========================================================================
-- SP for market cap size
-- ==========================================================================

USE ipo_tracker;

DELIMITER $$
CREATE PROCEDURE sp_market_cap_classification()

BEGIN
	SET SQL_SAFE_UPDATES = 0;

	UPDATE market_cap 
	SET market_cap_classification = 
		CASE
			WHEN market_cap >= 200000000000 THEN 'Mega-cap'
            WHEN market_cap >= 10000000000 AND market_cap < 200000000000 THEN 'Large-cap'
            WHEN market_cap >= 2000000000 AND market_cap < 10000000000 THEN 'Mid-cap'
            WHEN market_cap >= 300000000 AND market_cap < 2000000000 THEN 'Small-cap'
            WHEN market_cap >= 50000000 AND market_cap <  300000000 THEN 'Micro-cap'
            WHEN market_cap > 0 AND market_cap < 50000000 THEN 'Nano-cap'
		END
	WHERE market_cap IS NOT NULL;
    
    SET SQL_SAFE_UPDATES = 1;
END$$

DELIMITER ; 


-- ==========================================================================
-- add duplicate check for performance
-- ==========================================================================

USE ipo_tracker;

DELIMITER $$
CREATE PROCEDURE sp_check_performance_dupes()

BEGIN
	SET SQL_SAFE_UPDATES = 0;
	-- update the days trading to 0 for the initial date
	UPDATE performance p
	INNER JOIN
		(
		SELECT symbol, date, count(*) as records
		FROM performance
		GROUP BY symbol, date
		) sub
		ON sub.symbol = p.symbol AND sub.date = p.date
    SET p.symbol_date_records = records;	
    
    SET SQL_SAFE_UPDATES = 1;
END$$

DELIMITER ; 
