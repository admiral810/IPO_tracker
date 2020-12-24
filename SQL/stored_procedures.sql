--=====================================================================
-- SP to pupulate days trading column in performance table
--=====================================================================

ALTER TABLE performance
ADD COLUMN days_trading INT;

SELECT * FROM performance ORDER BY id

-- update the first date for a symbol to 0 for days trading
UPDATE performance p
SET days_trading = 0
FROM
	(
	SELECT symbol, MIN(date) as first_trade_day
	FROM performance 
	GROUP BY symbol
	) sub
WHERE sub.symbol = p.symbol AND sub.first_trade_day = p.date;

-- update the remaining days trading values based on the initial trading day
UPDATE performance p
SET days_trading = p.date - sub.first_trade_day
FROM
	(
	SELECT symbol, MIN(date) as first_trade_day
	FROM performance 
	GROUP BY symbol
	) sub
WHERE sub.symbol = p.symbol;

