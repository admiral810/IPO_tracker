-- get the first date with performance information
-- join of a subquery

SELECT sub.symbol, sub.first_trade_day, p.open, p.close, p.high, p.low, p.volume
FROM
	(
	SELECT symbol, MIN(date) as first_trade_day
	FROM performance 
	GROUP BY symbol
	) sub

INNER JOIN performance p 
	ON p.symbol = sub.symbol AND p.date = sub.first_trade_day

ORDER BY sub.symbol;


-- get the performance after one week
-- account for days market isn't open (holiday and weekends)



	








