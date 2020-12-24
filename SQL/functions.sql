-- =======================================
-- get date n days after given date
-- =======================================
CREATE FUNCTION fn_PerformanceOverTime
	(
		initial_date DATE,
		days_between INT,
		OUT answer DATE		
	)

AS $$
BEGIN
	answer := initial_date + days_between;
END; $$
LANGUAGE plpgsql;

-- example
SELECT symbol, date, fn_performanceovertime(date, 7)
FROM performance;

-- ==================================================================================
-- get date n days after given date, adjust date if market not open (take day after)
-- ==================================================================================
CREATE FUNCTION fn_date_n_days_out
	(
		initial_date DATE,
		days_between INT,
		OUT answer DATE		
	)
RETURNS DATE
LANGUAGE plpgsql
DECLARE
	new_date_out DATE;

AS $$
BEGIN
	SELECT symbol
	FROM performance
	WHERE date = initial_date + days_between;
END; $$


-- get adjusted 7-day date if market not open (take day after)
SELECT fd.*, 
	fn_performanceovertime(first_trade_day, 7) AS seven_day_date,
	p.open,
	p.close,
	p.high,
	p.low

FROM vw_first_day_perf fd
INNER JOIN 
	performance p ON p.symbol = fd.symbol AND p.date = fd.first_trade_day
;
