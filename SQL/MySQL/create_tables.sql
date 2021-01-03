-- ==========================================================
-- CREATE TABLES
-- ==========================================================
USE ipo_tracker;
CREATE TABLE stocks
(
	symbol VARCHAR(10) PRIMARY KEY,
	company VARCHAR(255) NOT NULL,
	exchange VARCHAR(255),
	proposed_share_price VARCHAR(255) NOT NULL,
	shares_offered INT,
	priced_date DATE NOT NULL,
	dollar_val_shares DECIMAL(13, 2) NOT NULL,
	deal_status VARCHAR(10)
);

CREATE TABLE company_info(
	id SERIAL PRIMARY KEY,
	symbol VARCHAR(10),
	address VARCHAR(255),
	city VARCHAR(255),
	state VARCHAR(255),
	zip_code VARCHAR(255),
	country VARCHAR(255),
	website VARCHAR(255),
	industry VARCHAR(255),
	sector VARCHAR(255),
	business_summary VARCHAR(1000),
	date_pulled DATE,
FOREIGN KEY (symbol) REFERENCES stocks(symbol) ON DELETE CASCADE 
);

CREATE TABLE performance(
	id SERIAL PRIMARY KEY,
	symbol VARCHAR(10),
	unix_time INT,
	date DATE,
	open DECIMAL(6,2),
	close DECIMAL(6,2),
	high DECIMAL(6,2),
	low DECIMAL(6,2),
	volume INT,
	date_pulled DATE,
	days_trading INT,
FOREIGN KEY (symbol) REFERENCES stocks(symbol) ON DELETE CASCADE 
);

DROP TABLE market_cap;
CREATE TABLE market_cap(
	id SERIAL PRIMARY KEY,
	symbol VARCHAR(10),
	unix_timestamp INT,
	date DATE,
	market_cap BIGINT,
	market_cap_formatted VARCHAR(25),
	date_pulled DATE,
FOREIGN KEY (symbol) REFERENCES stocks(symbol) ON DELETE CASCADE 
);

SELECT * FROM market_cap;


-- ==================================================================================
-- generated column for proposed market cap
-- ==================================================================================

ALTER TABLE stocks
ADD COLUMN cap_classifiction VARCHAR(255) GENERATED ALWAYS AS
	(
		CASE
			WHEN dollar_val_shares >= 200000000000 THEN 'Mega-cap'
            WHEN dollar_val_shares >= 10000000000 AND dollar_val_shares < 200000000000 THEN 'Large-cap'
            WHEN dollar_val_shares >= 2000000000 AND dollar_val_shares < 10000000000 THEN 'Mid-cap'
            WHEN dollar_val_shares >= 300000000 AND dollar_val_shares < 2000000000 THEN 'Small-cap'
            WHEN dollar_val_shares >= 50000000 AND dollar_val_shares <  300000000 THEN 'Micro-cap'
            WHEN dollar_val_shares > 0 AND dollar_val_shares < 50000000 THEN 'Nano-cap'
            ELSE null 
		END
	);
    
ALTER TABLE stocks
RENAME COLUMN cap_classifiction TO proposed_cap_classifiction;