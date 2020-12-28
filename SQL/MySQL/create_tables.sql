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

