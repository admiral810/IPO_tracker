CREATE TABLE stocks
(
	id SERIAL NOT NULL PRIMARY KEY,
	symbol VARCHAR(10) UNIQUE,
	company VARCHAR(255) NOT NULL,
	exchange VARCHAR(255),
	proposed_share_price VARCHAR(255) NOT NULL,
	shares_offered INT,
	priced_date DATE NOT NULL,
	dollar_val_shares DECIMAL(13, 2) NOT NULL,
	deal_status VARCHAR(10)
);

SELECT * FROM stocks;