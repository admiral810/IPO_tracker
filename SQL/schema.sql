-- Create IPO table

CREATE TABLE ipo (
id SERIAL,
symbol VARCHAR(30) NOT NULL,
company VARCHAR(256),
offer_date DATE,
date_status VARCHAR(30),
market_cap_offered INTEGER,
first_day_close DECIMAL,

PRIMARY KEY (id)
);


-- Create performance table

CREATE TABLE customer_email (
    id SERIAL,
    stock_id INTEGER NOT NULL,
    symbol  VARCHAR(30) NOT NULL,
    price DECIMAL, 
    market_cap INTEGER,
    date DATE,
    date_milestone VARCHAR(80),

    PRIMARY KEY (id),
    FOREIGN KEY (stock_id) REFERENCES ipo(id)
);
