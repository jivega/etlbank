CREATE EXTERNAL TABLE IF NOT EXISTS balance (
    age INT,
    job STRING,
    marital STRING,
    education STRING,
    defaultB STRING,
    balance DOUBLE,
    housing STRING,
    loan STRING,
    contact STRING,
    day INT,
    month INT,
    duration INT,
    campaign INT,
    pdays INT,
    previous INT,
    poutcome STRING,
    deposit STRING)
STORED AS PARQUET
LOCATION '/output';


