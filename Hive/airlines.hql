CREATE TABLE airlines
(
id int,
name string,
alias string,
iata string,
icao string,
callsign string,
country string,
active string
)
CLUSTERED BY (id)
INTO 2 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' ;