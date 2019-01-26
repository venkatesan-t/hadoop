CREATE TABLE airline_routes
(
code string,
id int,
scode string,
sid int,
dcode string,
did int,
codeshare string,
stops int,
equipment string
)
CLUSTERED BY (id)
INTO 2 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' ;