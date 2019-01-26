airlines = LOAD 'datasets/airlines' USING PigStorage(',') AS (id:int, name:chararray, alias:chararray, iata:chararray, icao:chararray, callsign:chararray, country:chararray, active:chararray);
active_airlines_us = FILTER airlines BY UPPER(TRIM(country)) == 'UNITED STATES' AND UPPER(TRIM(active)) == 'Y';
airline_names = FOREACH active_airlines_us GENERATE id, name;
STORE airline_names INTO '/user/edureka_409125/assignments/output/active_airlines_in_us' USING PigStorage (',');