routes = LOAD 'datasets/airline_routes' USING PigStorage(',') AS (code:chararray, id:int, scode:chararray, sid:int, dcode:chararray, did:int, codeshare:chararray, stops:int, equipment: chararray);

zero_stop_routes = FILTER routes by stops == 0;

zero_stop_airlines = GROUP zero_stop_routes BY id;

airlines = LOAD 'datasets/airlines' USING PigStorage(',') AS (id:int, name:chararray, alias:chararray, iata:chararray, icao:chararray, callsign:chararray, country:chararray, active:chararray);

joined_data = JOIN zero_stop_airlines by group, airlines by id;

airline_with_zero_stops = FOREACH joined_data GENERATE airlines::id, airlines::name;

STORE airline_with_zero_stops INTO '/user/edureka_409125/assignments/output/airlines_with_zero_stops' USING PigStorage (',');