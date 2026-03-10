CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE INDEX IF NOT EXISTS routes_validity_gist
ON routes USING GIST (validity);

CREATE INDEX IF NOT EXISTS routes_arrival_airport_validity_gist
ON routes USING GIST (arrival_airport, validity);

CREATE INDEX IF NOT EXISTS routes_departure_airport_validity_gist
ON routes USING GIST (departure_airport, validity);

CREATE INDEX IF NOT EXISTS airports_city_country_en_btree
ON airports_data ((city->>'en'), (country->>'en'));

CREATE INDEX IF NOT EXISTS flights_scheddep_route_scheduled_idx
ON flights (scheduled_departure, route_no)
WHERE status = 'Scheduled';

CREATE INDEX IF NOT EXISTS flights_route_scheddep_scheduled_idx
ON flights (route_no, scheduled_departure)
WHERE status = 'Scheduled';

CREATE INDEX IF NOT EXISTS segments_ticket_no_idx
ON segments (ticket_no);

CREATE INDEX IF NOT EXISTS segments_flight_id_idx
ON segments (flight_id);

CREATE INDEX IF NOT EXISTS boarding_passes_flight_boardingno_desc_idx
ON boarding_passes (flight_id, boarding_no DESC);

CREATE INDEX IF NOT EXISTS boarding_passes_flight_seat_idx
ON boarding_passes (flight_id, seat_no);
