SELECT b.book_ref, b.total_amount, SUM(s.price)
FROM bookings b
JOIN tickets t ON t.book_ref = b.book_ref
JOIN segments s ON s.ticket_no = t.ticket_no
GROUP BY b.book_ref, b.total_amount
HAVING b.total_amount <> SUM(s.price);

SELECT
    s.ticket_no,
    s.flight_id,
    r.route_no,
    r.validity,
    s.price,
    s.fare_conditions,
    (f.scheduled_arrival - f.scheduled_departure) AS flight_interval,
    r.departure_airport,
    r.arrival_airport,
    r.airplane_code,
    r.days_of_week,
    r.scheduled_time,
    r.duration,
    s.price / (EXTRACT(EPOCH FROM (f.scheduled_arrival - f.scheduled_departure)) / 60.0) AS price_per_scheduled_minute
FROM segments s
JOIN flights f ON f.flight_id = s.flight_id
JOIN routes r ON r.route_no = f.route_no
ORDER BY s.ticket_no, s.flight_id;

WITH x AS (
    SELECT
        s.fare_conditions,
        s.price / (EXTRACT(EPOCH FROM (f.scheduled_arrival - f.scheduled_departure)) / 60.0) AS price_per_scheduled_minute
    FROM segments s
    JOIN flights f ON f.flight_id = s.flight_id
)
SELECT
    fare_conditions,
    MIN(price_per_scheduled_minute) AS min_ppm,
    MAX(price_per_scheduled_minute) AS max_ppm
FROM x
GROUP BY fare_conditions;

WITH x AS (
    SELECT
        s.price AS actual_price,
        CASE s.fare_conditions
            WHEN 'Business' THEN (EXTRACT(EPOCH FROM (f.scheduled_arrival - f.scheduled_departure)) / 60.0) * 100
            WHEN 'Comfort'  THEN (EXTRACT(EPOCH FROM (f.scheduled_arrival - f.scheduled_departure)) / 60.0) * 65
            WHEN 'Economy'  THEN (EXTRACT(EPOCH FROM (f.scheduled_arrival - f.scheduled_departure)) / 60.0) * 50
            ELSE NULL
        END AS expected_price
    FROM segments s
    JOIN flights f ON f.flight_id = s.flight_id
)
SELECT
    actual_price,
    expected_price
FROM x
WHERE actual_price <> expected_price;

SELECT
	s.price AS actual_price,
	s.ticket_no,
    s.flight_id,
    EXTRACT(EPOCH FROM (f.scheduled_arrival - f.scheduled_departure)) / 60.0 AS scheduled_minutes,
    s.fare_conditions,
    CASE s.fare_conditions
    	WHEN 'Business' THEN (EXTRACT(EPOCH FROM (f.scheduled_arrival - f.scheduled_departure)) / 60.0) * 100
        WHEN 'Comfort'  THEN (EXTRACT(EPOCH FROM (f.scheduled_arrival - f.scheduled_departure)) / 60.0) * 65
        WHEN 'Economy'  THEN (EXTRACT(EPOCH FROM (f.scheduled_arrival - f.scheduled_departure)) / 60.0) * 50
        ELSE NULL
    END AS expected_price
FROM segments s
JOIN flights f ON f.flight_id = s.flight_id
