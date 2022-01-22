SELECT
	CAST(tpep_pickup_datetime AS DATE) AS day
FROM
	yellow_taxi_trips t
GROUP BY
	day
ORDER BY
	MAX(tip_amount) DESC
LIMIT 1