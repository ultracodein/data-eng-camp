SELECT
	z."Zone"
FROM
	yellow_taxi_trips t
	LEFT JOIN zones z ON t."DOLocationID" = z."LocationID"
WHERE
	CAST(tpep_pickup_datetime AS DATE) = '2021-01-14'
GROUP BY
	z."Zone"
ORDER BY
	COUNT(1) DESC
LIMIT 1