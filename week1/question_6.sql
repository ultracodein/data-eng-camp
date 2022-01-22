SELECT
	z1."Zone", z2."Zone"
FROM
	yellow_taxi_trips t
	LEFT JOIN zones z1 ON t."PULocationID" = z1."LocationID"
	LEFT JOIN zones z2 on t."DOLocationID" = z2."LocationID"
GROUP BY
	z1."Zone", z2."Zone"
ORDER BY
	AVG(total_amount) DESC
LIMIT 1