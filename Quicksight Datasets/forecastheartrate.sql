SELECT date_parse(SUBSTRING(creationDate, 1, 19), '%Y-%m-%d %H:%i:%s') as parsed_date, AVG(value) as average_heart_rate
/* Needed to add database before table because of quicksight*/
FROM health_database.health_metrics
WHERE type = 'HKQuantityTypeIdentifierHeartRate'
GROUP BY date_parse(SUBSTRING(creationDate, 1, 19), '%Y-%m-%d %H:%i:%s')
ORDER BY date_parse(SUBSTRING(creationDate, 1, 19), '%Y-%m-%d %H:%i:%s')