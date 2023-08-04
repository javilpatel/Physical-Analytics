SELECT date_parse(SUBSTRING(creationDate, 1, 10), '%Y-%m-%d') as Date, AVG(value) as Heart_Rate
/* Needed to add database before table because of quicksight*/
FROM health_database.health_metrics
WHERE type = 'HKQuantityTypeIdentifierHeartRate'
GROUP BY date_parse(SUBSTRING(creationDate, 1, 10), '%Y-%m-%d')
ORDER BY date_parse(SUBSTRING(creationDate, 1, 10), '%Y-%m-%d');