SELECT date_parse(SUBSTRING(creationDate, 1, 10), '%Y-%m-%d') as Date, SUM(value) as Steps
/* Needed to add database before table because of quicksight*/
FROM health_database.health_metrics
WHERE type = 'HKQuantityTypeIdentifierStepCount'
GROUP BY date_parse(SUBSTRING(creationDate, 1, 10), '%Y-%m-%d')