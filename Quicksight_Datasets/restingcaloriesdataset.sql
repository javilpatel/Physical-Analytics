SELECT date_parse(SUBSTRING(creationDate, 1, 10), '%Y-%m-%d') as Date, SUM(value) as energy_burned
/* Needed to add database before table because of quicksight*/
FROM health_database.health_metrics
WHERE type = 'HKQuantityTypeIdentifierBasalEnergyBurned'
GROUP BY date_parse(SUBSTRING(creationDate, 1, 10), '%Y-%m-%d')