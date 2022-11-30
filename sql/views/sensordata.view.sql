CREATE VIEW v_sensors AS -- PH sensors --
WITH cte_ph1 AS (
    SELECT
        date_trunc('minute', "timestamp") AS timestamp,
        "value" AS ph1
    FROM
        ph_sensors
    WHERE
        sensor_name = '400E_PH1'
),
cte_ph2 AS (
    SELECT
        date_trunc('minute', "timestamp") AS timestamp,
        "value" AS ph2
    FROM
        ph_sensors
    WHERE
        sensor_name = '400E_PH2'
),
cte_phs AS (
    SELECT
        cte_ph1.timestamp,
        ph1,
        ph2
    FROM
        cte_ph1 FULL
        JOIN cte_ph2 ON cte_ph1.timestamp = cte_ph2.timestamp
),
-- Temp sensors --
cte_temp1 AS (
    SELECT
        date_trunc('minute', "timestamp") AS timestamp,
        "value" AS temp1
    FROM
        temp_sensors
    WHERE
        sensor_name = '400E_Temp1'
),
cte_temp2 AS (
    SELECT
        date_trunc('minute', "timestamp") AS timestamp,
        "value" AS temp2
    FROM
        temp_sensors
    WHERE
        sensor_name = '400E_Temp2'
),
cte_temps AS (
    SELECT
        cte_temp1.timestamp,
        temp1,
        temp2
    FROM
        cte_temp1 FULL
        JOIN cte_temp2 ON cte_temp1.timestamp = cte_temp2.timestamp
) -- Combined -- 
SELECT
    cte_phs.timestamp,
    ph1,
    ph2,
    temp1,
    temp2
FROM
    cte_temps FULL
    JOIN cte_phs ON cte_temps.timestamp = cte_phs.timestamp;