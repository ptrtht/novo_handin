CREATE VIEW v_metawithsensor AS
SELECT
    *,
    -- TODO: make this only one subquery
    (
        SELECT
            v_metadata.batch_phase AS batch_phase
        FROM
            v_metadata
        WHERE
            v_metadata.start_date < v_sensors.timestamp
            AND v_sensors.timestamp < v_metadata.end_date
    ),
    (
        SELECT
            v_metadata.batch_id AS batch_id
        FROM
            v_metadata
        WHERE
            v_metadata.start_date < v_sensors.timestamp
            AND v_sensors.timestamp < v_metadata.end_date
    )
FROM
    v_sensors;