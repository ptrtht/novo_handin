CREATE VIEW v_metadata AS WITH cte_bp AS(
    SELECT
        "start_date",
        "end_date",
        "batch_phase",
        CASE
            WHEN batch_phase = 'preperation' THEN start_date
            WHEN batch_phase = 'cultivation' THEN end_date
        END join_ts
    FROM
        batch_phase
),
cte_bi AS(
    SELECT
        *,
        CASE
            WHEN n = 1 THEN start_date
            WHEN n = 2 THEN end_date
        END join_ts
    FROM
        batch_info
        CROSS JOIN (
            SELECT
                1 AS n
            UNION
            ALL
            SELECT
                2
        ) n
)
SELECT
    batch_id,
    batch_phase,
    date_trunc('minute', cte_bp.start_date) as start_date,
    date_trunc('minute', cte_bp.end_date) as end_date
FROM
    cte_bi
    INNER JOIN cte_bp ON cte_bp.join_ts = cte_bi.join_ts
    WHERE batch_id <> '' ;
