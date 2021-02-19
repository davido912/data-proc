DROP TABLE IF EXISTS modelled.fact_events;
CREATE TABLE IF NOT EXISTS modelled.fact_events AS (
    WITH base_table AS (
        SELECT received_at::DATE,
               username,
               user_type,
               organization_name
        FROM raw.raw_events
        GROUP BY 1, 2, 3, 4
    ),
         agg_events_by_date AS (
             SELECT received_at,
                    count(*) AS total_events_by_date
             FROM base_table
             GROUP BY 1
         ),
         agg_events_by_usertype AS (
             SELECT received_at,
                    user_type,
                    COUNT(*) OVER (PARTITION BY received_at::DATE, user_type) AS total_events_by_date_by_usertype
             FROM base_table
         ),
         grouped_agg_events_by_usertype AS (
             SELECT *
             FROM agg_events_by_usertype
             GROUP BY 1, 2, 3
         ),
         agg_events_by_org AS (
             SELECT received_at,
                    organization_name,
                    COUNT(*) OVER (PARTITION BY received_at::DATE, organization_name) AS total_events_by_date_by_org
             FROM base_table
         ),
         grouped_agg_events_by_org AS (
             SELECT *
             FROM agg_events_by_org
             GROUP BY 1, 2, 3
         ),
         agg_events_by_unique_users AS (
             SELECT received_at,
                    count(distinct username) AS total_events_by_date_unique_users
             FROM base_table
             GROUP BY 1
         )

    SELECT t1.received_at,
           t1.organization_name,
           t1.user_type,
           t4.total_events_by_date,
           t5.total_events_by_date_unique_users,
           t2.total_events_by_date_by_org,
           t3.total_events_by_date_by_usertype
    FROM base_table t1
-- COALESCING to get also NULL count per date
             LEFT JOIN grouped_agg_events_by_org t2 ON t1.received_at = t2.received_at AND
                                                       COALESCE(t1.organization_name, 'none') =
                                                       COALESCE(t2.organization_name, 'none')
             LEFT JOIN grouped_agg_events_by_usertype t3 ON t1.received_at = t3.received_at AND
                                                            COALESCE(t1.user_type, 'none') = COALESCE(t3.user_type, 'none')
             LEFT JOIN agg_events_by_date t4 ON t1.received_at = t4.received_at
             LEFT JOIN agg_events_by_unique_users t5 ON t1.received_at = t5.received_at
    ORDER BY received_at
);