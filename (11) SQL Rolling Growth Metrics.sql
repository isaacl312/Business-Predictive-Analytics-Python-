Rolling 1M growth metrics in SQL
(1) DAU : unique active user 1M
(2) first month activation rate (new/DAU)
(3) Retention rate mom    (MAU1m/ MAU2m-1m)
(4) Reactivation rate mom (MAU1m/ not active 2m-1m)

3TBL: (1 calendar) (2 daily activity) (3 Users)        
      (cal_dt)      (userid, acti_dt) (userid, reg_dt)  

WITH DAU_list AS (                         #(날짜 - id들)
    SELECT DISTINCT cal_dt AS active_dt,
          user_id
    FROM calendar 
    LEFT JOIN daily_activity 
    ON   acti_dt BETWEEN cal_dt - 27 AND cal_dt
)  

SELECT cal_dt
    ,COUNT(CASE WHEN curr.user_id IS NOT NULL THEN 1 END) AS active_users
    ,COUNT(CASE WHEN cal_dt <= reg_dt + 27 AND curr.user_id IS NOT NULL THEN 1 END) /
          NULLIF(COUNT(CASE WHEN cal_dt <= reg_dt + 27 THEN 1 END), 0)::FLOAT AS activation_rate
    ,COUNT(CASE WHEN prev.user_id IS NOT NULL AND curr.user_id IS NOT NULL THEN 1 END) / 
          NULLIF(COUNT(CASE WHEN prev.user_id IS NOT NULL THEN 1 END), 0)::FLOAT AS retention_rate
    ,COUNT(CASE WHEN prev.user_id IS NULL AND cal_dt - 28 >= reg_dt AND curr.user_id IS NOT NULL
           THEN 1 END) / NULLIF(COUNT(CASE WHEN prev.user_id IS NULL AND cal_dt - 28 >= reg_dt THEN 1 END), 0)::FLOAT AS reactivation_rate
FROM calendar JOIN users ON cal_dt >= reg_dt        #(날짜- reg_dt(그전가입자들), id들)  #(날짜 - act_dt(T27D(그전 27일간 활동인)), id들)
LEFT JOIN DAU_list AS prev 
ON   users.user_id = prev.user_id AND cal_dt - 28 = prev.active_dt  
LEFT JOIN DAU_list AS curr
ON   users.user_id = prev.user_id AND cal_dt = curr.active_dt 
GROUP BY 1
ORDER BY 1
),

JOIN (날짜비교)
(1) (날짜 - 그전27활동인들)
    (calendar날짜 - act_dt(T27D), id들) (1:n) 
    FROM calendar 
    LEFT JOIN daily_activity 
    ON   acti_dt BETWEEN cal_dt - 27 AND cal_dt 

(2) (날짜 - 그전가입자들)
    (caldendar날짜 - reg_dt(작은, id들) (1:n) 
    FROM calendar JOIN users 
    ON   cal_dt >= reg_dt 

(1) Define events. For each (user_id, activity_dt) in the daily_activity table, we’ll create a pair of “events” corresponding to the date when the activity occurs (and hence should first be counted as part of a 28-day rolling window), and 28 days later when the activity expires (after which the activity no longer counts toward any subsequent 28-day rolling window). We’ll mark these two events with scores +1 and -1 (to be used in the next step). We’ll also create an event with score 0 on the user’s registration date.
(2)Compute rolling states. By computing the cumulative sum of the scores for each event date, we’ll know the number of days the user was active over the trailing 28-day period. Checking if this sum was greater than zero indicates whether the user was 28-day active as of each event date.
(3)Deduplicate states. For parsimony, we’ll remove redundant events where the user’s 28-day activity status has not changed.
(4)Convert to intervals. Finally, we’ll convert the remaining events into activity intervals.

(빠른버전: “date intervals” over which each user is 28-day active)
SELECT cal_dt
    ,COUNT(CASE
        WHEN curr.user_id IS NOT NULL
        THEN 1 END) AS active_users
    ,COUNT(CASE
        WHEN cal_dt <= registration_dt + 27
            AND curr.user_id IS NOT NULL
        THEN 1 END) /
    NULLIF(COUNT(CASE
        WHEN cal_dt <= registration_dt + 27
        THEN 1 END), 0)::FLOAT AS activation_rate
    ,COUNT(CASE
        WHEN prev.user_id IS NOT NULL
            AND curr.user_id IS NOT NULL
        THEN 1 END) / 
    NULLIF(COUNT(CASE
        WHEN prev.user_id IS NOT NULL
        THEN 1 END), 0)::FLOAT AS retention_rate
    ,COUNT(CASE
        WHEN prev.user_id IS NULL
            AND cal_dt - 28 >= registration_dt
            AND curr.user_id IS NOT NULL
        THEN 1 END) / 
    NULLIF(COUNT(CASE
        WHEN prev.user_id IS NULL
            AND cal_dt - 28 >= registration_dt
        THEN 1 END), 0)::FLOAT AS reactivation_rate
FROM calendar
JOIN users
    ON cal_dt >= users.registration_dt
LEFT JOIN trailing_activity_intervals AS prev
    ON cal_dt - 28 BETWEEN prev.start_dt AND prev.end_dt
    AND users.user_id = prev.user_id
LEFT JOIN trailing_activity_intervals AS curr
    ON cal_dt BETWEEN curr.start_dt AND curr.end_dt
    AND users.user_id = curr.user_id
GROUP BY 1
ORDER BY 1;

CREATE TABLE trailing_activity_intervals AS (
    WITH events AS (
        SELECT user_id
            ,activity_dt AS event_dt
            ,1 AS days_active_delta
        FROM daily_activity
        UNION ALL
        SELECT user_id
            ,activity_dt + 28 AS event_dt
            ,-1 AS days_active_delta
        FROM daily_activity
        UNION ALL
        SELECT user_id
            ,registration_dt AS event_dt
            ,0 AS days_active_delta
        FROM users
    ),
    rolling_states AS (
        SELECT user_id
            ,event_dt
            ,SUM(days_active_delta) OVER (
                PARTITION BY user_id
                ORDER BY event_dt ASC
                ROWS UNBOUNDED PRECEDING
            ) > 0 AS active
        FROM (
            SELECT user_id
                ,event_dt
                ,SUM(days_active_delta) AS days_active_delta
            FROM events
            GROUP BY 1, 2
        )
    ),
    deduplicated_states AS (
        SELECT user_id
            ,event_dt
            ,active
        FROM (
            SELECT user_id
                ,event_dt
                ,active
                ,COALESCE(
                    active = LAG(active, 1) OVER (
                        PARTITION BY user_id
                        ORDER BY event_dt ASC
                    ),
                    FALSE
                ) AS redundant
            FROM rolling_states
        )
        WHERE NOT redundant
    )
    SELECT user_id
        ,start_dt
        ,end_dt
    FROM (
        SELECT user_id
            ,event_dt AS start_dt
            ,COALESCE(
                LEAD(event_dt, 1) OVER (
                    PARTITION BY user_id
                    ORDER BY event_dt ASC
                ),
                CURRENT_DATE
            ) - 1 AS end_dt
            ,active
        FROM deduplicated_states
    )
    WHERE active
);