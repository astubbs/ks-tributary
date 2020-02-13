CREATE STREAM goalevents (goal_id string, serviceUserId string, score integer, initialScore integer) WITH (kafka_topic='goal-events', value_format='JSON');

CREATE STREAM user_goals as SELECT serviceUserId + goal_id as user_goal_id, * FROM goalevents;

CREATE STREAM progressed_goals AS SELECT * FROM user_goals WHERE (score - initialScore) > 0;
CREATE STREAM SIGNIFICANT_PROGRESSION_EVENTS AS SELECT * FROM user_goals WHERE (score - initialScore) >= 3;

CREATE TABLE progressed_goals_count AS SELECT COUNT(*) as progression_events_count FROM progressed_goals GROUP BY goal_id;
CREATE TABLE significant_progressed_goals_count AS SELECT COUNT(*) as significant_progression_events_count FROM significant_progression_events GROUP BY goal_id;

CREATE TABLE progressed_goals_count_per_user AS SELECT COUNT(*) as progression_goals_count FROM progressed_goals GROUP BY user_goal_id;
CREATE TABLE significant_progressed_goals_count_per_user AS SELECT COUNT(*) as significant_progressed_goals_count FROM significant_progression_events GROUP BY user_goal_id;
