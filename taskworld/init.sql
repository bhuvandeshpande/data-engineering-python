CREATE TABLE IF NOT EXISTS user_activity (
    user_id VARCHAR PRIMARY KEY,
    top_workspace VARCHAR,
    longest_streak INT
);
