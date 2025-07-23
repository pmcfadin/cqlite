CREATE TABLE example.users (
    user_id uuid PRIMARY KEY,
    username text,
    email text,
    created_at timestamp,
    profile_data map<text, text>
);