create table if not exists fetch_result (
    created_at timestamp default current_timestamp,

    url text,
    status int,
    body text
);

create table if not exists queue (
    id integer primary key autoincrement,
    created_at timestamp default current_timestamp,

    name text,
    payload text, -- but actually should be json object
    started_at timestamp,
    pid int unique -- ensure only 1 task per process
);
