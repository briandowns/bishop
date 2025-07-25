CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    command TEXT NOT NULL,
    run_at INTEGER NOT NULL,
    interval INTEGER DEFAULT 0,
    priority INTEGER DEFAULT 0,
    enabled INTEGER DEFAULT 1,
    retries INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    next_run INTEGER,
    backoff INTEGER DEFAULT 1,
    fail_count INTEGER DEFAULT 0
);