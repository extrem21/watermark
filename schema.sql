-- Applied identically to both source-db and downstream-db.
-- Stage 1: a single table is enough to prove happy-path replication.
CREATE TABLE IF NOT EXISTS accounts (
    id      INTEGER PRIMARY KEY,
    owner   TEXT NOT NULL,
    balance INTEGER NOT NULL
);
