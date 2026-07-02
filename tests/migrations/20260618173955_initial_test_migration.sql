-- Integration test migration: create test_items table
CREATE TABLE test_items (
    id INTEGER NOT NULL PRIMARY KEY,
    name NVARCHAR(256) NOT NULL,
    value NVARCHAR(MAX),
    created_at DATETIME2 NOT NULL DEFAULT GETUTCDATE()
)
