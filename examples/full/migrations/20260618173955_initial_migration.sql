-- Add migration script here
CREATE TABLE tests (
    id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    test_description NVARCHAR(MAX),
    test_date DATETIMEOFFSET NOT NULL
)