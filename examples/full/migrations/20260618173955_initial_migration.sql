-- Add migration script here
CREATE TABLE users (
    id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    description NVARCHAR(MAX),
    add_date DATETIMEOFFSET NOT NULL
)