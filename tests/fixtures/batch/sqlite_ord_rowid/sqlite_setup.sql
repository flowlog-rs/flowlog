CREATE TABLE Source(value TEXT COLLATE NOCASE);
INSERT INTO Source VALUES ('a'), ('b'), ('A');
CREATE INDEX by_value ON Source(value);
