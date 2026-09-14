CREATE TABLE Source(score REAL, enabled INTEGER, right TEXT, id INTEGER, left INTEGER, extra TEXT);
INSERT INTO Source VALUES (2.5, 1, 'other', 2, 8, 'ignored');
INSERT INTO Source VALUES (1.5, 0, 'quoted '' value', 1, 7, 'ignored');
ATTACH 'output/output.sqlite' AS output;
CREATE TABLE output.Untouched(x INTEGER);
INSERT INTO output.Untouched VALUES (42);
