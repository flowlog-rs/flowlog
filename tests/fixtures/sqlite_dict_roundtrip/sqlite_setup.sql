CREATE TABLE __SymbolTable (id INTEGER PRIMARY KEY, symbol TEXT UNIQUE);
INSERT INTO __SymbolTable VALUES (1, 'b'), (2, 'a'), (3, 'c');
CREATE TABLE _Source ("0" INTEGER NOT NULL, "1" INTEGER NOT NULL);
INSERT INTO _Source VALUES (1, 10), (2, 20), (3, 30);
CREATE VIEW Source AS SELECT "_symtab_0".symbol AS name, _Source."1" AS n
  FROM _Source, __SymbolTable AS "_symtab_0" WHERE _Source."0" = "_symtab_0".id;
