CREATE TABLE knows AS
    SELECT "Person.id" AS k_person1id, "Person.id_1" AS k_person2id
    FROM read_csv(:dataDir || '/person_knows_person.txt', delim='|', header=true,
        columns={'Person.id':'BIGINT', 'Person.id_1':'BIGINT', 'creationDate':'VARCHAR'});

CREATE TABLE param AS SELECT * FROM read_csv(:dataDir || '/interactive_13_param.txt',
    delim='|', header=true, columns={'person1Id':'BIGINT', 'person2Id':'BIGINT'});

-- The checker runs one (person1Id, person2Id) pair per invocation, so keep this
-- close to the scalar-parameter benchmark query and only adapt the parameter plumbing.
WITH RECURSIVE
    input(person1Id, person2Id) AS (
        SELECT person1Id, person2Id
        FROM param
        LIMIT 1
    ),
    search_graph(link, level, path) AS (
            SELECT person1Id, 0, [person1Id]
            FROM input
        UNION ALL
            (WITH sg(link, level) as (select * from search_graph) -- Note: sg is only the diff produced in the previous iteration
            SELECT DISTINCT k_person2id, x.level + 1, array_append(path, k_person2id)
            FROM knows, sg x
            WHERE 1=1
            and x.link = k_person1id
            -- stop if we have reached person2 in the previous iteration
            and not exists(select * from sg y where y.link = (SELECT person2Id FROM input))
            -- skip reaching persons reached in the previous iteration
            and not exists(select * from sg y where y.link = k_person2id)
          )
)
select max(level) AS shortestPathLength from (
select level from search_graph where link = (SELECT person2Id FROM input)
union select -1) tmp;
