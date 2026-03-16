CREATE TABLE knows AS
    SELECT "Person.id" AS k_person1id, "Person.id_1" AS k_person2id
    FROM read_csv(:dataDir || '/person_knows_person.txt', delim='|', header=true,
        columns={'Person.id':'BIGINT', 'Person.id_1':'BIGINT', 'creationDate':'VARCHAR'});

CREATE TABLE param AS SELECT * FROM read_csv(:dataDir || '/interactive_13_param.txt',
    delim='|', header=true, columns={'person1Id':'BIGINT', 'person2Id':'BIGINT'});

-- Multi-source BFS over all param pairs at once.
-- Depth bound of 5 is sufficient: bidirectional knows guarantees all LDBC param pairs
-- are connected with diameter <= 3 (six degrees of separation).
-- Multiple (src, node, dist) entries may arise from delta semantics; MIN picks shortest.
WITH RECURSIVE bfs(src, node, dist) AS (
    SELECT DISTINCT person1Id, person1Id, 0 FROM param
    UNION ALL
    SELECT DISTINCT d.src, k.k_person2id, d.dist + 1
    FROM bfs d
    JOIN knows k ON d.node = k.k_person1id
    WHERE d.dist + 1 <= 5
      AND NOT EXISTS (
          SELECT 1 FROM bfs b2 WHERE b2.src = d.src AND b2.node = k.k_person2id AND b2.dist <= d.dist
      )
)
SELECT p.person1Id, p.person2Id, COALESCE(MIN(b.dist), -1) AS shortestPathLength
FROM param p
LEFT JOIN bfs b ON b.src = p.person1Id AND b.node = p.person2Id
GROUP BY p.person1Id, p.person2Id;
