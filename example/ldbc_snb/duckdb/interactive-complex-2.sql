CREATE TABLE param AS SELECT * FROM read_csv(:dataDir || '/interactive_2_param.txt', delim='|', header=true,
    columns={'personId':'BIGINT', 'maxDate':'VARCHAR'});

CREATE TABLE person_raw AS SELECT * FROM read_csv(:dataDir || '/person.txt', delim='|', header=true,
    columns={'id':'BIGINT','firstName':'VARCHAR','lastName':'VARCHAR','gender':'VARCHAR','birthday':'VARCHAR','creationDate':'VARCHAR','locationIP':'VARCHAR','browserUsed':'VARCHAR'});

CREATE TABLE comment_raw AS SELECT * FROM read_csv(:dataDir || '/comment.txt', delim='|', header=true,
    columns={'id':'BIGINT','creationDate':'VARCHAR','locationIP':'VARCHAR','browserUsed':'VARCHAR','content':'VARCHAR','length':'BIGINT'});

CREATE TABLE post_raw AS SELECT * FROM read_csv(:dataDir || '/post.txt', delim='|', header=true,
    columns={'id':'BIGINT','imageFile':'VARCHAR','creationDate':'VARCHAR','locationIP':'VARCHAR','browserUsed':'VARCHAR','language':'VARCHAR','content':'VARCHAR','length':'BIGINT'});

CREATE TABLE comment_hc AS SELECT * FROM read_csv(:dataDir || '/comment_hasCreator_person.txt', delim='|', header=true,
    columns={'Comment.id':'BIGINT','Person.id':'BIGINT'});

CREATE TABLE post_hc AS SELECT * FROM read_csv(:dataDir || '/post_hasCreator_person.txt', delim='|', header=true,
    columns={'Post.id':'BIGINT','Person.id':'BIGINT'});

CREATE TABLE knows_raw AS SELECT * FROM read_csv(:dataDir || '/person_knows_person.txt', delim='|', header=true,
    columns={'Person.id':'BIGINT','Person.id_1':'BIGINT','creationDate':'VARCHAR'});

SELECT p.id, p.firstName, p.lastName,
       msg.m_messageid,
       COALESCE(NULLIF(msg.m_ps_imagefile, ''), msg.m_content) AS content,
       msg.m_creationdate
FROM param
JOIN knows_raw k ON k."Person.id" = param.personId
JOIN person_raw p ON p.id = k."Person.id_1"
JOIN (
    SELECT c.id AS m_messageid, '' AS m_ps_imagefile, c.content AS m_content,
           c.creationDate AS m_creationdate, ch."Person.id" AS m_creatorid
    FROM comment_raw c JOIN comment_hc ch ON c.id = ch."Comment.id"
    UNION ALL
    SELECT p2.id, p2.imageFile, p2.content, p2.creationDate, ph."Person.id"
    FROM post_raw p2 JOIN post_hc ph ON p2.id = ph."Post.id"
) msg ON p.id = msg.m_creatorid
WHERE msg.m_creationdate < param.maxDate;
