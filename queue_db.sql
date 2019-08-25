CREATE TABLE query_queue (
    id BIGSERIAL NOT NULL PRIMARY KEY,
    query TEXT NOT NULL,
    status SMALLINT DEFAULT 0,  -- Статус запроса: 0 - новый, 1 - исполняется, 2 - завершен, 3 - ошибка исполнения
    notification TEXT
);

CREATE TABLE query_results (
   id BIGSERIAL NOT NULL PRIMARY KEY,
   query_id BIGINT NOT NULL,
   period varchar NOT NULL,
   owner_inn varchar NOT NULL,
   owner_name varchar,
   type int NOT NULL,
   contractor_inn varchar NOT NULL,
   contractor_name varchar,
   date varchar NOT NULL,
   number varchar NOT NULL
);
