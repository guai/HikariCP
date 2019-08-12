CREATE TABLE invocation_queue
(
  id            SERIAL PRIMARY KEY,
  connection_id INTEGER               NOT NULL,
  statement_id  INTEGER               NOT NULL,
  class         CHAR                  NOT NULL,
  method        CHARACTER VARYING(84) NOT NULL,
  args          BYTEA
);
