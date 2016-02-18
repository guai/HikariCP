CREATE TABLE invocation_queue
(
  id            SERIAL PRIMARY KEY,
  connection_id INTEGER               NOT NULL,
  statement_id  INTEGER               NOT NULL,
  class         INTEGER               NOT NULL,
  method_name   CHARACTER VARYING(23) NOT NULL,
  args          BYTEA
);
