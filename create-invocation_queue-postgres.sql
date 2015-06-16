CREATE TABLE invocation_queue (
	id serial PRIMARY KEY,
	connection_id integer NOT NULL,
	statement_id integer NOT NULL,
	class integer NOT NULL,
	method_name character varying(23) NOT NULL,
	args bytea NOT NULL
);
