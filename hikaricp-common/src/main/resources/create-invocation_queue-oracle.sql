CREATE TABLE invocation_queue
(
  id            INTEGER PRIMARY KEY,
  connection_id INTEGER     NOT NULL,
  statement_id  INTEGER     NOT NULL,
  class         INTEGER     NOT NULL,
  method_name   VARCHAR(23) NOT NULL,
  args          BLOB
);

CREATE SEQUENCE invocation_queue_id_seq START WITH 1 INCREMENT BY 1;

CREATE TRIGGER invocation_queue_set_id
BEFORE INSERT ON invocation_queue FOR EACH ROW
  BEGIN
    SELECT invocation_queue_id_seq.nextval
    INTO :new.id
    FROM dual;
  END;

CREATE PROCEDURE reset_invocation_queue_id_seq
IS
  val NUMBER;
  BEGIN
    EXECUTE IMMEDIATE
    'SELECT invocation_queue_id_seq.nextval FROM dual'
    INTO val;
    EXECUTE IMMEDIATE
    'ALTER SEQUENCE invocation_queue_id_seq INCREMENT BY -' || val || ' MINVALUE 1';
    EXECUTE IMMEDIATE
    'SELECT invocation_queue_id_seq.nextval FROM dual'
    INTO val;
    EXECUTE IMMEDIATE
    'ALTER SEQUENCE invocation_queue_id_seq INCREMENT BY 1 MINVALUE 1';
  END;
