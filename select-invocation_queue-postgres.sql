SELECT
		id,
		connection_id,
		statement_id,
		CASE
			WHEN class = -1325073376 THEN 'CallableStatement'
			WHEN class = 1373428040  THEN 'Connection'
			WHEN class = -105951463  THEN 'PreparedStatement'
			WHEN class = -1820536797 THEN 'Statement'
		END AS class_name,
		method_name,
		args,
		length(args)
	FROM invocation_queue;
