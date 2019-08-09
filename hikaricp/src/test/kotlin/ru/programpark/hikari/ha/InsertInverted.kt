package ru.programpark.hikari.ha

import org.postgresql.ds.PGSimpleDataSource
import ru.programpark.hikari.HikariConfig
import ru.programpark.hikari.HikariDataSource
import java.awt.Component
import java.awt.Container


fun main() {

	val ds1 = PGSimpleDataSource().apply {
		url = "jdbc:postgresql://localhost:5432/activiti2"
		user = "postgres"
		password = "postgres"
	}

	val nonexistent = PGSimpleDataSource().apply {
		url = "jdbc:postgresql://localhost:5432/nonexistent"
		user = "postgres"
		password = "postgres"
	}

	val cfg = HikariConfig().apply {
		dataSource = ds1
		dataSource2 = nonexistent // fallback

		poolName = "activitiHikariCP"
		isRegisterMbeans = true
		isAllowPoolSuspension = true
		twinJmxUrl = "localhost:9010"

		maximumPoolSize = 3
		isAutoCommit = false
	}
	val hikari = HikariDataSource(cfg)

	hikari.connection.use { con ->
		con.prepareStatement("INSERT INTO table1 VALUES(?)").use { pst ->
			Thread.currentThread().contextClassLoader.getResource("hibernate.properties")!!.openStream().use { blob ->
				pst.setBinaryStream(1, blob)
				pst.execute()

				con.commit()
			}
		}
	}
}
