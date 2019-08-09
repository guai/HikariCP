package ru.programpark.hikari.ha

import org.postgresql.ds.PGSimpleDataSource
import ru.programpark.hikari.HikariConfig
import ru.programpark.hikari.HikariDataSource
import java.awt.*
import java.awt.event.ActionEvent
import java.awt.event.ActionListener
import java.lang.management.ManagementFactory
import javax.management.ObjectName
import javax.swing.JFrame
import javax.swing.WindowConstants


/**
 * socat TCP4-LISTEN:65432,fork TCP4:127.0.0.1:5432
 */

fun main() {

	val ds1 = PGSimpleDataSource().apply {
		url = "jdbc:postgresql://localhost:5432/activiti1"
		user = "postgres"
		password = "postgres"
	}

	val ds2 = PGSimpleDataSource().apply {
		url = "jdbc:postgresql://localhost:65432/activiti2"
		user = "postgres"
		password = "postgres"
	}

	val cfg = HikariConfig().apply {
		dataSource = ds1
		dataSource2 = ds2

		poolName = "activitiHikariCP"
		isRegisterMbeans = true
		isAllowPoolSuspension = true
		twinJmxUrl = "localhost:9011"

		maximumPoolSize = 3
		isAutoCommit = false
	}
	val hikari = HikariDataSource(cfg)


	val frame = JFrame().apply {
		defaultCloseOperation = WindowConstants.EXIT_ON_CLOSE
		setBounds(200, 300, 150, 400)
		layout = FlowLayout()

		add(Checkbox("commit/rollback", true))

		put(Button("insert blob 1")) {
			addActionListener(MyActionListener(hikari, "propfile1.properties"))
		}

		put(Button("insert blob 2")) {
			addActionListener(MyActionListener(hikari, "propfile2.properties"))
		}

		put(Button("insert blob 3")) {
			addActionListener(MyActionListener(hikari, "propfile3.properties"))
		}

		put(Button("restore direct")) {
			addActionListener {
				// test stuff. not to be called directly
				val mBeanServer = ManagementFactory.getPlatformMBeanServer()
				val poolName = ObjectName("ru.programpark.hikari:type=Pool (" + hikari.poolName + ")")
				mBeanServer.invoke(poolName, "suspendPool", null, null)
				mBeanServer.invoke(poolName, "restoreDirect", null, null)
				mBeanServer.invoke(poolName, "resumePool", null, null)
			}
		}

		isVisible = true
	}

	hikari.connection.use { con ->
		con.createStatement().use { st ->
			st.executeQuery("SELECT 1 AS id, 'foobar' AS name, current_timestamp AS now;").use { rs ->
				while (rs.next()) {
					print(rs.getLong(1))
					print(" ")
					print(rs.getString(2))
					print(" ")
					println(rs.getTimestamp(3))
				}
				con.commit()
			}
		}
	}

}

class MyActionListener(private val hikari: HikariDataSource, private val resource: String) : ActionListener {

	override fun actionPerformed(e: ActionEvent) {
		hikari.connection.use { con ->
			con.prepareStatement("INSERT INTO table1 VALUES(?)").use { pst ->
				this::class.java.classLoader.getResource(resource)!!.openStream().use { blob ->
					pst.setBinaryStream(1, blob)
					pst.execute()

					if ((e.source as Component).parent.components.findIsInstance<Checkbox> { it.label == "commit/rollback" }!!.state)
						con.commit()
					else
						con.rollback()
				}
			}
		}
	}
}


private inline fun <T : Component> Container.put(c: T, block: T.() -> Unit): T = c.apply(block).also { this.add(it) }

private inline fun <reified R> Array<*>.findIsInstance(predicate: (R)-> Boolean): R? {
	for (item in this)
		if(item is R && predicate(item))
			return item
	return null
}
