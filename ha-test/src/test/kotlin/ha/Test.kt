package ha

import ru.programpark.hikari.HikariConfig
import ru.programpark.hikari.HikariDataSource
import ru.programpark.hikari.util.DriverDataSource
import net.miginfocom.swing.MigLayout
import java.awt.Component
import java.awt.Container
import java.awt.event.ActionListener
import java.io.File
import java.lang.management.ManagementFactory
import java.util.*
import javax.management.ObjectName
import javax.swing.*


object Test {
    private var proxy12: PortProxy? = PortProxy("127.0.0.1", 5432, 15432).apply { start() }
    private var proxy21: PortProxy? = PortProxy("127.0.0.1", 5432, 25432).apply { start() }

    private val ds1direct = DriverDataSource(
            "jdbc:postgresql://localhost:5432/activiti1",
            "org.postgresql.Driver",
            Properties(),
            "postgres",
            "postgres"
    )
    private val ds2direct = DriverDataSource(
            "jdbc:postgresql://localhost:5432/activiti2",
            "org.postgresql.Driver",
            Properties(),
            "postgres",
            "postgres"
    )
    private val ds1proxy = DriverDataSource(
            "jdbc:postgresql://localhost:15432/activiti1",
            "org.postgresql.Driver",
            Properties(),
            "postgres",
            "postgres"
    )
    private val ds2proxy = DriverDataSource(
            "jdbc:postgresql://localhost:25432/activiti2",
            "org.postgresql.Driver",
            Properties(),
            "postgres",
            "postgres"
    )
    private var twin12: HikariDataSource? = null
    private var twin21: HikariDataSource? = null

    @JvmStatic
    fun main(args: Array<String>) {

        val commitRollbackActionListener = { dsp: () -> HikariDataSource?, resource: String, checkbox: JCheckBox ->
            ActionListener {
                val ds = dsp()?.takeIf { it.isRunning } ?: return@ActionListener
                ds.connection.use { con ->
                    con.prepareStatement("INSERT INTO table1 VALUES(?)").use { pst ->
                        File("src/test/resources/$resource").inputStream().use { blob ->
                            pst.setBinaryStream(1, blob)
                            pst.execute()
                            if (checkbox.isSelected) con.commit() else con.rollback()
                        }
                    }
                }
            }
        }

        val autoCommitActionListener = { dsp: () -> HikariDataSource?, resource: String ->
            ActionListener {
                val ds = dsp()?.takeIf { it.isRunning } ?: return@ActionListener
                ds.connection.use { con ->
                    con.autoCommit = true
                    con.prepareStatement("INSERT INTO table1 VALUES(?)").use { pst ->
                        File("src/test/resources/$resource").inputStream().use { blob ->
                            pst.setBinaryStream(1, blob)
                            pst.execute()
                        }
                    }
                }
            }
        }

        val selectActionListener = { dsp: () -> HikariDataSource? ->
            ActionListener {
                val ds = dsp()?.takeIf { it.isRunning } ?: return@ActionListener
                ds.connection.use { con ->
                    con.createStatement().use { st ->
                        st.executeQuery("SELECT 1 AS id, 'foobar' AS name, current_timestamp AS now;").use { rs ->
                            while (rs.next()) {
                                val id = rs.getLong(1)
                                val name = rs.getString(2)
                                val now = rs.getTimestamp(3)
                                println("$id $name $now")
                            }
                            con.commit()
                        }
                    }
                }
            }
        }

/*        val restoreDirectActionListener = { dsp: () -> HikariDataSource? ->
            ActionListener {
                val ds = dsp()?.takeIf { it.isRunning } ?: return@ActionListener
                ds.hikariPoolMXBean.restoreDirect()
//                val mBeanServer = ManagementFactory.getPlatformMBeanServer()
//                val poolName = ObjectName("com.zaxxer.hikari:type=Pool (" + ds.poolName + ")")
//                mBeanServer.invoke(poolName, "suspendPool", null, null)
//                mBeanServer.invoke(poolName, "restoreDirect", null, null)
//                mBeanServer.invoke(poolName, "resumePool", null, null)
            }
        }*/


        JFrame().apply {
            setBounds(1400, 550, 450, 400)
            defaultCloseOperation = WindowConstants.EXIT_ON_CLOSE
            layout = MigLayout("fill, wrap 2")

//            put(Panel(MigLayout("fill")), "dock north") {
            put(JButton("drop tables")) {
                addActionListener {
                    sequenceOf(ds1direct, ds2direct).forEach {
                        it.connection.use { con ->
                            con.createStatement().use { st ->
                                st.execute("DROP TABLE IF EXISTS table1")
                                st.execute("DROP TABLE IF EXISTS invocation_queue")
                            }
                        }
                    }
                }
            }

            put(JButton("make tables")) {
                addActionListener {
                    sequenceOf(ds1direct, ds2direct).forEach {
                        it.connection.use { con ->
                            con.createStatement().use { st ->
                                st.execute("CREATE TABLE IF NOT EXISTS table1 (blob bytea)")
                            }
                        }
                    }
                }
            }
//            }
            put(JSeparator(), "spanx, growx")

            fun unregisterPool(poolName: String) {
                val mBeanServer = ManagementFactory.getPlatformMBeanServer()
                val beanConfigName = ObjectName("com.zaxxer.hikari:type=PoolConfig ($poolName)")
                val beanPoolName = ObjectName("com.zaxxer.hikari:type=Pool ($poolName)")
                if (mBeanServer.isRegistered(beanConfigName)) {
                    mBeanServer.unregisterMBean(beanConfigName)
                    mBeanServer.unregisterMBean(beanPoolName)
                }
            }

            put(JButton("make dataSource 12")) {
                addActionListener {
                    twin12?.apply {
                        unregisterPool(poolName)
                        close()
                    }
                    twin12 = HikariDataSource(HikariConfig().apply {
                        dataSource = ds1proxy
                        twinDataSource = ds2proxy

                        poolName = "pool_12"
                        twinPoolName = "pool_21"
                        twinJmxUrl = "localhost:9010"

                        isRegisterMbeans = true
                        isAllowPoolSuspension = true

                        maximumPoolSize = 3
                        isAutoCommit = false

                        connectionTimeout = 3000
                    })
                }
            }

            put(JButton("make dataSource 21")) {
                addActionListener {
                    twin21?.apply {
                        unregisterPool(poolName)
                        close()
                    }
                    twin21 = HikariDataSource(HikariConfig().apply {
                        dataSource = ds2proxy
                        twinDataSource = ds1proxy

                        poolName = "pool_21"
                        twinPoolName = "pool_12"
                        twinJmxUrl = "localhost:9010"

                        isRegisterMbeans = true
                        isAllowPoolSuspension = true

                        maximumPoolSize = 3
                        isAutoCommit = false

                        connectionTimeout = 3000
                    })
                }
            }

            put(JButton("close dataSource 12")) {
                flat()
                addActionListener {
                    twin12?.apply {
                        unregisterPool(poolName)
                        close()
                    }
                    twin12 = null
                }
            }

            put(JButton("close dataSource 21")) {
                flat()
                addActionListener {
                    twin21?.apply {
                        unregisterPool(poolName)
                        close()
                    }
                    twin21 = null
                }
            }

            val checkbox12 = put(JCheckBox("commit/rollback", true), "spanx, alignx 50%")

            put(JButton("commit/rollback blob to 12")) {
                addActionListener(commitRollbackActionListener(::twin12, "propfile1.properties", checkbox12))
            }

            put(JButton("commit/rollback blob to 21")) {
                addActionListener(commitRollbackActionListener(::twin21, "propfile1.properties", checkbox12))
            }

            put(JButton("autocommit blob to 12")) {
                addActionListener(autoCommitActionListener(::twin12, "propfile2.properties"))
            }

            put(JButton("autocommit blob to 21")) {
                addActionListener(autoCommitActionListener(::twin21, "propfile2.properties"))
            }

            put(JButton("select 12")) {
                addActionListener(selectActionListener(::twin12))
            }

            put(JButton("select 21")) {
                addActionListener(selectActionListener(::twin21))
            }

            put(JSeparator(), "spanx, growx")

            put(JButton("synchronize twins 12")) {
                flat()
                addActionListener {
                    twin12?.hikariPoolMXBean?.synchronizeTwins()
                }
            }

            put(JButton("synchronize twins 21")) {
                flat()
                addActionListener {
                    twin21?.hikariPoolMXBean?.synchronizeTwins()
                }
            }

            put(JPanel(MigLayout("fill", "[20%|60%|20%]")), "spanx, growx") {
                val ac12 = put(JLabel(""), "alignx right")
                val ac = put(JButton("show active connections"), "alignx center")
                val ac21 = put(JLabel(""), "alignx left")
                ac.addActionListener {
                    ac12.text = twin12?.hikariPoolMXBean?.activeConnections?.toString() ?: ""
                    ac21.text = twin21?.hikariPoolMXBean?.activeConnections?.toString() ?: ""
                }
            }

            put(JSeparator(), "spanx, growx")

            put(JButton("close proxy 12")) {
                flat()
                addActionListener {
                    proxy12?.close()
                    proxy12 = null
                }
            }

            put(JButton("close proxy 21")) {
                flat()
                addActionListener {
                    proxy21?.close()
                    proxy21 = null
                }
            }

            put(JButton("make proxy 12")) {
                addActionListener {
                    proxy12?.close()
                    proxy12 = PortProxy("127.0.0.1", 5432, 15432).apply { start() }
                }
            }
            put(JButton("make proxy 21")) {
                addActionListener {
                    proxy21?.close()
                    proxy21 = PortProxy("127.0.0.1", 5432, 25432).apply { start() }
                }
            }

            isVisible = true
        }
    }

    private inline fun <T : Component> Container.put(c: T, constraints: Any? = null, block: T.() -> Unit = {}): T = c.apply(block).also { this.add(it, constraints) }
    private inline fun <T : Component, R : Any> Container.letsPut(c: T, constraints: Any? = null, block: T.() -> R): R = with(c.also { this.add(it, constraints) }, block)

    inline fun <T : AutoCloseable> T.use(block: (T) -> Unit) = try {
        block.invoke(this)
    } finally {
        this.close()
    }

    private fun JButton.flat() {
        isFocusPainted = false
        isFocusPainted = false
        isContentAreaFilled = false
    }
}
