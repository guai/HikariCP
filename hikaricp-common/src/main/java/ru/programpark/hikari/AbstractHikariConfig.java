/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.programpark.hikari;

import ru.programpark.hikari.util.PropertyBeanSetter;
import ru.programpark.hikari.util.UtilityElf;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@Setter
@Getter
public abstract class AbstractHikariConfig implements HikariConfigMBean
{

   private static final Logger LOGGER = LoggerFactory.getLogger(HikariConfig.class);

   private static final long CONNECTION_TIMEOUT = TimeUnit.SECONDS.toMillis(30);
   private static final long VALIDATION_TIMEOUT = TimeUnit.SECONDS.toMillis(5);
   private static final long IDLE_TIMEOUT = TimeUnit.MINUTES.toMillis(10);
   private static final long MAX_LIFETIME = TimeUnit.MINUTES.toMillis(30);

   private static int poolNumber;
   private static boolean unitTest;

   // Properties changeable at runtime through the MBean
   //
   private volatile long connectionTimeout;
   private volatile long validationTimeout;
   private volatile long idleTimeout;
   private volatile long leakDetectionThreshold;
   private volatile long maxLifetime;
   private volatile int maximumPoolSize;
   private volatile int minimumIdle;

   // Properties NOT changeable at runtime
   //
   private String catalog;
   private String connectionCustomizerClassName;
   private String connectionInitSql;
   private String connectionTestQuery;
   private String dataSourceClassName;
   private String dataSourceJndiName;
   private String driverClassName;
   private String jdbcUrl;
   private String password;
   private String poolName;
   private String transactionIsolation;
   private String username;
   private boolean isAutoCommit;
   private boolean isReadOnly;
   private boolean isInitializationFailFast;
   private boolean isIsolateInternalQueries;
   private boolean isRegisterMbeans;
   private boolean isAllowPoolSuspension;
   private DataSource dataSource;
   private Properties dataSourceProperties;

   private DataSource dataSource2;
   private String twinJmxUrl;

   private IConnectionCustomizer customizer;
   private ThreadFactory threadFactory;
   private Object metricRegistry;
   private Object healthCheckRegistry;
   private Properties healthCheckProperties;

   /**
    * Default constructor
    */
   public AbstractHikariConfig()
   {
      dataSourceProperties = new Properties();
      healthCheckProperties = new Properties();

      connectionTimeout = CONNECTION_TIMEOUT;
      validationTimeout = VALIDATION_TIMEOUT;
      idleTimeout = IDLE_TIMEOUT;
      isAutoCommit = true;
      isInitializationFailFast = true;
      minimumIdle = -1;
      maximumPoolSize = 10;
      maxLifetime = MAX_LIFETIME;
      customizer = new IConnectionCustomizer() {
         @Override
         public void customize(Connection connection) throws SQLException
         {
         }
      };

      String systemProp = System.getProperty("hikaricp.configurationFile");
      if ( systemProp != null) {
         loadProperties(systemProp);
      }
   }

   /**
    * Construct a HikariConfig from the specified properties object.
    *
    * @param properties the name of the property file
    */
   public AbstractHikariConfig(Properties properties)
   {
      this();
      PropertyBeanSetter.setTargetFromProperties(this, properties);
   }

   /**
    * Construct a HikariConfig from the specified property file name.  <code>propertyFileName</code>
    * will first be treated as a path in the file-system, and if that fails the 
    * ClassLoader.getResourceAsStream(propertyFileName) will be tried.
    *
    * @param propertyFileName the name of the property file
    */
   public AbstractHikariConfig(String propertyFileName)
   {
      this();

      loadProperties(propertyFileName);
   }

   /**
    * Get the customizer instance specified by the user.
    *
    * @return an instance of IConnectionCustomizer
    */
   @Deprecated
   public IConnectionCustomizer getConnectionCustomizer()
   {
      return customizer;
   }

   /**
    * Set the connection customizer to be used by the pool.
    *
    * @param customizer an instance of IConnectionCustomizer
    */
   @Deprecated
   public void setConnectionCustomizer(IConnectionCustomizer customizer)
   {
      this.customizer = customizer;
      LOGGER.warn("The connectionCustomizer property has been deprecated and may be removed in a future release");
   }

   public void addDataSourceProperty(String propertyName, Object value)
   {
      dataSourceProperties.put(propertyName, value);
   }

   public String getDataSourceJNDI()
   {
      return this.dataSourceJndiName;
   }

   public void setDataSourceJNDI(String jndiDataSource)
   {
      this.dataSourceJndiName = jndiDataSource;
   }

   /**
    * Get the default auto-commit behavior of connections in the pool.
    *
    * @return the default auto-commit behavior of connections
    */
   public boolean isAutoCommit()
   {
      return isAutoCommit;
   }

   /**
    * Set the default auto-commit behavior of connections in the pool.
    *
    * @param isAutoCommit the desired auto-commit default for connections
    */
   public void setAutoCommit(boolean isAutoCommit)
   {
      this.isAutoCommit = isAutoCommit;
   }

   /**
    * Get the pool suspension behavior (allowed or disallowed).
    *
    * @return the pool suspension behavior
    */
   public boolean isAllowPoolSuspension()
   {
      return isAllowPoolSuspension;
   }

   /**
    * Set whether or not pool suspension is allowed.  There is a performance
    * impact when pool suspension is enabled.  Unless you need it (for a
    * redundancy system for example) do not enable it.
    *
    * @param isAllowPoolSuspension the desired pool suspension allowance
    */
   public void setAllowPoolSuspension(boolean isAllowPoolSuspension)
   {
      this.isAllowPoolSuspension = isAllowPoolSuspension;
   }

   /**
    * Get whether or not the construction of the pool should throw an exception
    * if the minimum number of connections cannot be created.
    *
    * @return whether or not initialization should fail on error immediately
    */
   public boolean isInitializationFailFast()
   {
      return isInitializationFailFast;
   }

   /**
    * Set whether or not the construction of the pool should throw an exception
    * if the minimum number of connections cannot be created.
    *
    * @param failFast true if the pool should fail if the minimum connections cannot be created
    */
   public void setInitializationFailFast(boolean failFast)
   {
      isInitializationFailFast = failFast;
   }

   public boolean isIsolateInternalQueries()
   {
      return isIsolateInternalQueries;
   }

   public void setIsolateInternalQueries(boolean isolate)
   {
      this.isIsolateInternalQueries = isolate;
   }

   @Deprecated
   public boolean isJdbc4ConnectionTest()
   {
      return false;
   }

   @Deprecated
   public void setJdbc4ConnectionTest(boolean useIsValid)
   {
      // ignored deprecated property
      LOGGER.warn("The jdbcConnectionTest property is now deprecated, see the documentation for connectionTestQuery");
   }

   public void addHealthCheckProperty(String key, String value)
   {
      healthCheckProperties.setProperty(key, value);
   }

   public boolean isReadOnly()
   {
      return isReadOnly;
   }

   public void setReadOnly(boolean readOnly)
   {
      this.isReadOnly = readOnly;
   }

   public boolean isRegisterMbeans()
   {
      return isRegisterMbeans;
   }

   public void setRegisterMbeans(boolean register)
   {
      this.isRegisterMbeans = register;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void setMaximumPoolSize(int maxPoolSize)
   {
      if (maxPoolSize < 1) {
         throw new IllegalArgumentException("maximumPoolSize cannot be less than 1");
      }
      this.maximumPoolSize = maxPoolSize;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void setMinimumIdle(int minIdle)
   {
      if (minIdle < 0) {
         throw new IllegalArgumentException("minimumIdle cannot be negative");
      }
      this.minimumIdle = minIdle;
   }

   public void validate()
   {
      Logger logger = LoggerFactory.getLogger(getClass());

      validateNumerics();

      if (connectionCustomizerClassName != null) {
         try {
            getClass().getClassLoader().loadClass(connectionCustomizerClassName);
         }
         catch (Exception e) {
            logger.warn("connectionCustomizationClass specified class '" + connectionCustomizerClassName + "' could not be loaded", e);
            connectionCustomizerClassName = null;
         }
      }

      if (driverClassName != null && jdbcUrl == null) {
         logger.error("when specifying driverClassName, jdbcUrl must also be specified");
         throw new IllegalStateException("when specifying driverClassName, jdbcUrl must also be specified");
      }
      else if (driverClassName != null && dataSourceClassName != null) {
         logger.error("both driverClassName and dataSourceClassName are specified, one or the other should be used");
         throw new IllegalStateException("both driverClassName and dataSourceClassName are specified, one or the other should be used");
      }
      else if (jdbcUrl != null) {
         // OK
      }
      else if (dataSource == null && dataSourceClassName == null) {
         logger.error("one of either dataSource, dataSourceClassName, or jdbcUrl and driverClassName must be specified");
         throw new IllegalArgumentException("one of either dataSource or dataSourceClassName must be specified");
      }
      else if (dataSource != null && dataSourceClassName != null) {
         logger.warn("both dataSource and dataSourceClassName are specified, ignoring dataSourceClassName");
      }

      if (transactionIsolation != null) {
         UtilityElf.getTransactionIsolation(transactionIsolation);
      }

      if (poolName == null) {
         poolName = "HikariPool-" + poolNumber++;
      }

      if (LOGGER.isDebugEnabled() || unitTest) {
         logConfiguration();
      }
   }

   private void validateNumerics()
   {
      Logger logger = LoggerFactory.getLogger(getClass());

      if (validationTimeout > connectionTimeout && connectionTimeout != 0) {
         logger.warn("validationTimeout is greater than connectionTimeout, setting validationTimeout to connectionTimeout.");
         validationTimeout = connectionTimeout;
      }

      if (minimumIdle < 0 || minimumIdle > maximumPoolSize) {
         minimumIdle = maximumPoolSize;
      }

      if (maxLifetime < 0) {
         logger.error("maxLifetime cannot be negative.");
         throw new IllegalArgumentException("maxLifetime cannot be negative.");
      }
      else if (maxLifetime > 0 && maxLifetime < TimeUnit.SECONDS.toMillis(30)) {
         logger.warn("maxLifetime is less than 30000ms, using default {}ms.", MAX_LIFETIME);
         maxLifetime = MAX_LIFETIME;
      }

      if (idleTimeout != 0 && idleTimeout < TimeUnit.SECONDS.toMillis(10)) {
         logger.warn("idleTimeout is less than 10000ms, using default {}ms.", IDLE_TIMEOUT);
         idleTimeout = IDLE_TIMEOUT;
      }
      else if (idleTimeout > maxLifetime && maxLifetime > 0) {
         logger.warn("idleTimeout is greater than maxLifetime, setting to maxLifetime.");
         idleTimeout = maxLifetime;
      }

      if (leakDetectionThreshold != 0 && leakDetectionThreshold < TimeUnit.SECONDS.toMillis(2) && !unitTest) {
         logger.warn("leakDetectionThreshold is less than 2000ms, setting to minimum 2000ms.");
         leakDetectionThreshold = 2000L;
      }
   }

   private void logConfiguration()
   {
      LOGGER.debug("HikariCP pool {} configuration:", poolName);
      final Set<String> propertyNames = new TreeSet<String>(PropertyBeanSetter.getPropertyNames(HikariConfig.class));
      for (String prop : propertyNames) {
         try {
            Object value = PropertyBeanSetter.getProperty(prop, this);
            if ("dataSourceProperties".equals(prop)) {
               Properties dsProps = PropertyBeanSetter.copyProperties(dataSourceProperties);
               dsProps.setProperty("password", "<masked>");
               value = dsProps;
            }
            value = (prop.contains("password") ? "<masked>" : value);
            LOGGER.debug((prop + "................................................").substring(0, 32) + (value != null ? value : ""));
         }
         catch (Exception e) {
            continue;
         }
      }
   }

   abstract protected void loadProperties(String propertyFileName);

   public void copyState(AbstractHikariConfig other)
   {
      for (Field field : AbstractHikariConfig.class.getDeclaredFields()) {
         if (!Modifier.isFinal(field.getModifiers())) {
            field.setAccessible(true);
            try {
               field.set(other, field.get(this));
            }
            catch (Exception e) {
               throw new RuntimeException("Exception copying HikariConfig state: " + e.getMessage(), e);
            }
         }
      }
   }
}
