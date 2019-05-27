package cci.config;

import org.hibernate.jpa.HibernatePersistenceProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;


import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;
import java.util.Properties;

@Configuration
@EnableTransactionManagement
@ComponentScan(basePackages={"cci.model.cert", "cci.app"})
@EnableJpaRepositories(basePackages={"cci.repository"})

public class AppConfig {

    private static final String PROPERTY_NAME_DATABASE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static final String PROPERTY_NAME_DATABASE_URL = "jdbc:oracle:thin:@//192.168.0.179:1521/orclpdb";
    private static final String PROPERTY_NAME_DATABASE_USERNAME = "beltpp";
    private static final String PROPERTY_NAME_DATABASE_PASSWORD = "123456";

    private static final String PROPERTY_NAME_HIBERNATE_DIALECT = "org.hibernate.dialect.Oracle12cDialect";
    private static final String PROPERTY_NAME_HIBERNATE_SHOW_SQL = "false";
    private static final String PROPERTY_NAME_ENTITYMANAGER_PACKAGES_TO_SCAN = "cci.model.cert";
    private static final String PROPERTY_NAME_HIBERNATE_HBM2DDL_AUTO = "validate";

    @Bean
    public EntityManagerFactory entityManagerFactory() {
      HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
      
      vendorAdapter.setGenerateDdl(true);
      LocalContainerEntityManagerFactoryBean factory = new LocalContainerEntityManagerFactoryBean();
      factory.setJpaProperties(hibernateProp());
      factory.setJpaVendorAdapter(vendorAdapter);
      factory.setPackagesToScan(PROPERTY_NAME_ENTITYMANAGER_PACKAGES_TO_SCAN);
      factory.setDataSource(dataSource());
      factory.afterPropertiesSet();
      System.out.println("Factory = " + factory.toString());
      return factory.getObject();
    }
    
    @Bean
    public JpaTransactionManager transactionManager() {
        JpaTransactionManager transactionManager = 
        		new JpaTransactionManager();
        transactionManager.setEntityManagerFactory(entityManagerFactory());
        System.out.println("TX = " + transactionManager.toString());
        return transactionManager;
    }

    @Bean
    public DataSource dataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();

        dataSource.setDriverClassName(PROPERTY_NAME_DATABASE_DRIVER);
        dataSource.setUrl(PROPERTY_NAME_DATABASE_URL);
        dataSource.setUsername(PROPERTY_NAME_DATABASE_USERNAME);
        dataSource.setPassword(PROPERTY_NAME_DATABASE_PASSWORD);
        System.out.println("DS = " + dataSource.toString());

        return dataSource;
    }

    private Properties hibernateProp() {
        Properties properties = new Properties();
        properties.put("hibernate.dialect",	PROPERTY_NAME_HIBERNATE_DIALECT);
        properties.put("hibernate.show_sql", PROPERTY_NAME_HIBERNATE_SHOW_SQL);
        properties.put("hibernate.hbm2ddl.auto", PROPERTY_NAME_HIBERNATE_HBM2DDL_AUTO);
        // hibernate.generate_statistics
        // hibernate.use_sql_comments
        // properties.put("hibernate.transaction.jta.platform", "org.hibernate.engine.transaction.jta.platform.internal.NoJtaPlatform");
        return properties;
    }

}