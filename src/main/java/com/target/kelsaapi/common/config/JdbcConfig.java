package com.target.kelsaapi.common.config;

import com.target.platform.connector.config.ConfigSource;
import com.target.platform.connector.config.ConfigurationException;
import com.target.platform.connector.config.FileSource;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
@ConfigurationProperties(prefix = "jdbcconfig")
@Slf4j
@Data
public class JdbcConfig implements InitializingBean {

    private String url;
    private String username;
    private String password;

    /**
     * Invoked by the containing {@code BeanFactory} after it has set all bean properties
     * and satisfied {@link BeanFactoryAware}, {@code ApplicationContextAware} etc.
     * <p>This method allows the bean instance to perform validation of its overall
     * configuration and final initialization when all bean properties have been set.
     *
     */
    @Override
    public void afterPropertiesSet() throws ConfigurationException {
        if (password==null||password.isEmpty()) {
            log.info("No password set in properties file, will attempt to retrieve from TAP secrets");
            try {
                FileSource source = ConfigSource.file("jdbc.password");
                if (source!=null) {
                    setPassword(source.getString());
                } else {
                    throw new ConfigurationException("Missing TAP secret for jdbc.password");
                }
            } catch (Exception e) {
                throw new ConfigurationException(e.getMessage());
            }
        }

    }

    @Bean
    public DataSource dataSource() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setUsername(getUsername());
        dataSource.setPassword(getPassword());
        dataSource.setJdbcUrl(getUrl());

        return dataSource;
    }
}
