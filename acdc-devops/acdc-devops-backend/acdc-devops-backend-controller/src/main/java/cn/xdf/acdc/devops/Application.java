package cn.xdf.acdc.devops;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.liquibase.LiquibaseProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Controller application.
 */
@SpringBootApplication
@EnableConfigurationProperties(LiquibaseProperties.class)
public class Application {
    
    /**
     * Controller project entry point.
     *
     * @param args string arrays
     */
    public static void main(final String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
