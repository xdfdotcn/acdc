package cn.xdf.acdc.devops;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.liquibase.LiquibaseProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Scheduler application.
 */
@SpringBootApplication
@EnableConfigurationProperties({LiquibaseProperties.class})
public class SchedulerApplication {

    /**
     * Scheduler project entry point.
     * @param args string arrays
     */
    public static void main(final String[] args) {
        SpringApplication.run(SchedulerApplication.class, args);
    }

}
