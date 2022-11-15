package cn.xdf.acdc.devops.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
@EnableScheduling
public class ScheduleAutoConfiguration {

    /**
     * Config task scheduler.
     * @return task scheduler
     */
    @Bean
    @ConfigurationProperties(prefix = "scheduler.config")
    public TaskScheduler taskScheduler() {
        return new ThreadPoolTaskScheduler();
    }

}
