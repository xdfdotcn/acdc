package cn.xdf.acdc.devops.config;

import io.micrometer.core.aop.TimedAspect;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MicrometerAutoConfiguration {
    
    /**
     * Config timed aspect.
     *
     * @param registry meter registry
     * @return timed aspect
     */
    @Bean
    public TimedAspect timedAspect(final MeterRegistry registry) {
        return new TimedAspect(registry);
    }
}
