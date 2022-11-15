package cn.xdf.acdc.devops.config;

import cn.xdf.acdc.devops.core.aop.logging.LoggingAspect;
import cn.xdf.acdc.devops.core.util.ACDCEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;

@Configuration
@EnableAspectJAutoProxy
public class LoggingAspectConfig {

    /**
     * Logging  aspect config.
     *
     * @param env env
     * @return LoggingAspect
     */
    @Bean
    @Profile(ACDCEnvironment.ENV_DEV)
    public LoggingAspect loggingAspect(final Environment env) {
        return new LoggingAspect(env);
    }
}
