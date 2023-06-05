package cn.xdf.acdc.devops.core.util;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Data
@Component
@Configuration
public class ACDCEnvironment {
    
    public static final String ENV_DEV = "dev";
    
    public static final String ENV_PROD = "prod";
    
    @Value("${spring.profiles.active:uat}")
    private String env;
    
    /**
     * 生产 环境.
     *
     * @return uat
     */
    public boolean isProd() {
        return Objects.equals(ENV_PROD, env);
    }
    
    /**
     * uat,或者dev环境.
     *
     * @return uat
     */
    public boolean isDevOrUat() {
        return Objects.equals(ENV_DEV, env);
    }
}
