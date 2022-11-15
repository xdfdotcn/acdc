package cn.xdf.acdc.devops.service.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Hive jdbc 账户配置.
 */
@Data
@Component
@ConfigurationProperties(prefix = "acdc.hive.jdbc")
public class HiveJdbcConfig {

    private String url;

    private String user;

    private String password;

}
