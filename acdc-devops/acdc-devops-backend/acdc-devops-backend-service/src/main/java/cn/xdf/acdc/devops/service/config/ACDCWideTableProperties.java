package cn.xdf.acdc.devops.service.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * A cdc wide table config.
 */
@Component
@ConfigurationProperties(prefix = "acdc.wide-table")
@Data
public class ACDCWideTableProperties {
    
    private long starrocksClusterId;
    
    private String databaseName;
}
