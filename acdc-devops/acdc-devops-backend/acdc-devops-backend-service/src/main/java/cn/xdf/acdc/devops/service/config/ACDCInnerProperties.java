package cn.xdf.acdc.devops.service.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * A cdc inner config.
 */
@Component
@ConfigurationProperties(prefix = "acdc.inner")
@Data
public class ACDCInnerProperties {
    
    private String userId;
    
    private String userDomainAccount;
    
    private long projectId;
}
