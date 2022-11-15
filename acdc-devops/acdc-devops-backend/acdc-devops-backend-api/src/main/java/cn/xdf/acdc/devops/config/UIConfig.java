package cn.xdf.acdc.devops.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "acdc.ui")
@Configuration
@Data
public class UIConfig {

    private Map<String, String> config = new HashMap<>();
}
