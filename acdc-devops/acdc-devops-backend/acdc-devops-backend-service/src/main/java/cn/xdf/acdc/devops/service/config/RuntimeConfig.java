package cn.xdf.acdc.devops.service.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
@ConfigurationProperties(prefix = "acdc.runtime")
@Getter
@Setter
public class RuntimeConfig {

    private final Host host = new Host();

    @Getter
    @Setter
    public static class Host {
        private Set<String> ranges;

        private Set<String> ips;
    }
}
