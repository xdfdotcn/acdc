package cn.xdf.acdc.devops.service.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConfigurationProperties(prefix = "acdc.mail")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ACDCEmailProperties {

    private String fromEmailAddress;

    private String baseUrl;

    private String ccEmailAddress;

    private List<String> dbaEmailAddress;
}
