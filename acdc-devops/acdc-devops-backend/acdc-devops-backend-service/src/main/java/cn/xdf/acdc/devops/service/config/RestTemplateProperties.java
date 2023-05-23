package cn.xdf.acdc.devops.service.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

/**
 * Copy from  scheduler.
 */
@Configuration
public class RestTemplateProperties {
    
    /**
     * Get rest template bean.
     *
     * @return rest template
     */
    @Bean
    public RestTemplate registerTemplate() {
        RestTemplate restTemplate = new RestTemplate(getFactory());
        return restTemplate;
    }
    
    /**
     * Config client http request factory.
     *
     * @return simple client http request factory
     */
    @Bean
    @ConfigurationProperties(prefix = "acdc.rest.connection")
    public SimpleClientHttpRequestFactory getFactory() {
        return new SimpleClientHttpRequestFactory();
    }
}
