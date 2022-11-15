package cn.xdf.acdc.devops.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableJpaRepositories("cn.xdf.acdc.devops.repository")
@EnableTransactionManagement
public class DatabaseConfiguration {

}
