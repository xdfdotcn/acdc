package cn.xdf.acdc.devops.statemachine;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableTransactionManagement(proxyTargetClass = true)
@ComponentScan({"cn.xdf.acdc.devops.statemachine", "cn.xdf.acdc.devops.service.aop"})
public class StatemachineTestConfig {
}
