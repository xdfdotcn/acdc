package cn.xdf.acdc.devops.service.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.persistence.Transient;

/**
 * 关系型数据 jdbc 账户配置.
 */
@Component
@ConfigurationProperties(prefix = "acdc.rdb.jdbc")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RdbJdbcConfig {

    private String user;

    @JsonIgnore
    @Transient
    private String password;
}
