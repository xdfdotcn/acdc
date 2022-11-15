package cn.xdf.acdc.devops.service.config;

import cn.xdf.acdc.devops.core.domain.entity.AuthorityDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorClassDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorDataExtensionDO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectorEventDO;
import cn.xdf.acdc.devops.core.domain.entity.DefaultConnectorConfigurationDO;
import cn.xdf.acdc.devops.core.domain.entity.HdfsDO;
import cn.xdf.acdc.devops.core.domain.entity.HdfsNamenodeDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.HiveTableDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.SinkConnectorColumnMappingDO;
import cn.xdf.acdc.devops.core.domain.entity.SourceRdbTableDO;
import cn.xdf.acdc.devops.core.domain.entity.UserDO;
import com.github.benmanes.caffeine.jcache.configuration.CaffeineConfiguration;
import org.hibernate.cache.jcache.ConfigSettings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cache.JCacheManagerCustomizer;
import org.springframework.boot.autoconfigure.orm.jpa.HibernatePropertiesCustomizer;
import org.springframework.boot.info.BuildProperties;
import org.springframework.boot.info.GitProperties;
import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.context.annotation.Bean;
import tech.jhipster.config.JHipsterProperties;
import tech.jhipster.config.cache.PrefixedKeyGenerator;

import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

/**
 * Caffeine cache config.
 * TODO 暂时取消掉二级本地缓存配置
 */
//@Configuration
//@EnableCaching
public class CacheConfiguration {

    private GitProperties gitProperties;

    private BuildProperties buildProperties;

    private final javax.cache.configuration.Configuration<Object, Object> jcacheConfiguration;

    public CacheConfiguration(final JHipsterProperties jHipsterProperties) {
        JHipsterProperties.Cache.Caffeine caffeine = jHipsterProperties.getCache().getCaffeine();

        CaffeineConfiguration<Object, Object> caffeineConfiguration = new CaffeineConfiguration<>();
        caffeineConfiguration.setMaximumSize(OptionalLong.of(caffeine.getMaxEntries()));
        caffeineConfiguration.setExpireAfterWrite(OptionalLong.of(TimeUnit.SECONDS.toNanos(caffeine.getTimeToLiveSeconds())));
        caffeineConfiguration.setStatisticsEnabled(true);
        jcacheConfiguration = caffeineConfiguration;
    }

    /**
     * Cache manager.
     * @param cacheManager  cacheManager
     * @return HibernatePropertiesCustomizer
     */
    @Bean
    public HibernatePropertiesCustomizer hibernatePropertiesCustomizer(final javax.cache.CacheManager cacheManager) {
        return hibernateProperties -> hibernateProperties.put(ConfigSettings.CACHE_MANAGER, cacheManager);
    }

    /**
     * Cache key config.
     * @return JCacheManagerCustomizer
     */
    @Bean
    public JCacheManagerCustomizer cacheManagerCustomizer() {
        return cm -> {
            createCache(cm, cn.xdf.acdc.devops.repository.UserRepository.USERS_BY_LOGIN_CACHE);
            createCache(cm, cn.xdf.acdc.devops.repository.UserRepository.USERS_BY_EMAIL_CACHE);
            createCache(cm, UserDO.class.getName());
            createCache(cm, AuthorityDO.class.getName());
            createCache(cm, UserDO.class.getName() + ".authorities");
            createCache(cm, ProjectDO.class.getName());
            createCache(cm, ProjectDO.class.getName() + ".rdbs");
            createCache(cm, ProjectDO.class.getName() + ".users");
            createCache(cm, RdbDO.class.getName());
            createCache(cm, RdbDO.class.getName() + ".rdbDatabases");
            createCache(cm, RdbDO.class.getName() + ".rdbInstances");
            createCache(cm, RdbDO.class.getName() + ".projects");
            createCache(cm, RdbInstanceDO.class.getName());
            createCache(cm, RdbDatabaseDO.class.getName());
            createCache(cm, RdbDatabaseDO.class.getName() + ".rdbTables");
            createCache(cm, RdbTableDO.class.getName());
            createCache(cm, KafkaClusterDO.class.getName());
            createCache(cm, KafkaClusterDO.class.getName() + ".kafkaTopics");
            createCache(cm, KafkaClusterDO.class.getName() + ".connectors");
            createCache(cm, KafkaTopicDO.class.getName());
            createCache(cm, KafkaTopicDO.class.getName() + ".sourceRdbTables");
            createCache(cm, KafkaTopicDO.class.getName() + ".sinkRdbTables");
            createCache(cm, ConnectorClassDO.class.getName());
            createCache(cm, ConnectorClassDO.class.getName() + ".defaultConnectorConfigurations");
            createCache(cm, DefaultConnectorConfigurationDO.class.getName());
            createCache(cm, ConnectClusterDO.class.getName());
            createCache(cm, ConnectClusterDO.class.getName() + ".connectors");
            createCache(cm, ConnectorEventDO.class.getName());
            createCache(cm, ConnectorDO.class.getName());
            createCache(cm, ConnectorDO.class.getName() + ".connectorConfigurations");
            createCache(cm, ConnectorConfigurationDO.class.getName());
            createCache(cm, SourceRdbTableDO.class.getName());
            createCache(cm, SourceRdbTableDO.class.getName() + ".connectorDataExtensions");
            createCache(cm, ConnectorDataExtensionDO.class.getName());
            createCache(cm, ConnectorDataExtensionDO.class.getName() + ".sourceRdbTables");
            createCache(cm, ConnectorDataExtensionDO.class.getName() + ".sinkRdbTables");
            createCache(cm, SinkConnectorColumnMappingDO.class.getName());
            createCache(cm, SinkConnectorColumnMappingDO.class.getName() + ".sinkRdbTables");
            createCache(cm, ProjectDO.class.getName() + ".hives");
            createCache(cm, ConnectorDataExtensionDO.class.getName() + ".sinkHiveTables");
            createCache(cm, SinkConnectorColumnMappingDO.class.getName() + ".sinkHiveTables");
            createCache(cm, HiveDO.class.getName());
            createCache(cm, HiveDO.class.getName() + ".hiveDatabases");
            createCache(cm, HiveDO.class.getName() + ".projects");
            createCache(cm, HdfsDO.class.getName());
            createCache(cm, HdfsDO.class.getName() + ".hdfsNamenodes");
            createCache(cm, HdfsDO.class.getName() + ".hives");
            createCache(cm, HdfsNamenodeDO.class.getName());
            createCache(cm, HiveDatabaseDO.class.getName());
            createCache(cm, HiveDatabaseDO.class.getName() + ".hiveTables");
            createCache(cm, HiveTableDO.class.getName());
            // jhipster-needle-caffeine-add-entry
        };
    }

    private void createCache(final javax.cache.CacheManager cm, final String cacheName) {
        javax.cache.Cache<Object, Object> cache = cm.getCache(cacheName);
        if (cache != null) {
            cache.clear();
        } else {
            cm.createCache(cacheName, jcacheConfiguration);
        }
    }

    /**
     * Set gitProperties.
     * @param gitProperties  gitProperties
     */
    @Autowired(required = false)
    public void setGitProperties(final GitProperties gitProperties) {
        this.gitProperties = gitProperties;
    }

    /**
     * Set buildProperties.
     * @param buildProperties  buildProperties
     */
    @Autowired(required = false)
    public void setBuildProperties(final BuildProperties buildProperties) {
        this.buildProperties = buildProperties;
    }

    /**
     * Bean keyGenerator.
     * @return KeyGenerator  KeyGenerator
     */
    @Bean
    public KeyGenerator keyGenerator() {
        return new PrefixedKeyGenerator(this.gitProperties, this.buildProperties);
    }
}
