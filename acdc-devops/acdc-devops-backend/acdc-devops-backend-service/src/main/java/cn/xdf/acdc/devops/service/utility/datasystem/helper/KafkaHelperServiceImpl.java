package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PreDestroy;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Service
public class KafkaHelperServiceImpl implements KafkaHelperService {
    
    private static final String USER_PRINCIPAL_PREFIX = "User:";
    
    private static final String PATTERN_ANY = "*";
    
    private static final long ADMIN_CLIENT_TTL_MINUTES = 10;
    
    private final LoadingCache<Map<String, Object>, AdminClient> adminClientCache = CacheBuilder.newBuilder()
            .expireAfterWrite(ADMIN_CLIENT_TTL_MINUTES, TimeUnit.MINUTES)
            .removalListener((RemovalListener<Map<String, Object>, AdminClient>) notification -> {
                log.info("closing kafka admin client with config: {}", notification.getKey());
                notification.getValue().close();
            })
            .build(new CacheLoader<Map<String, Object>, AdminClient>() {
                @Override
                public AdminClient load(final Map<String, Object> config) {
                    log.info("creating kafka admin client with config: {}", config);
                    return AdminClient.create(config);
                }
            });
    
    /**
     * Close admin client.
     */
    @PreDestroy
    public void closeAdminClient() {
        ConcurrentMap<Map<String, Object>, AdminClient> adminClientMap = adminClientCache.asMap();
        Collection<AdminClient> adminClients = adminClientMap.values();
        if (!CollectionUtils.isEmpty(adminClients)) {
            adminClients.forEach(AdminClient::close);
        }
    }
    
    @Override
    public void createTopic(
            final String topicName,
            final int numPartitions,
            final short replicationFactor,
            final Map<String, String> topicConfig,
            final Map<String, Object> adminConfig
    ) {
        NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
        if (topicConfig != null && !topicConfig.isEmpty()) {
            newTopic.configs(topicConfig);
        }
        
        CreateTopicsResult result = getAdminClient(adminConfig).createTopics(Lists.newArrayList(newTopic));
        try {
            result.all().get();
        } catch (ExecutionException | InterruptedException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof TopicExistsException) {
                log.warn("create topic warning: {}", cause.getMessage(), cause);
            } else {
                throw new ServerErrorException(cause);
            }
        }
    }
    
    @Override
    public void deleteTopics(final List<String> topics, final Map<String, Object> adminConfig) {
        DeleteTopicsResult deleteTopicsResult = getAdminClient(adminConfig).deleteTopics(topics);
        try {
            deleteTopicsResult.all().get();
        } catch (ExecutionException | InterruptedException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof UnknownTopicOrPartitionException) {
                log.warn("create topic warning: {}", cause.getMessage(), cause);
            } else {
                throw new ServerErrorException(cause);
            }
        }
    }
    
    @Override
    public void addAcl(final String topic, final String userName, final AclOperation aclOperation, final Map<String, Object> adminConfig) {
        ResourcePattern resourcePattern = new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL);
        AccessControlEntry accessControlEntry = new AccessControlEntry(userPrincipal(userName), PATTERN_ANY, aclOperation, AclPermissionType.ALLOW);
        AclBinding aclBinding = new AclBinding(resourcePattern, accessControlEntry);
        CreateAclsResult aclResult = getAdminClient(adminConfig).createAcls(Lists.newArrayList(aclBinding));
        try {
            aclResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new ServerErrorException(e);
        }
    }
    
    @Override
    public void deleteAcl(final String topic, final String userName, final Map<String, Object> adminConfig) {
        ResourcePatternFilter resourcePatternFilter = new ResourcePatternFilter(ResourceType.TOPIC, topic, PatternType.ANY);
        AccessControlEntryFilter accessControlEntryFilter = new AccessControlEntryFilter(userPrincipal(userName), null, AclOperation.ANY, AclPermissionType.ANY);
        AclBindingFilter filter = new AclBindingFilter(resourcePatternFilter, accessControlEntryFilter);
        DeleteAclsResult deleteAclsResult = getAdminClient(adminConfig).deleteAcls(Lists.newArrayList(filter));
        try {
            deleteAclsResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new ServerErrorException(e);
        }
    }
    
    @Override
    public Set<String> listTopics(final Map<String, Object> adminConfig) {
        Set<String> topics;
        try {
            topics = getAdminClient(adminConfig).listTopics().names().get();
        } catch (KafkaException | InterruptedException | ExecutionException e) {
            throw new ServerErrorException(e);
        }
        return topics.stream().filter(topic -> !topic.startsWith("_")).collect(Collectors.toSet());
    }
    
    @Override
    public void checkAdminClientConfig(final Map<String, Object> adminConfig) {
        this.listTopics(adminConfig);
    }
    
    private String userPrincipal(final String userName) {
        return USER_PRINCIPAL_PREFIX + userName;
    }
    
    private AdminClient getAdminClient(final Map<String, Object> adminConfig) {
        AdminClient adminClient;
        try {
            adminClient = adminClientCache.get(adminConfig);
        } catch (ExecutionException e) {
            throw new ServerErrorException(e);
        }
        return adminClient;
    }
}
