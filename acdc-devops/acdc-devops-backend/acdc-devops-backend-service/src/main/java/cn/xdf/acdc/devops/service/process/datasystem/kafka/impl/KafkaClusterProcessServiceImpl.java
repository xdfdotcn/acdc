package cn.xdf.acdc.devops.service.process.datasystem.kafka.impl;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.domain.dto.KafkaClusterDTO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.ProjectDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import cn.xdf.acdc.devops.core.domain.query.KafkaClusterQuery;
import cn.xdf.acdc.devops.core.domain.query.PagedQuery;
import cn.xdf.acdc.devops.repository.KafkaClusterRepository;
import cn.xdf.acdc.devops.repository.ProjectRepository;
import cn.xdf.acdc.devops.service.entity.ConnectorService;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.common.AsyncInvokeService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.KafkaClusterProcessService;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.KafkaHelperService;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.Client;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.Kafka;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.persistence.EntityExistsException;
import javax.persistence.EntityNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class KafkaClusterProcessServiceImpl implements KafkaClusterProcessService {

    private static final String SECURITY_PROTOCOL_SASL_PLAINTEXT = "SASL_PLAINTEXT";

    private static final String SASL_MECHANISM_PLAIN = "PLAIN";

    private static final String SASL_JAAS_CONFIG_PATTERN = "%s required username=\"%s\" password=\"%s\";";

    private static final String SCRAM_LOGIN_MODULE_CLASS = "org.apache.kafka.common.security.scram.ScramLoginModule";

    private static final String PLAIN_LOGIN_MODULE_CLASS = "org.apache.kafka.common.security.plain.PlainLoginModule";

    @Autowired
    private KafkaClusterRepository kafkaClusterRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DataSystemMetadataService<KafkaClusterDO> dataSystemMetadataService;

    @Autowired
    private AsyncInvokeService asyncInvokeService;

    @Autowired
    private KafkaHelperService kafkaHelperService;

    @Autowired
    private ProjectRepository projectRepository;

    @Autowired
    private KafkaClusterProcessService kafkaClusterProcessService;

    @Autowired
    private ConnectorService connectorService;

    @Autowired
    private I18nService i18n;

    @Override
    public KafkaClusterDTO getKafkaCluster(final Long id) {
        return kafkaClusterRepository.findById(id)
                .map(KafkaClusterDTO::new)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));
    }

    @Override
    public KafkaClusterDTO getKafkaClusterByBootstrapServers(final String bootstrapServers) {
        if (StringUtils.isEmpty(bootstrapServers)
        ) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER, bootstrapServers));
        }
        KafkaClusterDO kafkaCluster = kafkaClusterRepository.findByBootstrapServers(bootstrapServers)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(Kafka.CLUSTER_NOT_FOUND, bootstrapServers)));
        return new KafkaClusterDTO(kafkaCluster);
    }

    @Override
    public KafkaClusterDTO getKafkaClusterWithFlatConfig(final Long id) {
        KafkaClusterDTO kafkaCluster = getKafkaCluster(id);
        String securityConfiguration = kafkaCluster.getSecurityConfiguration();
        Map<String, Object> config;
        try {
            config = objectMapper.readValue(securityConfiguration, Map.class);
        } catch (JsonProcessingException e) {
            throw new ServerErrorException(e);
        }
        Object securityProtocol = config.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
        if (Objects.nonNull(securityProtocol)) {
            kafkaCluster.setSecurityProtocol(String.valueOf(securityProtocol));
        }
        Object saslMechanism = config.get(SaslConfigs.SASL_MECHANISM);
        if (Objects.nonNull(saslMechanism)) {
            kafkaCluster.setSaslMechanism(String.valueOf(saslMechanism));
        }
        return kafkaCluster;
    }

    @Override
    public void saveKafkaClusterAndSyncKafkaClusterTopic(final KafkaClusterDTO dto) {
        KafkaClusterDO kafkaClusterDO = kafkaClusterProcessService.saveKafkaCluster(dto).toKafkaClusterDO();
        // 异步刷新kafka topic
        asyncInvokeService.asyncInvoke(() -> dataSystemMetadataService.refreshMetadata(Lists.newArrayList(kafkaClusterDO)));
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public KafkaClusterDTO saveKafkaCluster(final KafkaClusterDTO dto) {
        Preconditions.checkArgument(Strings.isNotBlank(dto.getBootstrapServers()), "param: bootstrapServers is required.");
        Preconditions.checkArgument(Objects.nonNull(dto.getProjectId()), "param: projectId is required.");
        ProjectDO project = projectRepository.findById(dto.getProjectId())
                .orElseThrow(() -> new EntityNotFoundException(String.format("projectId: %s", dto.getProjectId())));
        KafkaClusterDO cluster = saveKafkaClusterAndRefreshTopic(dto);
        // 保存项目-kafka集群关联
        Set<KafkaClusterDO> kafkaClusters = project.getKafkaClusters();
        kafkaClusters.add(cluster);
        project.setKafkaClusters(kafkaClusters);
        projectRepository.save(project);
        return new KafkaClusterDTO(cluster);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public KafkaClusterDTO saveInternalKafkaCluster(final KafkaClusterDTO cluster) {
        if (StringUtils.isEmpty(cluster.getBootstrapServers())
                || Objects.isNull(cluster.getClusterType())
                || StringUtils.isEmpty(cluster.getSecurityProtocol())
                || !checkSecurityProtocolForSaslPlaintext(cluster)
        ) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER, cluster));
        }
        // 已经存在集群创建失败,KafkaClusterType+bootstrapServers, 这个逻辑仅生效内部集群维护,用户集群,可以是同一个KafkaClusterType对应多个集群地址
        String bootstrapServers = cluster.getBootstrapServers().trim();
        KafkaClusterType clusterType = cluster.getClusterType();
        kafkaClusterRepository.findByClusterTypeOrBootstrapServers(clusterType, bootstrapServers)
                .ifPresent(it -> {
                    throw new EntityExistsException(i18n.msg(Kafka.CLUSTER_ALREADY_EXISTED, it.getBootstrapServers()));
                });

        // 权限校验,加密处理
        KafkaClusterDO savedCluster = saveKafkaClusterAndRefreshTopic(cluster);
        return new KafkaClusterDTO(savedCluster);
    }

    @Override
    public void deleteInternalKafkaCluster(final String bootstrapServers) {

        KafkaClusterDO kafkaCluster = kafkaClusterRepository.findByBootstrapServers(bootstrapServers)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(Kafka.CLUSTER_NOT_FOUND, bootstrapServers)));

        if (!CollectionUtils.isEmpty(kafkaCluster.getConnectors())) {
            throw new ClientErrorException(i18n.msg(Kafka.CLUSTER_DELETE_CLUSTER_WITH_CONNECTORS));
        }
        kafkaClusterRepository.deleteById(kafkaCluster.getId());
    }

    private boolean checkSecurityProtocolForSaslPlaintext(final KafkaClusterDTO cluster) {
        String securityProtocol = Optional.ofNullable(cluster.getSecurityProtocol()).orElse(SystemConstant.EMPTY_STRING).trim();
        String saslMechanism = Optional.ofNullable(cluster.getSaslMechanism()).orElse(SystemConstant.EMPTY_STRING).trim();
        String saslUsername = Optional.ofNullable(cluster.getSaslUsername()).orElse(SystemConstant.EMPTY_STRING).trim();
        String saslPassword = Optional.ofNullable(cluster.getSaslPassword()).orElse(SystemConstant.EMPTY_STRING).trim();
        if (SystemConstant.Kafka.SASL_PLAINTEXT.equals(securityProtocol)) {
            if (StringUtils.isEmpty(saslMechanism)
                    || StringUtils.isEmpty(saslUsername)
                    || StringUtils.isEmpty(saslPassword)
            ) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void updateKafkaCluster(final KafkaClusterDTO dto) {
        Preconditions.checkArgument(Strings.isNotBlank(dto.getBootstrapServers()), "param: bootstrapServers is required.");
        Preconditions.checkArgument(Objects.nonNull(dto.getId()), "param: id is required.");
        saveKafkaClusterAndRefreshTopic(dto);

    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Page<KafkaClusterDTO> queryByProject(final KafkaClusterQuery query) {
        if (Objects.isNull(query.getProjectId())) {
            throw new ClientErrorException("param: projectId is required.");
        }
        // 根据项目查询关联的集群
        ProjectDO project = projectRepository.findById(query.getProjectId())
                .orElseThrow(() -> new EntityNotFoundException(String.format("project id: %s", query.getProjectId())));
        List<Long> kafkaClusterIds = project.getKafkaClusters().stream().map(KafkaClusterDO::getId).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(kafkaClusterIds)) {
            return Page.empty();
        }
        query.setKafkaClusterIds(kafkaClusterIds);
        return queryAll(query);
    }

    private Page<KafkaClusterDTO> queryAll(final KafkaClusterQuery query) {
        Pageable pageable = PagedQuery.ofPage(query.getCurrent(), query.getPageSize());
        return kafkaClusterRepository.queryAll(query, pageable).map(KafkaClusterDTO::new);
    }

    private KafkaClusterDO saveKafkaClusterAndRefreshTopic(final KafkaClusterDTO dto) {
        KafkaClusterDO kafkaCluster = dto.toKafkaClusterDO();
        Map<String, Object> config = buildSecurityConfiguration(dto);

        // 检查客户端配置是否正确
        kafkaHelperService.checkConfig(kafkaCluster.getBootstrapServers(), config);

        // 加密处理sasl.jaas.config
        if (config.containsKey(SaslConfigs.SASL_JAAS_CONFIG)) {
            String encryptJaasConfig = EncryptUtil.encrypt(String.valueOf(config.get(SaslConfigs.SASL_JAAS_CONFIG)));
            config.put(SaslConfigs.SASL_JAAS_CONFIG, encryptJaasConfig);
        } else {
            config.put(SaslConfigs.SASL_MECHANISM, SystemConstant.EMPTY_STRING);
            config.put(SaslConfigs.SASL_JAAS_CONFIG, SystemConstant.EMPTY_STRING);
        }

        String securityConfiguration;
        try {
            securityConfiguration = objectMapper.writeValueAsString(config);
        } catch (JsonProcessingException e) {
            throw new ServerErrorException(e);
        }
        kafkaCluster.setSecurityConfiguration(securityConfiguration);

        // 保存kafka集群信息
        KafkaClusterDO savedKafkaCluster = kafkaClusterRepository.save(kafkaCluster);
        return savedKafkaCluster;
    }

    /**
     * 构建kafka配置. 数据库JSON格式如下： { "security.protocol":"SASL_PLAINTEXT", // PLAINTEXT、SASL_PLAINTEXT "sasl.mechanism":"SCRAM-SHA-512", // PLAIN、SCRAM-SHA-512、SCRAM-SHA-256
     * "sasl.jaas.config":"org.apache.kafka.common.security.scram.ScramLoginModule required username=\"xxx\" password=\"xxx\";" }
     */
    private Map<String, Object> buildSecurityConfiguration(final KafkaClusterDTO dto) {
        Map<String, Object> config = Maps.newHashMapWithExpectedSize(3);
        String securityProtocol = dto.getSecurityProtocol();
        // 安全协议为SASL_PLAINTEXT时，需要添加sasl.mechanism、sasl.jaas.config两项配置
        if (StringUtils.equals(SECURITY_PROTOCOL_SASL_PLAINTEXT, securityProtocol)) {
            config.put(SaslConfigs.SASL_MECHANISM, dto.getSaslMechanism());
            String saslJaasConfig;
            if (StringUtils.equals(SASL_MECHANISM_PLAIN, dto.getSaslMechanism())) {
                saslJaasConfig = String.format(SASL_JAAS_CONFIG_PATTERN, PLAIN_LOGIN_MODULE_CLASS, dto.getSaslUsername(),
                        dto.getSaslPassword());
            } else {
                saslJaasConfig = String.format(SASL_JAAS_CONFIG_PATTERN,
                        SCRAM_LOGIN_MODULE_CLASS, dto.getSaslUsername(), dto.getSaslPassword());
            }
            config.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        }
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        return config;
    }

    @Override
    public Map<String, Object> getAdminConfig(final Long id) {
        KafkaClusterDO kafkaCluster = kafkaClusterRepository.findById(id)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));
        try {
            Map<String, Object> adminConfig = objectMapper.readValue(kafkaCluster.getSecurityConfiguration(),
                    Map.class);
            // SASL_JAAS_CONFIG 解密处理
            String saslJaasConfig = String.valueOf(adminConfig.get(SaslConfigs.SASL_JAAS_CONFIG));
            if (StringUtils.isNotBlank(saslJaasConfig)) {
                adminConfig.put(SaslConfigs.SASL_JAAS_CONFIG, EncryptUtil.decrypt(saslJaasConfig));
            }
            // 集群地址
            adminConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers());
            return adminConfig;
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
