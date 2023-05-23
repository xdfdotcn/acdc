package cn.xdf.acdc.devops.service.process.kafka.impl;

import cn.xdf.acdc.devops.core.constant.SystemConstant;
import cn.xdf.acdc.devops.core.domain.dto.KafkaClusterDTO;
import cn.xdf.acdc.devops.core.domain.entity.KafkaClusterDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaClusterType;
import cn.xdf.acdc.devops.core.util.StringUtil;
import cn.xdf.acdc.devops.repository.KafkaClusterRepository;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.kafka.KafkaClusterService;
import cn.xdf.acdc.devops.service.process.kafka.KafkaConstant;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.KafkaHelperService;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.Client;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey.Kafka;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.persistence.EntityExistsException;
import javax.persistence.EntityNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Service
public class KafkaClusterServiceImpl implements KafkaClusterService {
    
    @Autowired
    private KafkaClusterRepository kafkaClusterRepository;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @Autowired
    private KafkaHelperService kafkaHelperService;
    
    @Autowired
    private I18nService i18n;
    
    @Override
    @Transactional
    public KafkaClusterDTO create(final KafkaClusterDTO kafkaClusterDTO, final Map<String, Object> securityConfig) {
        if (StringUtils.isEmpty(kafkaClusterDTO.getBootstrapServers())
                || Objects.isNull(kafkaClusterDTO.getClusterType())
                || StringUtils.isEmpty(kafkaClusterDTO.getSecurityProtocol())
                || CollectionUtils.isEmpty(securityConfig)
                || !checkSecurityProtocolForSaslPlaintext(kafkaClusterDTO)
        ) {
            throw new ClientErrorException(i18n.msg(Client.INVALID_PARAMETER, kafkaClusterDTO));
        }
        
        String bootstrapServers = kafkaClusterDTO.getBootstrapServers().trim();
        KafkaClusterType clusterType = kafkaClusterDTO.getClusterType();
        
        checkAdminClientConfig(bootstrapServers, securityConfig);
        
        kafkaClusterRepository.findByClusterTypeOrBootstrapServers(clusterType, bootstrapServers)
                .ifPresent(it -> {
                    throw new EntityExistsException(i18n.msg(Kafka.CLUSTER_ALREADY_EXISTED, it.getBootstrapServers()));
                });
        
        KafkaClusterDO kafkaClusterDO = kafkaClusterDTO.toDO();
        kafkaClusterDO.setSecurityConfiguration(generateEncryptSecurityConfig(securityConfig));
        
        return new KafkaClusterDTO(kafkaClusterRepository.save(kafkaClusterDO));
    }
    
    private void checkAdminClientConfig(final String bootstrapServers, final Map<String, Object> securityConfig) {
        Map<String, Object> adminConfig = new HashMap<>(securityConfig);
        adminConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaHelperService.checkAdminClientConfig(adminConfig);
    }
    
    @Override
    @Transactional
    public KafkaClusterDTO getById(final Long id) {
        return kafkaClusterRepository
                .findById(id)
                .map(KafkaClusterDTO::new)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(Kafka.CLUSTER_NOT_FOUND, id)));
    }
    
    @Override
    @Transactional
    public Optional<KafkaClusterDTO> getByBootstrapServers(final String bootstrapServers) {
        Optional<KafkaClusterDO> kafkaClusterDOOptional = kafkaClusterRepository.findByBootstrapServers(bootstrapServers);
        if (kafkaClusterDOOptional.isPresent()) {
            return Optional.of(new KafkaClusterDTO(kafkaClusterDOOptional.get()));
        }
        return Optional.empty();
    }
    
    @Override
    @Transactional
    public KafkaClusterDTO getACDCKafkaCluster() {
        return kafkaClusterRepository
                .findByClusterType(KafkaClusterType.INNER)
                .map(KafkaClusterDTO::new)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(Kafka.CLUSTER_NOT_FOUND, KafkaClusterType.INNER.name())));
    }
    
    @Override
    @Transactional
    public KafkaClusterDTO getTICDCKafkaCluster() {
        return kafkaClusterRepository
                .findByClusterType(KafkaClusterType.TICDC)
                .map(KafkaClusterDTO::new)
                .orElseThrow(() -> new EntityNotFoundException(i18n.msg(Kafka.CLUSTER_NOT_FOUND, KafkaClusterType.TICDC.name())));
    }
    
    @Override
    @Transactional
    public Map<String, Object> getDecryptedAdminConfig(final Long id) {
        KafkaClusterDTO kafkaCluster = this.getById(id);
        Map<String, String> securityConfiguration = StringUtil.convertJsonStringToMap(kafkaCluster.getSecurityConfiguration());
        
        Map<String, Object> adminConfig = new HashMap<>(securityConfiguration);
        adminConfig.computeIfPresent(SaslConfigs.SASL_JAAS_CONFIG, (key, value) -> EncryptUtil.decrypt(value.toString()));
        // 集群地址
        adminConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers());
        
        return adminConfig;
    }
    
    private boolean checkSecurityProtocolForSaslPlaintext(final KafkaClusterDTO kafkaClusterDTO) {
        String securityProtocol = Optional.ofNullable(kafkaClusterDTO.getSecurityProtocol()).orElse(SystemConstant.EMPTY_STRING).trim();
        String saslMechanism = Optional.ofNullable(kafkaClusterDTO.getSaslMechanism()).orElse(SystemConstant.EMPTY_STRING).trim();
        String saslUsername = Optional.ofNullable(kafkaClusterDTO.getSaslUsername()).orElse(SystemConstant.EMPTY_STRING).trim();
        String saslPassword = Optional.ofNullable(kafkaClusterDTO.getSaslPassword()).orElse(SystemConstant.EMPTY_STRING).trim();
        
        if (KafkaConstant.SECURITY_PROTOCOL_SASL_PLAINTEXT.equals(securityProtocol)) {
            if (StringUtils.isEmpty(saslMechanism)
                    || StringUtils.isEmpty(saslUsername)
                    || StringUtils.isEmpty(saslPassword)
            ) {
                return false;
            }
        }
        return true;
    }
    
    private String generateEncryptSecurityConfig(final Map<String, Object> securityConfig) {
        // sasl.jaas.config
        if (securityConfig.containsKey(SaslConfigs.SASL_JAAS_CONFIG)) {
            String encryptJaasConfig = EncryptUtil.encrypt(String.valueOf(securityConfig.get(SaslConfigs.SASL_JAAS_CONFIG)));
            securityConfig.put(SaslConfigs.SASL_JAAS_CONFIG, encryptJaasConfig);
        } else {
            securityConfig.put(SaslConfigs.SASL_MECHANISM, SystemConstant.EMPTY_STRING);
            securityConfig.put(SaslConfigs.SASL_JAAS_CONFIG, SystemConstant.EMPTY_STRING);
        }
        
        try {
            return objectMapper.writeValueAsString(securityConfig);
        } catch (JsonProcessingException e) {
            throw new ServerErrorException(e);
        }
    }
    
    @Override
    @Transactional
    public void deleteById(final Long id) {
        kafkaClusterRepository.deleteById(id);
    }
}
