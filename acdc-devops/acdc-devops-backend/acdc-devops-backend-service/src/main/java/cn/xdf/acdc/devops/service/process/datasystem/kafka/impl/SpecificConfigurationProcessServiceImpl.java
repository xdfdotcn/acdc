package cn.xdf.acdc.devops.service.process.datasystem.kafka.impl;

import cn.xdf.acdc.devops.core.domain.dto.KafkaSpecificConfDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.KafkaConverterType;
import cn.xdf.acdc.devops.service.error.BadRequestException;
import cn.xdf.acdc.devops.service.error.ErrorMsg;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.SpecificConfigurationProcessService;
import cn.xdf.acdc.devops.service.util.BizAssert;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SpecificConfigurationProcessServiceImpl implements SpecificConfigurationProcessService {

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public KafkaSpecificConfDTO kafkaSpecificConfDeserialize(final String json) {
        BizAssert.badRequest(!Strings.isNullOrEmpty(json), ErrorMsg.E_112, "Kafka converter must not be null");
        try {
            KafkaSpecificConfDTO conf = objectMapper.readValue(json, KafkaSpecificConfDTO.class);
            KafkaConverterType.valueOf(conf.getKafkaConverterType());
            return conf;
        } catch (JsonProcessingException e) {
            throw new BadRequestException(ErrorMsg.E_112, e.getMessage());
        }
    }
}
