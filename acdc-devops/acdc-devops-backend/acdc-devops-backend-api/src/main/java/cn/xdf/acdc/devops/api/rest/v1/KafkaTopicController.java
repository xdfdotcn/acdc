package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.core.domain.dto.KafkaTopicDTO;
import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.KafkaTopicProcessService;
import cn.xdf.acdc.devops.service.util.QueryUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api")
@Transactional
public class KafkaTopicController {

    @Autowired
    private KafkaTopicProcessService kafkaTopicProcessService;

    /**
     * 查询用户的 kafka topic 列表.
     * @param kafkaTopic kafkaTopic
     * @return Page
     */
    @GetMapping("/kafkaTopics")
    public PageDTO<KafkaTopicDTO> queryKafkaTopic(final KafkaTopicDTO kafkaTopic) {
        if (QueryUtil.isNullId(kafkaTopic.getKafkaClusterId())) {
            return PageDTO.empty();
        }
        Page<KafkaTopicDTO> page = kafkaTopicProcessService.queryKafkaTopic(kafkaTopic);
        return PageDTO.of(page.getContent(), page.getTotalElements());
    }
}
