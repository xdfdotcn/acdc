package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.core.domain.dto.KafkaTopicDTO;
import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.query.KafkaTopicQuery;
import cn.xdf.acdc.devops.service.process.kafka.KafkaTopicService;
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
    private KafkaTopicService kafkaTopicService;

    /**
     * Query kafka topic.
     *
     * @param query query
     * @return Page
     */
    @GetMapping("/kafkaTopics")
    public PageDTO<KafkaTopicDTO> query(final KafkaTopicQuery query) {
        if (QueryUtil.isNullId(query.getKafkaClusterId())) {
            return PageDTO.empty();
        }
        Page<KafkaTopicDTO> page = kafkaTopicService.pagedQuery(query);
        return PageDTO.of(page.getContent(), page.getTotalElements());
    }
}
