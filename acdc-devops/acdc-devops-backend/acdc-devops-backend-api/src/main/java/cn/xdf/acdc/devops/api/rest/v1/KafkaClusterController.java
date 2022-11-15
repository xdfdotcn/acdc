package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.core.domain.dto.KafkaClusterDTO;
import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.query.KafkaClusterQuery;
import cn.xdf.acdc.devops.service.process.datasystem.kafka.KafkaClusterProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/v1")
public class KafkaClusterController {

    @Autowired
    private KafkaClusterProcessService kafkaClusterProcessService;

    /**
     * query kafka cluster list of project by page.
     *
     * @param query query conditions
     * @return Page
     */
    @GetMapping("/kafka-clusters")
    public PageDTO<KafkaClusterDTO> queryKafkaClusters(final KafkaClusterQuery query) {
        Page<KafkaClusterDTO> page = kafkaClusterProcessService.queryByProject(query);
        return PageDTO.of(page.getContent(), page.getTotalElements());
    }

    /**
     * save kafka cluster.
     *
     * @param dto kafka cluster DTO
     * @date 2022/9/14 2:16 下午
     */
    @PostMapping("/kafka-clusters")
    public void saveKafkaCluster(@RequestBody final KafkaClusterDTO dto) {
        kafkaClusterProcessService.saveKafkaClusterAndSyncKafkaClusterTopic(dto);
    }

    /**
     * update kafka cluster.
     *
     * @param dto kafka cluster DTO
     * @date 2022/9/14 2:16 下午
     */
    @PutMapping("/kafka-clusters")
    public void updateKafkaCluster(@RequestBody final KafkaClusterDTO dto) {
        kafkaClusterProcessService.updateKafkaCluster(dto);
    }

    /**
     * get kafka cluster.
     *
     * @param id kafka cluster id
     * @return cn.xdf.acdc.devops.core.domain.dto.KafkaClusterDTO
     * @date 2022/9/15 5:54 下午
     */
    @GetMapping("/kafka-clusters/{id}")
    public KafkaClusterDTO getKafkaCluster(@PathVariable final Long id) {
        return kafkaClusterProcessService.getKafkaClusterWithFlatConfig(id);
    }
}
