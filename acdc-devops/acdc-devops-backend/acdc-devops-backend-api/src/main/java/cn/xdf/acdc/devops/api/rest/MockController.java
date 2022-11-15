package cn.xdf.acdc.devops.api.rest;

// CHECKSTYLE:OFF

import cn.xdf.acdc.devops.core.domain.dto.KafkaClusterDTO;
import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.dto.RdbInstanceDTO;
import cn.xdf.acdc.devops.core.domain.dto.UserDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RoleType;
import cn.xdf.acdc.devops.repository.ConnectorClassRepository;
import cn.xdf.acdc.devops.service.config.TopicConfig;
import cn.xdf.acdc.devops.service.process.connection.ConnectionProcessService;
import cn.xdf.acdc.devops.service.process.connection.ConnectionRequisitionProcessService;
import cn.xdf.acdc.devops.service.process.connection.approval.ApprovalStateMachine;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("api/mock")
public class MockController {

    @Autowired
    private ConnectorClassRepository connectorClassRepository;

    @Autowired
    private ConnectionRequisitionProcessService connectionRequisitionProcessService;

    @Autowired
    private ConnectionProcessService connectionProcessService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private TopicConfig topicConfig;

    @Autowired
    private ApprovalStateMachine approvalStateMachine;

    @GetMapping("/project-users")
    public PageDTO<UserDTO> queryUser(final UserDTO projectDTO) {
        List<UserDTO> users = new ArrayList<>();
        users.add(UserDTO.builder().id(1L).email("test01@xdf.cn").build());
        users.add(UserDTO.builder().id(2L).email("test02@xdf.cn").build());
        users.add(UserDTO.builder().id(3L).email("test03@xdf.cn").build());
        return PageDTO.of(users, users.size());
    }

    @GetMapping("/users")
    public PageDTO<UserDTO> userSelect(final UserDTO projectDTO) {
        List<UserDTO> users = new ArrayList<>();
        users.add(UserDTO.builder().id(1L).email("test01@xdf.cn").build());
        users.add(UserDTO.builder().id(2L).email("test02@xdf.cn").build());
        users.add(UserDTO.builder().id(3L).email("test03@xdf.cn").build());
        users.add(UserDTO.builder().id(4L).email("test04@xdf.cn").build());
        users.add(UserDTO.builder().id(5L).email("test05@xdf.cn").build());
        return PageDTO.of(users, users.size());
    }

    @GetMapping("/rdb-instances")
    public PageDTO<RdbInstanceDTO> queryRdbInStance(final RdbInstanceDTO rdbInstanceDTO) {
        List<RdbInstanceDTO> rdbInstances = new ArrayList<>();
        rdbInstances.add(RdbInstanceDTO.builder().id(1L).host("127.0.0.1").port("3301").roleType(RoleType.MASTER).build());
        rdbInstances.add(RdbInstanceDTO.builder().id(2L).host("127.0.0.2").port("3302").roleType(RoleType.SLAVE).build());
        rdbInstances.add(RdbInstanceDTO.builder().id(3L).host("127.0.0.3").port("3303").roleType(RoleType.SLAVE).build());
        rdbInstances.add(RdbInstanceDTO.builder().id(4L).host("127.0.0.4").port("3304").roleType(RoleType.SLAVE).build());
        rdbInstances.add(RdbInstanceDTO.builder().id(5L).host("127.0.0.5").port("3305").roleType(RoleType.DATA_SOURCE).build());
        return PageDTO.of(rdbInstances, rdbInstances.size());
    }


    @GetMapping("/kafka-cluster")
    public PageDTO<KafkaClusterDTO> queryKafkaCluster(final KafkaClusterDTO kafka) {

        List<KafkaClusterDTO> clusterList = Lists.newArrayList(
                KafkaClusterDTO.builder().id(1l).name("test01").bootstrapServers("192.168.0.0.1:9093").version("2.6.3").description("test").build(),
                KafkaClusterDTO.builder().id(2L).name("test02").bootstrapServers("192.168.0.0.2:9093").version("2.6.3").description("test").build()
        );
        return PageDTO.of(clusterList, clusterList.size());
    }

    @GetMapping("/kafka-clusters/{id}")
    public KafkaClusterDTO queryProject(@PathVariable("id") final Long id) {
        return KafkaClusterDTO.builder()
                .name("test01")
                .bootstrapServers("192.168.0.0.1:9093")
                .version("2.6.3")
                .description("test")
                .securityProtocol("SASL_PLAINTEXT")
                .saslMechanism("SCRAM-SHA-512")
                .saslUsername("admin")
                .saslPassword("1234")
                .build();
    }
}
