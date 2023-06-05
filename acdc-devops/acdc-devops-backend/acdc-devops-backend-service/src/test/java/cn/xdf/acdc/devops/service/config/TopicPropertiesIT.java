package cn.xdf.acdc.devops.service.config;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class TopicPropertiesIT {
    
    @Autowired
    private TopicProperties topicProperties;
    
    @Test
    public void testTICDCConfig() {
        // topic create
        Assertions.assertThat(topicProperties.getTicdc().getPartitions()).isEqualTo(12);
        Assertions.assertThat(topicProperties.getTicdc().getReplicationFactor()).isEqualTo((short) 6);
        Assertions.assertThat(topicProperties.getTicdc().getConfigs()).isEmpty();
        
        // topic acl
        String[] expectACLOperations = new String[]{"READ", "WRITE"};
        
        Assertions.assertThat(topicProperties.getTicdc().getAcl().getUsername()).isEqualTo("acdc");
        Assertions.assertThat(topicProperties.getTicdc().getAcl().getOperations()).isEqualTo(expectACLOperations);
    }
}
