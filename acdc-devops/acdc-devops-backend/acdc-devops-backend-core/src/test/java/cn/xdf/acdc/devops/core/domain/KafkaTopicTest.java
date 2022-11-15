package cn.xdf.acdc.devops.core.domain;

import cn.xdf.acdc.devops.core.domain.entity.KafkaTopicDO;
import cn.xdf.acdc.devops.core.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class KafkaTopicTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(KafkaTopicDO.class);
        KafkaTopicDO kafkaTopic1 = new KafkaTopicDO();
        kafkaTopic1.setId(1L);
        KafkaTopicDO kafkaTopic2 = new KafkaTopicDO();
        kafkaTopic2.setId(kafkaTopic1.getId());
        Assertions.assertThat(kafkaTopic1).isEqualTo(kafkaTopic2);
        kafkaTopic2.setId(2L);
        Assertions.assertThat(kafkaTopic1).isNotEqualTo(kafkaTopic2);
        kafkaTopic1.setId(null);
        Assertions.assertThat(kafkaTopic1).isNotEqualTo(kafkaTopic2);
    }
}
