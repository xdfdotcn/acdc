package cn.xdf.acdc.devops.core.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class KafkaSpecificConfDTO {
    
    private String kafkaConverterType;
    
}
