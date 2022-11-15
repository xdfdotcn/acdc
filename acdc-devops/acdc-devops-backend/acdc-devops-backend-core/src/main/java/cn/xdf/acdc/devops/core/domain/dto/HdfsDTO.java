package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.HdfsDO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

// CHECKSTYLE:OFF
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class HdfsDTO {

    private Long id;

    private String name;

    public static HdfsDTO toHdfsDTO(HdfsDO hdfsDO) {
        return HdfsDTO.builder()
            .id(hdfsDO.getId())
            .name(hdfsDO.getName())
            .build();
    }
}
