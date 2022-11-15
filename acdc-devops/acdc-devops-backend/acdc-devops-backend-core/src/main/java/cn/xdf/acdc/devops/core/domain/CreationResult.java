package cn.xdf.acdc.devops.core.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CreationResult<T> {

    private boolean isPresent;

    private T result;
}
