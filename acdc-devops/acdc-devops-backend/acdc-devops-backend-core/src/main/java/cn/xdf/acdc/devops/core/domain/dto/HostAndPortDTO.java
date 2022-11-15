package cn.xdf.acdc.devops.core.domain.dto;

import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import com.google.common.base.Preconditions;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.util.CollectionUtils;

/**
 * Host and port.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HostAndPortDTO {

    public static final HostAndPortDTO INVALID_HOST_AND_PORT = HostAndPortDTO.builder().host("-99").port(-99).build();

    private String host;

    private int port;

    public HostAndPortDTO(final RdbInstanceDO instance) {
        this.host = instance.getHost();
        this.port = instance.getPort();
    }

    /**
     * Convert rdbInstances to hostAndPortDTO set.
     * @param rdbInstances rdbInstances
     * @return Set
     */
    public static Set<HostAndPortDTO> toHostAndPortDTOs(final Set<RdbInstanceDO> rdbInstances) {
        Preconditions.checkArgument(!CollectionUtils.isEmpty(rdbInstances));
        return rdbInstances.stream().map(HostAndPortDTO::new).collect(Collectors.toSet());
    }

    /**
     * A pojo signature.
     *
     * @return signature
     */
    public String getSignature() {
        return host + ":" + port;
    }
}
