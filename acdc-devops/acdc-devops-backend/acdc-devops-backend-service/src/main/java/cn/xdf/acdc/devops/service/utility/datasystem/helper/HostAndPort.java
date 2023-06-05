package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Host and port.
 */
@Data
@AllArgsConstructor
public class HostAndPort {
    
    private String host;
    
    private int port;
    
    /**
     * A pojo signature.
     *
     * @return signature
     */
    public String getSignature() {
        return host + ":" + port;
    }
}
