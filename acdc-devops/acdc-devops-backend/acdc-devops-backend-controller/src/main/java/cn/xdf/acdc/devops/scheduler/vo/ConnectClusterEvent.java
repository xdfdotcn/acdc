package cn.xdf.acdc.devops.scheduler.vo;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ConnectClusterEvent {

    private Long clusterId;

    private Object event;

}
