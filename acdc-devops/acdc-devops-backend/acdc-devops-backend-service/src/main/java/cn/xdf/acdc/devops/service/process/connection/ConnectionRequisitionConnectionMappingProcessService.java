package cn.xdf.acdc.devops.service.process.connection;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionRequisitionConnectionMappingDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionRequisitionConnectionMappingDO;

import java.util.List;

public interface ConnectionRequisitionConnectionMappingProcessService {

    /**
     * Save connection and requisition mappings.
     *
     * @param mappings mappings
     * @return saved result
     */
    List<ConnectionRequisitionConnectionMappingDTO> saveAll(List<ConnectionRequisitionConnectionMappingDO> mappings);
}
