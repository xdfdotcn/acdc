package cn.xdf.acdc.devops.service.process.connection.impl;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionRequisitionConnectionMappingDTO;
import cn.xdf.acdc.devops.core.domain.entity.ConnectionRequisitionConnectionMappingDO;
import cn.xdf.acdc.devops.repository.ConnectionRequisitionConnectionMappingRepository;
import cn.xdf.acdc.devops.service.process.connection.ConnectionRequisitionConnectionMappingProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class ConnectionRequisitionConnectionMappingProcessServiceImpl implements ConnectionRequisitionConnectionMappingProcessService {

    @Autowired
    private ConnectionRequisitionConnectionMappingRepository connectionRequisitionConnectionMappingRepository;

    @Override
    public List<ConnectionRequisitionConnectionMappingDTO> saveAll(final List<ConnectionRequisitionConnectionMappingDO> mappings) {
        return connectionRequisitionConnectionMappingRepository.saveAll(mappings).stream()
                .map(ConnectionRequisitionConnectionMappingDTO::toConnectionRequisitionConnectionMappingDTO)
                .collect(Collectors.toList());
    }
}
