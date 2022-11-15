package cn.xdf.acdc.devops.service.process.datasystem.hive.impl;

import cn.xdf.acdc.devops.core.domain.dto.HdfsDTO;
import cn.xdf.acdc.devops.repository.HdfsRepository;
import cn.xdf.acdc.devops.service.process.datasystem.hive.HdfsProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;

@Service
public class HdfsProcessServiceImpl implements HdfsProcessService {

    @Autowired
    private HdfsRepository hdfsRepository;

    @Override
    public HdfsDTO getHdfs(final Long id) {
        return hdfsRepository.findById(id)
            .map(HdfsDTO::toHdfsDTO)
            .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));
    }
}
