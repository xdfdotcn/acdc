package cn.xdf.acdc.devops.service.process.datasystem.hive.impl;

import cn.xdf.acdc.devops.core.domain.dto.HiveDTO;
import cn.xdf.acdc.devops.repository.HiveRepository;
import cn.xdf.acdc.devops.service.process.datasystem.hive.HiveProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;

@Service
public class HiveProcessServiceImpl implements HiveProcessService {

    @Autowired
    private HiveRepository hiveRepository;

    @Override
    public HiveDTO getHive(final Long id) {
        return hiveRepository.findById(id)
            .map(HiveDTO::toHiveDTO)
            .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));
    }
}
