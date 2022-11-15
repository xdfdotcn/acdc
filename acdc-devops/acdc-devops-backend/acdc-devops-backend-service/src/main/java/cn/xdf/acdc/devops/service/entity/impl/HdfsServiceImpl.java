package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.HdfsDO;
import cn.xdf.acdc.devops.repository.HdfsRepository;
import cn.xdf.acdc.devops.service.entity.HdfsService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class HdfsServiceImpl implements HdfsService {

    @Autowired
    private HdfsRepository hdfsRepository;

    @Override
    public HdfsDO save(final HdfsDO hdfs) {
        return hdfsRepository.saveAndFlush(hdfs);
    }

    @Override
    public List<HdfsDO> findAll() {
        return hdfsRepository.findAll();
    }
}
