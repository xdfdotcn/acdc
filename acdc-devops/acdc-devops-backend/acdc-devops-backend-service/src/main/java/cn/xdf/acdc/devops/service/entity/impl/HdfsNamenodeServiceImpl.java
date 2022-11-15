package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.HdfsNamenodeDO;
import cn.xdf.acdc.devops.repository.HdfsNamenodeRepository;
import cn.xdf.acdc.devops.service.entity.HdfsNamenodeService;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class HdfsNamenodeServiceImpl implements HdfsNamenodeService {

    @Autowired
    private HdfsNamenodeRepository hdfsNamenodeRepository;

    @Override
    public HdfsNamenodeDO save(final HdfsNamenodeDO hdfsNamenode) {
        return hdfsNamenodeRepository.save(hdfsNamenode);
    }

    @Override
    public List<HdfsNamenodeDO> findAll() {
        return hdfsNamenodeRepository.findAll();
    }

    @Override
    public Optional<HdfsNamenodeDO> findById(final Long id) {
        return hdfsNamenodeRepository.findById(id);
    }
}
