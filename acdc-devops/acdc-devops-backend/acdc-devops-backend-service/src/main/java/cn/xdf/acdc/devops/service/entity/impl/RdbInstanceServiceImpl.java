package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RoleType;
import cn.xdf.acdc.devops.repository.RdbInstanceRepository;
import cn.xdf.acdc.devops.service.entity.RdbInstanceService;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Service
public class RdbInstanceServiceImpl implements RdbInstanceService {

    @Autowired
    private RdbInstanceRepository rdbInstanceRepository;

    @Override
    public RdbInstanceDO save(final RdbInstanceDO rdbInstance) {
        return rdbInstanceRepository.save(rdbInstance);
    }

    @Override
    public List<RdbInstanceDO> saveAll(final List<RdbInstanceDO> rdbInstanceList) {
        return rdbInstanceRepository.saveAll(rdbInstanceList);
    }

    @Override
    public Page<RdbInstanceDO> query(final RdbInstanceDO rdbInstance, final Pageable pageable) {

        return rdbInstanceRepository.findAll(RdbInstanceService.specificationOf(rdbInstance), pageable);
    }

    @Override
    public List<RdbInstanceDO> queryAll(final RdbInstanceDO rdbInstance) {
        return rdbInstanceRepository.findAll(RdbInstanceService.specificationOf(rdbInstance));
    }

    @Override
    public Optional<RdbInstanceDO> findById(final Long id) {
        return rdbInstanceRepository.findById(id);
    }

    @Override
    public List<RdbInstanceDO> findAllById(final Iterable<Long> ids) {
        return rdbInstanceRepository.findAllById(ids);
    }

    @Override
    public Optional<RdbInstanceDO> findByRdbIdAndHostAndPort(final Long rdbId, final String host, final Integer port) {
        return rdbInstanceRepository.findByRdbIdAndHostAndPort(rdbId, host, port);
    }

    @Override
    public Optional<RdbInstanceDO> findDataSourceInstanceByRdbId(final Long rdbId) {
        return rdbInstanceRepository.findByRdbIdAndRole(rdbId, RoleType.DATA_SOURCE);
    }
}
