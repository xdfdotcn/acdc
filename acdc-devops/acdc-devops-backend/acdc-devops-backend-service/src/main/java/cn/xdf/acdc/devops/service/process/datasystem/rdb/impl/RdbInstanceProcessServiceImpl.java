package cn.xdf.acdc.devops.service.process.datasystem.rdb.impl;

import cn.xdf.acdc.devops.core.domain.dto.RdbInstanceDTO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbDatabaseDO;
import cn.xdf.acdc.devops.core.domain.entity.RdbInstanceDO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.DataSystemType;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.RoleType;
import cn.xdf.acdc.devops.repository.RdbDatabaseRepository;
import cn.xdf.acdc.devops.repository.RdbInstanceRepository;
import cn.xdf.acdc.devops.repository.RdbRepository;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.common.AsyncInvokeService;
import cn.xdf.acdc.devops.service.process.datasystem.DataSystemMetadataService;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbInstanceProcessService;
import cn.xdf.acdc.devops.service.util.EncryptUtil;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.MysqlHelperService;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.persistence.EntityNotFoundException;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class RdbInstanceProcessServiceImpl implements RdbInstanceProcessService {

    @Autowired
    private RdbInstanceRepository rdbInstanceRepository;

    @Autowired
    private RdbRepository rdbRepository;

    @Autowired
    private MysqlHelperService mysqlHelperService;

    @Autowired
    private AsyncInvokeService asyncInvokeService;

    @Autowired
    @Qualifier("rdbProcessServiceImpl")
    private DataSystemMetadataService<RdbDO> rdbDataSystemMetadataService;

    @Autowired
    private DataSystemMetadataService<RdbDatabaseDO> rdbDatabaseDataSystemMetadataService;

    @Autowired
    private RdbDatabaseRepository rdbDatabaseRepository;

    @Override
    public RdbInstanceDTO getRdbInstance(final Long id) {
        return rdbInstanceRepository.findById(id)
                .map(RdbInstanceDTO::new)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", id)));
    }

    @Override
    public List<RdbInstanceDTO> queryInstancesByRdbId(final Long rdbId) {
        List<RdbInstanceDO> rdbInstanceDOList = rdbInstanceRepository.findRdbInstanceDOSByRdbId(rdbId);
        List<RdbInstanceDTO> rdbInstanceDTOList = Lists.newArrayList();
        if (CollectionUtils.isEmpty(rdbInstanceDOList)) {
            return rdbInstanceDTOList;
        }

        return rdbInstanceDOList.stream()
                .map(RdbInstanceDTO::new)
                .collect(Collectors.toList());
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void saveRdbInstances(final Long rdbId, final List<RdbInstanceDTO> rdbInstanceDTOList) {
        if (CollectionUtils.isEmpty(rdbInstanceDTOList)) {
            return;
        }
        RdbDO rdbDO = rdbRepository.findById(rdbId)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", rdbId)));
        List<RdbInstanceDO> instances = rdbInstanceDTOList.stream().map(RdbInstanceDTO::toRdbInstanceDO)
                .collect(Collectors.toList());

        // 校验用户权限、binlog是否开启、binlog格式
        checkUserPermissionAndBinlogConfiguration(instances, rdbDO);

        // 保存rdb_instance
        rdbInstanceRepository.saveAll(instances);
        rdbDO.setRdbInstances(Sets.newHashSet(instances));
        // todo: 不手动提交事务
        rdbRepository.saveAndFlush(rdbDO);

        // 异步刷新database、table元数据
        refreshRdbMetadata(rdbDO);
    }

    @Override
    public RdbInstanceDTO getDataSourceInstanceByRdbId(final Long rdbId) {
        return rdbInstanceRepository.findByRdbIdAndRole(rdbId, RoleType.DATA_SOURCE)
                .map(RdbInstanceDTO::new)
                .orElseThrow(() -> new EntityNotFoundException(String.format("id: %s", rdbId)));
    }

    private void checkUserPermissionAndBinlogConfiguration(final List<RdbInstanceDO> instances, final RdbDO rdbDO) {
        String password = EncryptUtil.decrypt(rdbDO.getPassword());
        try {
            // todo: tidb 的相关检查
            if (DataSystemType.MYSQL.getName().equals(rdbDO.getRdbType())) {
                mysqlHelperService.checkUserPermissionsAndBinlogConfiguration(instances, rdbDO.getUsername(), password);
            }
        } catch (ServerErrorException e) {
            throw new ClientErrorException(e.getMessage(), e);
        }
    }

    private void refreshRdbMetadata(final RdbDO rdbDO) {
        asyncInvokeService.asyncInvoke(() -> {
            // 刷新databases
            rdbDataSystemMetadataService.refreshMetadata(Lists.newArrayList(rdbDO));

            // 刷新tables
            List<RdbDatabaseDO> databases = rdbDatabaseRepository.findAllByRdb(rdbDO);
            rdbDatabaseDataSystemMetadataService.refreshMetadata(databases);
        });
    }

}
