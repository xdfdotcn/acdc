package cn.xdf.acdc.devops.service.process.datasystem.check.impl;

import cn.xdf.acdc.devops.core.domain.entity.RdbDO;
import cn.xdf.acdc.devops.core.domain.query.RdbQuery;
import cn.xdf.acdc.devops.repository.RdbRepository;
import cn.xdf.acdc.devops.service.entity.RdbService;
import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import cn.xdf.acdc.devops.service.process.datasystem.check.MetadataCheckService;
import cn.xdf.acdc.devops.service.utility.datasystem.helper.MysqlHelperService;
import io.jsonwebtoken.lang.Collections;
import io.jsonwebtoken.lang.Maps;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Order(2)
@Transactional
@Slf4j
public class RdbInstancePermissionCheckService implements MetadataCheckService {

    private static final String CONNECTION_FAILED_TITLE = "数据库实例权限检查异常，请查看数据库权限及master设置";

    @Autowired
    private RdbService rdbService;

    @Autowired
    private RdbRepository rdbRepository;

    @Autowired
    private MysqlHelperService mysqlHelperService;

    @Override
    public Map<String, List<String>> checkMetadataAndReturnErrorMessage() {
        List<RdbDO> rdbs = rdbRepository.queryAll(new RdbQuery());
        List<String> rdbInstances = rdbs.stream()
                .filter(this::rdbInstancePermissionsForbidden)
                .map(RdbDO::getDbInstances)
                .map(List::toString)
                .collect(Collectors.toList());

        if (Collections.isEmpty(rdbInstances)) {
            return new HashMap<>();
        }
        return Maps.of(CONNECTION_FAILED_TITLE, rdbInstances).build();
    }

    private boolean rdbInstancePermissionsForbidden(final RdbDO rdbDO) {
        if (Collections.isEmpty(rdbDO.getRdbInstances())) {
            return false;
        }
        try {
            mysqlHelperService.checkRdbPermissions(rdbDO);
            return false;
        } catch (ServerErrorException e) {
            log.error("Check rdb instance permissions exception:{}, stack:{}.", e, e.getStackTrace());
        }
        return true;
    }
}
