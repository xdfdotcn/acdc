package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.core.domain.dto.MysqlDataSourceDTO;
import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.dto.RdbInstanceDTO;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbInstanceProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("api")
public class RdbInstanceController {

    @Autowired
    private RdbProcessService rdbProcessService;

    @Autowired
    private RdbInstanceProcessService instanceProcessService;

    /**
     * 根据rdb查询实例列表.
     *
     * @param id rdb主键
     * @return java.util.List
     * @date 2022/8/3 2:54 下午
     */
    @GetMapping("/rdbs/{id}/instances")
    public PageDTO<RdbInstanceDTO> queryInstances(@PathVariable("id") final Long id) {
        List<RdbInstanceDTO> rdbInstanceList = instanceProcessService.queryInstancesByRdbId(id);
        return PageDTO.of(rdbInstanceList, rdbInstanceList.size());
    }

    /**
     * 创建 rdbMysql.
     *
     * @param mysqlDataSourceDTO mysqlDataSourceDTO
     */
    @PostMapping("/rdbs/mysqlInstance")
    public void createRdbMysqlInstance(@RequestBody final MysqlDataSourceDTO mysqlDataSourceDTO) {
        rdbProcessService.saveMysqlInstance(mysqlDataSourceDTO);
    }

    /**
     * 获取 mysql 实例配置 .
     *
     * @param rdbId rdbId
     * @return RdbMysqlDTO
     */
    @GetMapping("/rdbs/mysqlInstance/{rdbId}")
    public MysqlDataSourceDTO getRdbMysqlInstance(@PathVariable("rdbId") final Long rdbId) {
        return rdbProcessService.getRdbMysqlInstance(rdbId);
    }

    /**
     * 批量添加rdb集群实例.
     *
     * @param id                 rdb id
     * @param rdbInstanceDTOList instance列表
     * @date 2022/8/3 4:12 下午
     */
    @PostMapping("/rdbs/{id}/instances")
    public void saveRdbInstances(@PathVariable("id") final Long id, @RequestBody final List<RdbInstanceDTO> rdbInstanceDTOList) {
        instanceProcessService.saveRdbInstances(id, rdbInstanceDTOList);
    }
}
