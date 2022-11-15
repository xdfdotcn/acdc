package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.dto.RdbDTO;
import cn.xdf.acdc.devops.service.process.datasystem.rdb.RdbProcessService;
import cn.xdf.acdc.devops.service.process.project.ProjectProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("api")
@Transactional
public class RdbController {

    @Autowired
    private RdbProcessService rdbProcessService;

    @Autowired
    private ProjectProcessService projectProcessService;

    /**
     * 添加rdb.
     *
     * @param rdbDTO rdb信息
     * @date 2022/8/3 4:12 下午
     */
    @PostMapping("/rdbs")
    public void saveRdb(@RequestBody final RdbDTO rdbDTO) {
        rdbProcessService.saveRdb(rdbDTO);
    }

    /**
     * 查询rdb详情.
     *
     * @param id rdb id
     * @return cn.xdf.acdc.devops.core.domain.dto.RdbDTO
     * @date 2022/8/3 4:12 下午
     */
    @GetMapping("/rdbs/{id}")
    public RdbDTO queryRdbInfo(@PathVariable final Long id) {
        return rdbProcessService.getRdb(id);
    }

    /**
     * 修改rdb.
     *
     * @param rdbDTO rdb信息
     * @date 2022/8/3 4:12 下午
     */
    @PatchMapping("/rdbs")
    public void updateRdb(@RequestBody final RdbDTO rdbDTO) {
        rdbProcessService.updateRdb(rdbDTO);
    }

    /**
     * 查询项目下RDB列表.
     *
     * @param id 项目主键
     * @return java.util.List
     * @date 2022/8/2 5:46 下午
     */
    @GetMapping("/projects/{id}/rdbs")
    public PageDTO<RdbDTO> queryRdbsByProject(@PathVariable("id") final Long id) {
        List<RdbDTO> rdbDTOList = projectProcessService.queryRdbsByProject(id);
        return PageDTO.of(rdbDTOList, rdbDTOList.size());
    }
}
