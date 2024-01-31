package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.api.util.ApiSecurityUtils;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.DataSystemResourcePermissionRequisitionBatchDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.LoginUserDTO;
import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.dto.widetable.WideTableDTO;
import cn.xdf.acdc.devops.core.domain.dto.widetable.WideTableDetailDTO;
import cn.xdf.acdc.devops.core.domain.query.WideTableQuery;
import cn.xdf.acdc.devops.service.process.widetable.WideTableService;
import cn.xdf.acdc.devops.service.util.UserUtil;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("api/v1")
public class WideTableController {
    
    @Autowired
    private WideTableService wideTableService;
    
    /**
     * Get detail by id.
     *
     * @param wideTableId wide table id
     * @return wide table detail
     */
    @GetMapping("/wide-table/{id}")
    public WideTableDetailDTO getDetailById(@PathVariable("id") final Long wideTableId) {
        return wideTableService.getDetailById(wideTableId);
    }
    
    /**
     * Wide table page query.
     *
     * @param query wide table query
     * @return wide table page
     */
    @GetMapping("/wide-table")
    public PageDTO<WideTableDTO> queryWideTable(final WideTableQuery query) {
        LoginUserDTO currentUser = ApiSecurityUtils.getCurrentUserDetails();
        
        if (!UserUtil.isAdmin(currentUser)) {
            query.setDomainAccount(currentUser.getDomainAccount());
        }
        Page<WideTableDTO> result = wideTableService.pagedQuery(query);
        
        return PageDTO.of(result.getContent(), result.getTotalElements());
    }
    
    /**
     * Get connection by wide table id.
     *
     * @param wideTableId wide table id
     * @return connections
     */
    @GetMapping("/wide-table/{id}/connections")
    public List<ConnectionDTO> getConnectionsByWideTableId(@PathVariable("id") final Long wideTableId) {
        return wideTableService.getConnectionsByWideTableId(wideTableId);
    }
    
    /**
     * Get requisition by wide table id.
     *
     * @param wideTableId wide table id
     * @return requisition
     */
    @GetMapping("/wide-table/{id}/requisition")
    public List<DataSystemResourcePermissionRequisitionBatchDetailDTO> getRequisitionByWideTableId(@PathVariable("id") final Long wideTableId) {
        DataSystemResourcePermissionRequisitionBatchDetailDTO requisition = wideTableService.getRequisitionByWideTableId(wideTableId);
        if (requisition == null) {
            return Lists.newArrayList();
        }
        return Lists.newArrayList(requisition);
    }
    
    /**
     * Enable the wide table.
     *
     * @param wideTableId wide table id
     */
    @PostMapping("/wide-table/{id}/enable")
    public void enable(@PathVariable("id") final Long wideTableId) {
        wideTableService.enable(wideTableId);
    }
    
    /**
     * Disable the wide table.
     *
     * @param wideTableId wide table id
     */
    @PostMapping("/wide-table/{id}/disable")
    public void disable(@PathVariable("id") final Long wideTableId) {
        wideTableService.disable(wideTableId);
    }
    
    /**
     * Create a wide table.
     *
     * @param wideTableDetailDTO wide table detail dto
     * @param beforeCreation is before creation or not
     * @return wide table detail
     */
    @PostMapping("/wide-table")
    public WideTableDetailDTO create(
            @RequestBody final WideTableDetailDTO wideTableDetailDTO,
            final boolean beforeCreation
    ) {
        LoginUserDTO currentUser = ApiSecurityUtils.getCurrentUserDetails();
        wideTableDetailDTO.setUserId(currentUser.getUserid());
        
        if (beforeCreation) {
            return wideTableService.beforeCreation(wideTableDetailDTO);
        }
        
        return wideTableService.create(wideTableDetailDTO);
    }
}
