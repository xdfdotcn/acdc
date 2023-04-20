package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.api.util.ApiSecurityUtils;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionRequisitionDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionRequisitionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.LoginUserDTO;
import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.dto.approve.ApproveDTO;
import cn.xdf.acdc.devops.core.domain.query.ConnectionRequisitionQuery;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connection.ConnectionRequisitionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("api/v1")
public class ConnectionRequisitionController {

    @Autowired
    private ConnectionRequisitionService connectionRequisitionService;

    @Autowired
    private ConnectionService connectionService;

    /**
     * Get connection by id.
     *
     * @param connectionRequisitionId connectionRequisitionId
     * @return ConnectionRequisitionDetailDTO
     */
    @GetMapping("/connection-requisition/{connectionRequisitionId}")
    public ConnectionRequisitionDetailDTO getConnectionRequisition(@PathVariable final Long connectionRequisitionId) {
        return connectionRequisitionService.getDetailById(connectionRequisitionId);
    }

    /**
     * Paging query requisition.
     *
     * @param query query
     * @return requisition page list
     */
    @GetMapping("/connections/{connectionId}/connection-requisitions")
    public PageDTO<ConnectionRequisitionDTO> queryRequisition(final ConnectionRequisitionQuery query) {
        List<ConnectionRequisitionDTO> requisitions = connectionRequisitionService.query(query);
        return PageDTO.of(requisitions, requisitions.size());
    }

    /**
     * Approve connection requisition.
     *
     * @param connectionRequisitionId connectionRequisitionId
     * @param approveDTO              approve info
     */
    @PostMapping("/connection-requisitions/{connectionRequisitionId}/approve")
    public void approve(
            @PathVariable final Long connectionRequisitionId,
            @RequestBody final ApproveDTO approveDTO
    ) {
        LoginUserDTO currentUser = ApiSecurityUtils.getCurrentUserDetails();
        connectionRequisitionService.approve(connectionRequisitionId, approveDTO, currentUser.getDomainAccount());
    }
}
