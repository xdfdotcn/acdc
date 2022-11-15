package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.api.util.ApiSecurityUtils;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionEditDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionInfoDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionRequisitionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.LoginUserDTO;
import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionInfoQuery;
import cn.xdf.acdc.devops.service.process.connection.ConnectionProcessService;
import cn.xdf.acdc.devops.service.process.connection.ConnectionRequisitionProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

// CHECKSTYLE:OFF

@RestController
@RequestMapping("api/v1")
public class ConnectionController {

    @Autowired
    private ConnectionRequisitionProcessService connectionRequisitionProcessService;

    @Autowired
    private ConnectionProcessService connectionProcessService;

    /**
     * Create connection.
     *
     * @param connectionRequisition connectionRequisition
     */
    @PostMapping("/connections")
    public void createConnection(@RequestBody final ConnectionRequisitionDetailDTO connectionRequisition) {
        LoginUserDTO currentUser = ApiSecurityUtils.getCurrentUserDetails();
        connectionRequisitionProcessService.bulkCreateRequisitionWithAutoSplit(connectionRequisition, currentUser.getDomainAccount());
    }

    /**
     * Edit connection.
     *
     * @param connections connectionRequisition
     */
    @PatchMapping("/connections")
    public void editConnection(@RequestBody final List<ConnectionEditDTO> connections) {
        connectionProcessService.bulkEditConnection(connections);
    }

    /**
     * Get connection detail.
     *
     * @param connectionId connectionId
     * @return ConnectionDetailDTO
     */
    @GetMapping("/connections/{connectionId}")
    public ConnectionDetailDTO editConnection(
            @PathVariable("connectionId") final Long connectionId
    ) {
        return connectionProcessService.getConnectionDetail(connectionId);
    }

    /**
     * Delete connection.
     *
     * @param connectionId connectionId
     */
    @DeleteMapping("/connections/{connectionId}")
    public void deleteConnection(@PathVariable("connectionId") final Long connectionId
    ) {
        connectionProcessService.deleteConnection(connectionId);
    }

    /**
     * Get connections.
     *
     * @param query query
     * @return Page
     */
    @GetMapping("/connections")
    public PageDTO<ConnectionInfoDTO> queryConnection(final ConnectionInfoQuery query) {
        LoginUserDTO currentUser = ApiSecurityUtils.getCurrentUserDetails();
        Page result = connectionProcessService.detailPagingQuery(query.getSinkDataSystemType(), query, currentUser.getDomainAccount());
        return PageDTO.of(result.getContent(), result.getTotalElements());
    }

    /**
     * Edit connection status.
     *
     * @param connectionId connectionId
     * @param state        state
     */
    @PutMapping("/connections/{connectionId}/desiredStatus")
    public void editConnectorStatus(
            @PathVariable("connectionId") final Long connectionId,
            final ConnectionState state) {
        connectionProcessService.editDesiredState(connectionId, state);
    }

    /**
     * Get connection actual status.
     *
     * @param connectionId connectionId
     * @return state
     */
    @GetMapping("/connections/{connectionId}/actualStatus")
    public ConnectionState getConnectionActualStatus(
            @PathVariable("connectionId") final Long connectionId
    ) {
        return connectionProcessService.getActualState(connectionId);
    }
}
