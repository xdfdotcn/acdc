package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.api.util.ApiSecurityUtils;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.ConnectionRequisitionDetailDTO;
import cn.xdf.acdc.devops.core.domain.dto.LoginUserDTO;
import cn.xdf.acdc.devops.core.domain.dto.PageDTO;
import cn.xdf.acdc.devops.core.domain.entity.enumeration.ConnectionState;
import cn.xdf.acdc.devops.core.domain.query.ConnectionQuery;
import cn.xdf.acdc.devops.service.process.connection.ConnectionRequisitionService;
import cn.xdf.acdc.devops.service.process.connection.ConnectionService;
import cn.xdf.acdc.devops.service.process.connection.columnconfiguration.impl.ConnectionColumnConfigurationGeneratorManager;
import cn.xdf.acdc.devops.service.util.UserUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

// CHECKSTYLE:OFF

@RestController
@RequestMapping("api/v1")
public class ConnectionController {

    @Autowired
    private ConnectionRequisitionService connectionRequisitionService;

    @Autowired
    private ConnectionService connectionService;

    @Autowired
    private ConnectionColumnConfigurationGeneratorManager connectionColumnConfigurationGeneratorManager;

    /**
     * Create connection.
     *
     * @param connectionRequisition connectionRequisition
     */
    @PostMapping("/connections")
    public void createConnection(@RequestBody final ConnectionRequisitionDetailDTO connectionRequisition) {
        LoginUserDTO currentUser = ApiSecurityUtils.getCurrentUserDetails();
        connectionRequisitionService.createRequisitionWithAutoSplit(connectionRequisition, currentUser.getDomainAccount());
    }

    /**
     * Edit connection.
     *
     * @param connections connectionRequisition
     */
    @PatchMapping("/connections")
    public void updateConnection(@RequestBody final List<ConnectionDetailDTO> connections) {
        connectionService.batchUpdate(connections);
    }

    /**
     * Get connection detail.
     *
     * @param connectionId connectionId
     * @return ConnectionDetailDTO
     */
    @GetMapping("/connections/{connectionId}")
    public ConnectionDetailDTO getDetailById(
            @PathVariable("connectionId") final Long connectionId
    ) {
        ConnectionDetailDTO connectionDetailDTO = connectionService.getDetailById(connectionId);
        return connectionDetailDTO;
    }

    /**
     * Delete connection.
     *
     * @param connectionId connectionId
     */
    @DeleteMapping("/connections/{connectionId}")
    public void deleteConnection(@PathVariable("connectionId") final Long connectionId
    ) {
        connectionService.deleteById(connectionId);
    }

    /**
     * Get connections.
     *
     * @param query query
     * @return Page
     */
    @GetMapping("/connections")
    public PageDTO<ConnectionDTO> queryConnection(final ConnectionQuery query) {
        LoginUserDTO currentUser = ApiSecurityUtils.getCurrentUserDetails();

        if (!UserUtil.isAdmin(currentUser)) {
            query.setDomainAccount(currentUser.getDomainAccount());
        }

        Page result = connectionService.pagedQuery(query);
        return PageDTO.of(result.getContent(), result.getTotalElements());
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
        return connectionService.getActualState(connectionId);
    }

    /**
     * Start the connection.
     *
     * @param connectionId connection id
     */
    @PostMapping("/connections/{connectionId}/start")
    public void start(@PathVariable("connectionId") final Long connectionId) {
        connectionService.start(connectionId);
    }

    /**
     * Stop the connection.
     *
     * @param connectionId connection id
     */
    @PostMapping("/connections/{connectionId}/stop")
    public void stop(@PathVariable("connectionId") final Long connectionId) {
        connectionService.stop(connectionId);
    }
}
