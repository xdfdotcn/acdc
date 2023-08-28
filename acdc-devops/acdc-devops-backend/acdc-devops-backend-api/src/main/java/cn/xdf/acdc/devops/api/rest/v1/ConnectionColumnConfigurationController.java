package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.core.domain.dto.ConnectionColumnConfigurationDTO;
import cn.xdf.acdc.devops.service.process.connection.columnconfiguration.impl.ConnectionColumnConfigurationGeneratorManager;
import cn.xdf.acdc.devops.service.util.QueryUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;

@RestController
@RequestMapping("api/v1")
@Transactional
public class ConnectionColumnConfigurationController {

    @Autowired
    private ConnectionColumnConfigurationGeneratorManager connectionColumnConfigurationGeneratorManager;

    /**
     * Generate column configurations.
     *
     * @param sinkDataCollectionId   sink collection id
     * @param sourceDataCollectionId source collection id
     * @return column configurations
     */
    @GetMapping("/connections/column-configurations/generate")
    public List<ConnectionColumnConfigurationDTO> generate(
            final Long sourceDataCollectionId,
            final Long sinkDataCollectionId
    ) {
        if (QueryUtil.isNullId(sourceDataCollectionId)
                || QueryUtil.isNullId(sinkDataCollectionId)
        ) {
            return Collections.EMPTY_LIST;
        }
        return connectionColumnConfigurationGeneratorManager.generateConnectionColumnConfiguration(sourceDataCollectionId, sinkDataCollectionId);
    }

    /**
     * Generate column configurations.
     *
     * @param connectionId connection id
     * @return column configurations
     */
    @GetMapping("/connections/{connectionId}/column-configurations/generate")
    public List<ConnectionColumnConfigurationDTO> generateByConnectionId(
            @PathVariable("connectionId") final Long connectionId
    ) {
        if (QueryUtil.isNullId(connectionId)) {
            return Collections.EMPTY_LIST;
        }
        return connectionColumnConfigurationGeneratorManager.generateConnectionColumnConfiguration(connectionId);
    }
}
