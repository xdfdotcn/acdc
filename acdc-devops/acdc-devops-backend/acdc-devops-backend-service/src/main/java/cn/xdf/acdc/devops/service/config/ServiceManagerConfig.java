package cn.xdf.acdc.devops.service.config;
// CHECKSTYLE:OFF

import cn.xdf.acdc.devops.service.process.connection.fieldmapping.FieldMappingService;
import cn.xdf.acdc.devops.service.process.connection.fieldmapping.FieldService;
import cn.xdf.acdc.devops.service.process.connection.fieldmapping.impl.FieldMappingProcessServiceManager;
import cn.xdf.acdc.devops.service.process.connector.SinkConnectorProcessService;
import cn.xdf.acdc.devops.service.process.connector.SourceConnectorProcessService;
import cn.xdf.acdc.devops.service.process.connector.impl.SinkConnectorProcessServiceManager;
import cn.xdf.acdc.devops.service.process.connector.impl.SourceConnectorProcessServiceManager;
import cn.xdf.acdc.devops.service.process.datasystem.DatasetProcessService;
import cn.xdf.acdc.devops.service.process.datasystem.DatasetProcessServiceManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * A cdc service config.
 */
@Configuration
public class ServiceManagerConfig {

    @Bean
    public DatasetProcessServiceManager datasetProcessServiceManager(
        final List<DatasetProcessService> services) {
        return new DatasetProcessServiceManager(services);
    }

    @Bean
    public FieldMappingProcessServiceManager fieldMappingProcessServiceManager(
        final List<FieldService> fieldServices,
        final List<FieldMappingService> fieldMappingServices
    ) {
        return new FieldMappingProcessServiceManager(fieldServices, fieldMappingServices);
    }

    @Bean
    public SourceConnectorProcessServiceManager sourceConnectProcessServiceManager(
        final List<SourceConnectorProcessService> services
    ) {
        return new SourceConnectorProcessServiceManager(services);
    }

    @Bean
    public SinkConnectorProcessServiceManager sinkConnectProcessServiceManager(
        final List<SinkConnectorProcessService> services
    ) {
        return new SinkConnectorProcessServiceManager(services);
    }
}
