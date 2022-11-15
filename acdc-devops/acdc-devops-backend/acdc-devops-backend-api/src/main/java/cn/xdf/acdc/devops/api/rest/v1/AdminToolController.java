package cn.xdf.acdc.devops.api.rest.v1;

import cn.xdf.acdc.devops.service.process.tool.command.ApiConnectCommand;
import cn.xdf.acdc.devops.service.process.tool.command.ApiKafkaClusterCommand;
import cn.xdf.acdc.devops.service.process.tool.command.ApiUserCommand;
import cn.xdf.acdc.devops.service.process.tool.command.CommandInvoker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("api/v1")
@Transactional
public class AdminToolController {

    @Autowired
    private CommandInvoker commandInvoker;

    /**
     * Connect command.
     *
     * @param commandEntity command to bind the entity class
     * @return result map
     */
    @PostMapping("/admin/tools/api-connect")
    public Map<String, Object> executeConnectCommand(final @RequestBody ApiConnectCommand.CommandEntity commandEntity) {
        return commandInvoker.executeCommand(commandEntity);
    }

    /**
     * User command.
     *
     * @param commandEntity command to bind the entity class
     * @return result map
     */
    @PostMapping("/admin/tools/api-user")
    public Map<String, Object> executeUserCommand(final @RequestBody ApiUserCommand.CommandEntity commandEntity) {
        return commandInvoker.executeCommand(commandEntity);
    }

    /**
     * Kafka cluster  command.
     *
     * @param commandEntity command to bind the entity class
     * @return result map
     */
    @PostMapping("/admin/tools/api-kafka-cluster")
    public Map<String, Object> executeKafkaClusterCommand(final @RequestBody ApiKafkaClusterCommand.CommandEntity commandEntity) {
        return commandInvoker.executeCommand(commandEntity);
    }
}
