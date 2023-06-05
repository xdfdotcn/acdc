package cn.xdf.acdc.devops.service.process.tool.command;

import cn.xdf.acdc.devops.core.constant.SystemConstant.Symbol;
import cn.xdf.acdc.devops.service.error.exceptions.ClientErrorException;
import cn.xdf.acdc.devops.service.utility.i18n.I18nKey;
import cn.xdf.acdc.devops.service.utility.i18n.I18nService;
import com.google.common.base.CaseFormat;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;

/**
 * Role (Invoker).
 */
@Component
public class CommandInvoker implements ApplicationContextAware {
    
    @Autowired
    private I18nService i18n;
    
    private ApplicationContext applicationContext;
    
    /**
     * Execute a command.
     *
     * @param commandEntity command parameter entity class
     * @return command execution result
     */
    public Map<String, Object> executeCommand(final Object commandEntity) {
        Map result = getCommandInstance(getCommandBeanId(commandEntity), Command.class).execute(commandEntity);
        return result;
    }
    
    /**
     * Injection a applicationContext.
     *
     * @param applicationContext applicationContext
     */
    @Override
    public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
    
    private Command getCommandInstance(final String beanId, final Class<? extends Command> commandClass) {
        return applicationContext.getBean(beanId, commandClass);
    }
    
    private String getCommandBeanId(final Object commandEntity) {
        if (Objects.isNull(commandEntity)) {
            throw new ClientErrorException(i18n.msg(I18nKey.Command.EXECUTION_FAILED));
        }
        String entityClsName = AopUtils.getTargetClass(commandEntity).getName();
        String commandClsName = entityClsName.split("\\" + Symbol.DOLLAR)[0];
        String commandClsId = commandClsName.substring(commandClsName.lastIndexOf(Symbol.DOT) + 1);
        return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, commandClsId);
    }
}
