package cn.xdf.acdc.devops.service.utility.i18n;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.MessageSource;
import org.springframework.stereotype.Service;

import java.util.Locale;

@Service
@Slf4j
public class I18nService {
    
    private final MessageSource messageSource;
    
    public I18nService(
            final MessageSource messageSource
    ) {
        this.messageSource = messageSource;
    }
    
    /**
     * 获取国际化之后的信息.
     *
     * @param i18nKey 国际化key
     * @param args 占位符参数
     * @return 国际化之后的信息
     */
    public String msg(final String i18nKey, final Object... args) {
        Locale locale = Locale.getDefault();
        return messageSource.getMessage(i18nKey, args, locale);
    }
}
