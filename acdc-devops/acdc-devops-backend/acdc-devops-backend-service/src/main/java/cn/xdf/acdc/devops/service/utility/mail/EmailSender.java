package cn.xdf.acdc.devops.service.utility.mail;

import cn.xdf.acdc.devops.core.domain.dto.DomainUserDTO;
import java.util.List;

/**
 * Inner email sender.
 */
public interface EmailSender {

    /**
     * Send email for inner warning purpose.
     *
     * @param template template
     * @param content content
     */
    void sendInnerWarningEmail(EmailTemplate template, Object content);

    /**
     * Send message.
     *
     * @param to to
     * @param template template
     * @param content content
     */
    default void sendEmail(
        List<DomainUserDTO> to,
        EmailTemplate template,
        Object content) {
        throw new UnsupportedOperationException();
    }

    /**
     * Send message.
     *
     * @param to to
     * @param cc cc
     * @param template template
     * @param content content
     */
    void sendEmail(
        List<DomainUserDTO> to,
        List<DomainUserDTO> cc,
        EmailTemplate template,
        Object content);
}
