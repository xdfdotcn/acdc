package cn.xdf.acdc.devops.service.utility.mail;

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
        List<DomainUser> to,
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
        List<DomainUser> to,
        List<DomainUser> cc,
        EmailTemplate template,
        Object content);
}
