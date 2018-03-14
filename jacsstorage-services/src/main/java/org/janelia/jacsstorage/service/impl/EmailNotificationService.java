package org.janelia.jacsstorage.service.impl;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.coreutils.PathUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.io.BundleReader;
import org.janelia.jacsstorage.io.BundleWriter;
import org.janelia.jacsstorage.io.DataBundleIOProvider;
import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.NotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

public class EmailNotificationService implements NotificationService {

    private static final Logger LOG = LoggerFactory.getLogger(EmailNotificationService.class);

    private final boolean disabled;
    private final Properties emailProperties = new Properties();
    private final String senderEmail;
    private final String senderPassword;
    private final Boolean authRequired;
    private final String recipients;

    @Inject
    public EmailNotificationService(@PropertyValue(name = "Storage.Email.SenderEmail") String senderEmail,
                                    @PropertyValue(name = "Storage.Email.SenderPassword") String senderPassword,
                                    @PropertyValue(name = "Storage.Email.AuthRequired") Boolean authRequired,
                                    @PropertyValue(name = "Storage.Email.EnableTLS") Boolean enableTLS,
                                    @PropertyValue(name = "Storage.Email.SMTPHost") String smtpHost,
                                    @PropertyValue(name = "Storage.Email.SMTPPort", defaultValue = "25") Integer smtpPort,
                                    @PropertyValue(name = "Storage.Email.Recipients") String recipients) {
        this.senderEmail = senderEmail;
        this.senderPassword = senderPassword;
        this.authRequired = authRequired;
        this.recipients = recipients;
        this.disabled = StringUtils.isBlank(smtpHost) || StringUtils.isBlank(this.recipients);
        emailProperties.put("mail.smtp.auth", authRequired.toString());
        emailProperties.put("mail.smtp.starttls.enable", enableTLS.toString());
        emailProperties.put("mail.smtp.host", smtpHost);
        emailProperties.put("mail.smtp.port", smtpPort == null ? "" : smtpPort.toString());
    }

    @Override
    public void sendNotification(String subject, String textMessage) {
        if (disabled) {
            LOG.info("Sending notification {} to {} is not enabled" +
                            " because either recipients or some session properties are missing: recipients: {}, smtp host: {}",
                    textMessage, subject, recipients, emailProperties.getProperty("mail.smtp.host"));
            return;
        }
        Session session;
        try {
            if (authRequired) {
                session = Session.getInstance(emailProperties,
                        new javax.mail.Authenticator() {
                            protected PasswordAuthentication getPasswordAuthentication() {
                                return new PasswordAuthentication(senderEmail, senderPassword);
                            }
                        });
            } else {
                session = Session.getInstance(emailProperties);
            }
            LOG.debug("Send {} to {}", subject, recipients);
            Message emailMessage = new MimeMessage(session);
            emailMessage.setFrom(new InternetAddress(senderEmail));
            emailMessage.setRecipients(Message.RecipientType.TO, InternetAddress.parse(recipients));
            emailMessage.setSubject(subject);
            emailMessage.setText(textMessage);
            Transport.send(emailMessage);
        } catch (Exception e) {
            LOG.warn("Error sending {} to {} from {} using {}", textMessage, recipients, senderEmail, emailProperties);
        }

    }
}
