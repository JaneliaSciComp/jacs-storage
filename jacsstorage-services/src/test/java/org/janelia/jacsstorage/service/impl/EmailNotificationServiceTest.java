package org.janelia.jacsstorage.service.impl;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.MimeMessage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Transport.class, EmailNotificationService.class})
public class EmailNotificationServiceTest {

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(Transport.class);
        MimeMessage message = Mockito.mock(MimeMessage.class);
        PowerMockito.whenNew(MimeMessage.class)
                .withParameterTypes(Session.class)
                .withArguments(any(Session.class))
                .thenReturn(message);
    }

    @Test
    public void sendNotification() throws Exception {
        EmailNotificationService  emailNotificationService = new EmailNotificationService("no-reply@test.com",
                "",
                false,
                false,
                "smtpHost",
                25,
                "r1@test.com,r2.test.com");
        emailNotificationService.sendNotification("s", "message");
        PowerMockito.verifyNew(MimeMessage.class, times(1)).withArguments(any(Session.class));
    }

    @Test
    public void disabledSendNotificationBecauseNoSmtpHost() throws Exception {
        EmailNotificationService  emailNotificationService = new EmailNotificationService("no-reply@test.com",
                "",
                false,
                false,
                "",
                25,
                "r1@test.com,r2.test.com");
        emailNotificationService.sendNotification("s", "message");
        PowerMockito.verifyNew(MimeMessage.class, times(0)).withArguments(any(Session.class));
    }

    @Test
    public void disabledSendNotificationBecauseNoRecipients() throws Exception {
        EmailNotificationService  emailNotificationService = new EmailNotificationService("no-reply@test.com",
                "",
                false,
                false,
                "",
                25,
                null);
        emailNotificationService.sendNotification("s", "message");
        PowerMockito.verifyNew(MimeMessage.class, times(0)).withArguments(any(Session.class));
    }
}
