package org.janelia.jacsstorage.service.impl;

import jakarta.mail.internet.MimeMessage;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EmailNotificationServiceTest {

    @Test
    public void sendNotification() throws Exception {
        try (MockedConstruction<MimeMessage> mockedMimeMessageConstruction = Mockito.mockConstruction(MimeMessage.class)) {
            EmailNotificationService  emailNotificationService = new EmailNotificationService("no-reply@test.com",
                    "",
                    false,
                    false,
                    "smtpHost",
                    25,
                    "r1@test.com,r2.test.com");
            emailNotificationService.sendNotification("s", "message");
            assertEquals(1, mockedMimeMessageConstruction.constructed().size());
        }
    }

    @Test
    public void disabledSendNotificationBecauseNoSmtpHost() throws Exception {
        try (MockedConstruction<MimeMessage> mockedMimeMessageConstruction = Mockito.mockConstruction(MimeMessage.class)) {
            EmailNotificationService emailNotificationService = new EmailNotificationService("no-reply@test.com",
                    "",
                    false,
                    false,
                    "",
                    25,
                    "r1@test.com,r2.test.com");
            emailNotificationService.sendNotification("s", "message");
            assertEquals(0, mockedMimeMessageConstruction.constructed().size());
        }
    }

    @Test
    public void disabledSendNotificationBecauseNoRecipients() throws Exception {
        try (MockedConstruction<MimeMessage> mockedMimeMessageConstruction = Mockito.mockConstruction(MimeMessage.class)) {
            EmailNotificationService emailNotificationService = new EmailNotificationService("no-reply@test.com",
                    "",
                    false,
                    false,
                    "",
                    25,
                    null);
            emailNotificationService.sendNotification("s", "message");
            assertEquals(0, mockedMimeMessageConstruction.constructed().size());
        }
    }
}
