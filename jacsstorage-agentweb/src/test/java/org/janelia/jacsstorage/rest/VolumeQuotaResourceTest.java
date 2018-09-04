package org.janelia.jacsstorage.rest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.app.JAXAgentStorageApp;
import org.janelia.jacsstorage.model.jacsstorage.UsageData;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.service.StorageUsageManager;
import org.janelia.jacsstorage.testrest.AbstractCdiInjectedResourceTest;
import org.janelia.jacsstorage.testrest.TestAgentStorageDependenciesProducer;
import org.janelia.jacsstorage.testrest.TestResourceBinder;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;

public class VolumeQuotaResourceTest extends AbstractCdiInjectedResourceTest {

    private TestAgentStorageDependenciesProducer dependenciesProducer = new TestAgentStorageDependenciesProducer();

    @Override
    protected JAXAgentStorageApp configure() {
        return new JAXAgentStorageApp() {
            @Override
            protected Set<Class<?>> getAppClasses() {
                return ImmutableSet.<Class<?>>builder()
                        .addAll(super.getAppClasses())
                        .build()
                ;
            }

            @Override
            public Set<Object> getSingletons() {
                return ImmutableSet.builder()
                        .addAll(super.getSingletons())
                        .add(new TestResourceBinder(dependenciesProducer))
                        .build()
                        ;
            }
        };
    }

    @Override
    protected Class<?>[] getTestBeanProviders() {
        return new Class<?>[] {
                TestAgentStorageDependenciesProducer.class
        };
    }

    @Test
    public void retrieveSubjectQuotaForVolumeName() throws IOException {
        StorageUsageManager storageUsageManager = dependenciesProducer.getStorageUsageManager();
        UsageData testUsageData = new UsageData("test", "200", "400", "100", .7, .9);
        Mockito.when(storageUsageManager.getUsageByVolumeName(anyString(), nullable(JacsCredentials.class)))
                .then(invocation -> {
                    return ImmutableList.of(testUsageData);
                });
        Mockito.when(storageUsageManager.getUsageByVolumeNameForUser(anyString(), anyString(), nullable(JacsCredentials.class)))
                .then(invocation -> {
                    return testUsageData;
                });

        class TestData {
            private final String testVolumeName;
            private final String testSubject;
            private final int expectedStatus;

            public TestData(String testVolumeName, String testSubject, int expectedStatus) {
                this.testVolumeName = testVolumeName;
                this.testSubject = testSubject;
                this.expectedStatus = expectedStatus;
            }
        }
        TestData[] testData = new TestData[] {
                new TestData("v1", "s1", 200),
                new TestData("v1", "s1", 200),
                new TestData("v1", "s1", 200),
                new TestData("v1", "", 200)
        };
        for (TestData td : testData) {
            Response testQuotaResponse = target()
                    .path(Constants.AGENTSTORAGE_URI_PATH)
                    .path("quota")
                    .path(td.testVolumeName)
                    .path("report")
                    .path(StringUtils.defaultIfBlank(td.testSubject,"/"))
                    .request()
                    .get();
            assertEquals("Volume: " + td.testVolumeName + ", subject: " + td.testSubject, td.expectedStatus, testQuotaResponse.getStatus());
            String usageDataResponse = testQuotaResponse.readEntity(String.class);
            assertThat(usageDataResponse, equalTo(dependenciesProducer.getObjectMapper().writeValueAsString(ImmutableList.of(testUsageData))));

            Response testStatusResponse = target()
                    .path(Constants.AGENTSTORAGE_URI_PATH)
                    .path("quota")
                    .path(td.testVolumeName)
                    .path("status")
                    .path(StringUtils.defaultIfBlank(td.testSubject,"/"))
                    .request()
                    .get();
            assertEquals("Volume: " + td.testVolumeName + ", subject: " + td.testSubject, td.expectedStatus, testStatusResponse.getStatus());
            String usageStatusResponse = testStatusResponse.readEntity(String.class);
            assertThat(usageStatusResponse, equalTo(dependenciesProducer.getObjectMapper().writeValueAsString(ImmutableList.of(testUsageData))));
        }
    }

    @Test
    public void retrieveSubjectQuotaForVolumeId() throws IOException {
        StorageUsageManager storageUsageManager = dependenciesProducer.getStorageUsageManager();
        UsageData testUsageData = new UsageData("test", "200", "400", "100", .7, .9);
        Mockito.when(storageUsageManager.getUsageByVolumeId(any(Number.class), nullable(JacsCredentials.class)))
                .then(invocation -> {
                    return ImmutableList.of(testUsageData);
                });
        Mockito.when(storageUsageManager.getUsageByVolumeIdForUser(any(Number.class), anyString(), nullable(JacsCredentials.class)))
                .then(invocation -> {
                    return testUsageData;
                });

        class TestData {
            private final Number testVolumeId;
            private final String testSubject;
            private final int expectedStatus;

            public TestData(Number testVolumeId, String testSubject, int expectedStatus) {
                this.testVolumeId = testVolumeId;
                this.testSubject = testSubject;
                this.expectedStatus = expectedStatus;
            }
        }
        TestData[] testData = new TestData[] {
                new TestData(1, "s1", 200),
                new TestData(1, "s1", 200),
                new TestData(1, "s1", 200),
                new TestData(1, "", 200)
        };
        for (TestData td : testData) {
            Response testResponse = target()
                    .path(Constants.AGENTSTORAGE_URI_PATH)
                    .path("volume_quota")
                    .path(td.testVolumeId.toString())
                    .path("report")
                    .path(StringUtils.defaultIfBlank(td.testSubject,"/"))
                    .request()
                    .get();
            assertEquals("VolumeID: " + td.testVolumeId + ", subject: " + td.testSubject, td.expectedStatus, testResponse.getStatus());
            String usageDataResponse = testResponse.readEntity(String.class);
            assertThat(usageDataResponse, equalTo(dependenciesProducer.getObjectMapper().writeValueAsString(ImmutableList.of(testUsageData))));
        }
    }

    @Test
    public void retrieveSubjectQuotaForDataPath() throws IOException {
        StorageUsageManager storageUsageManager = dependenciesProducer.getStorageUsageManager();
        UsageData testUsageData = new UsageData("test", "200", "400", "100", .7, .9);
        Mockito.when(storageUsageManager.getUsageByStoragePath(anyString(), nullable(JacsCredentials.class)))
                .then(invocation -> {
                    return ImmutableList.of(testUsageData);
                });
        Mockito.when(storageUsageManager.getUsageByStoragePathForUser(anyString(), anyString(), nullable(JacsCredentials.class)))
                .then(invocation -> {
                    return testUsageData;
                });

        class TestData {
            private final String testPath;
            private final String testSubject;
            private final int expectedStatus;

            public TestData(String testPath, String testSubject, int expectedStatus) {
                this.testPath = testPath;
                this.testSubject = testSubject;
                this.expectedStatus = expectedStatus;
            }
        }
        TestData[] testData = new TestData[] {
                new TestData("volPrefix", "s1", 200),
                new TestData("volPrefix/testPath", "s1", 200),
                new TestData("/volPrefix/testPath", "s1", 200),
                new TestData("volPrefix/testPath", "", 200)
        };
        for (TestData td : testData) {
            Response testResponse = target()
                    .path(Constants.AGENTSTORAGE_URI_PATH)
                    .path("path_quota")
                    .path(td.testPath)
                    .path("report")
                    .path(StringUtils.defaultIfBlank(td.testSubject,"/"))
                    .request()
                    .get();
            assertEquals("Path: " + td.testPath + ", subject: " + td.testSubject, td.expectedStatus, testResponse.getStatus());
            String usageDataResponse = testResponse.readEntity(String.class);
            assertThat(usageDataResponse, equalTo(dependenciesProducer.getObjectMapper().writeValueAsString(ImmutableList.of(testUsageData))));
        }
    }

}
