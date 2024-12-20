package org.janelia.jacsstorage.rest;

import java.io.IOException;
import java.util.Set;

import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.Response;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.app.JAXMasterStorageApp;
import org.janelia.jacsstorage.model.jacsstorage.UsageData;
import org.janelia.jacsstorage.service.StorageUsageManager;
import org.janelia.jacsstorage.testrest.AbstractCdiInjectedResourceTest;
import org.janelia.jacsstorage.testrest.TestMasterStorageDependenciesProducer;
import org.janelia.jacsstorage.testrest.TestResourceBinder;
import org.junit.Test;
import org.mockito.Mockito;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

public class MasterStorageQuotaResourceTest extends AbstractCdiInjectedResourceTest {

    private TestMasterStorageDependenciesProducer dependenciesProducer = new TestMasterStorageDependenciesProducer();

    @Override
    protected JAXMasterStorageApp configure() {
        return new JAXMasterStorageApp() {
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
                TestMasterStorageDependenciesProducer.class
        };
    }

    @Test
    public void retrieveSubjectQuotaForVolumeName() throws IOException {
        StorageUsageManager storageUsageManager = dependenciesProducer.getStorageUsageManager();
        UsageData testUsageData = new UsageData("test", "200", "400", "100", .7, .9, null);
        Mockito.when(storageUsageManager.getUsageByVolumeName(anyString()))
                .then(invocation -> {
                    return ImmutableList.of(testUsageData);
                });
        Mockito.when(storageUsageManager.getUsageByVolumeNameForUser(anyString(), anyString()))
                .then(invocation -> testUsageData);

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
            WebTarget reportWt = target()
                    .path("storage/quota")
                    .path(td.testVolumeName)
                    .path("report");
            if (StringUtils.isNotBlank(td.testSubject)) {
                reportWt = reportWt.queryParam("subjectName", td.testSubject);
            }
            Response testQuotaResponse = reportWt.request().get();
            assertEquals("Volume: " + td.testVolumeName + ", subject: " + td.testSubject, td.expectedStatus, testQuotaResponse.getStatus());
            String usageDataResponse = testQuotaResponse.readEntity(String.class);
            assertThat(usageDataResponse, equalTo(dependenciesProducer.getObjectMapper().writeValueAsString(ImmutableList.of(testUsageData))));

            WebTarget statusWt = target()
                    .path("storage/quota")
                    .path(td.testVolumeName)
                    .path("status");
            if (StringUtils.isNotBlank(td.testSubject)) {
                statusWt = statusWt.queryParam("subjectName", td.testSubject);
            }
            Response testStatusResponse = statusWt.request().get();
            assertEquals("Volume: " + td.testVolumeName + ", subject: " + td.testSubject, td.expectedStatus, testStatusResponse.getStatus());
            String usageStatusResponse = testStatusResponse.readEntity(String.class);
            assertThat(usageStatusResponse, equalTo(dependenciesProducer.getObjectMapper().writeValueAsString(ImmutableList.of(testUsageData))));
        }
    }

    @Test
    public void retrieveSubjectQuotaForVolumeId() throws IOException {
        StorageUsageManager storageUsageManager = dependenciesProducer.getStorageUsageManager();
        UsageData testUsageData = new UsageData("test", "200", "400", "100", .7, .9, null);
        Mockito.when(storageUsageManager.getUsageByVolumeId(any(Number.class)))
                .then(invocation -> {
                    return ImmutableList.of(testUsageData);
                });
        Mockito.when(storageUsageManager.getUsageByVolumeIdForUser(any(Number.class), anyString()))
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
            WebTarget reportWt = target()
                    .path("storage/volume_quota")
                    .path(td.testVolumeId.toString())
                    .path("report");
            if (StringUtils.isNotBlank(td.testSubject)) {
                reportWt = reportWt.queryParam("subjectName", td.testSubject);
            }
            Response testResponse = reportWt.request().get();
            assertEquals("VolumeID: " + td.testVolumeId + ", subject: " + td.testSubject, td.expectedStatus, testResponse.getStatus());
            String usageDataResponse = testResponse.readEntity(String.class);
            assertThat(usageDataResponse, equalTo(dependenciesProducer.getObjectMapper().writeValueAsString(ImmutableList.of(testUsageData))));
        }
    }

    @Test
    public void retrieveSubjectQuotaForDataPath() throws IOException {
        StorageUsageManager storageUsageManager = dependenciesProducer.getStorageUsageManager();
        UsageData testUsageData = new UsageData("test", "200", "400", "100", .7, .9, "jacs");
        Mockito.when(storageUsageManager.getUsageByStoragePath(anyString()))
                .then(invocation -> ImmutableList.of(testUsageData));
        Mockito.when(storageUsageManager.getUsageByStoragePathForUser(anyString(), anyString()))
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
            WebTarget reportWt = target()
                    .path("storage/path_quota")
                    .path(td.testPath)
                    .path("report");
            if (StringUtils.isNotBlank(td.testSubject)) {
                reportWt = reportWt.queryParam("subjectName", td.testSubject);
            }
            Response testResponse = reportWt.request().get();
            assertEquals("Path: " + td.testPath + ", subject: " + td.testSubject, td.expectedStatus, testResponse.getStatus());
            String usageDataResponse = testResponse.readEntity(String.class);
            assertThat(usageDataResponse, equalTo(dependenciesProducer.getObjectMapper().writeValueAsString(ImmutableList.of(testUsageData))));
        }
    }

}
