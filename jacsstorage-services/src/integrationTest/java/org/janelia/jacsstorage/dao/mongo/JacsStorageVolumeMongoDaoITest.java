package org.janelia.jacsstorage.dao.mongo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class JacsStorageVolumeMongoDaoITest extends AbstractMongoDaoITest {

    private List<JacsStorageVolume> testData = new ArrayList<>();
    private JacsStorageVolumeDao testDao;

    @Before
    public void setUp() {
        testDao = new JacsStorageVolumeMongoDao(testMongoDatabase, idGenerator);
    }

    @After
    public void tearDown() {
        // delete the data that was created for testing
        deleteAll(testDao, testData);
    }

    @Test
    public void findByNullId() {
        assertNull(testDao.findById(null));
    }

    @Test
    public void saveTestEntity() {
        JacsStorageVolume te = persistEntity(testDao, createTestEntity("127.0.0.1", 100, "testVol", "/tmp", 100L));
        JacsStorageVolume retrievedTe = testDao.findById(te.getId());
        assertThat(retrievedTe.getName(), equalTo(te.getName()));
        assertNotSame(te, retrievedTe);
    }

    @Test
    public void saveTestEntityWithNoHost() {
        JacsStorageVolume te = persistEntity(testDao, createTestEntity(null, 0, "testVol", "/tmp", 100L));
        JacsStorageVolume retrievedTe = testDao.findById(te.getId());
        assertThat(retrievedTe.getName(), equalTo(te.getName()));
        assertNull(retrievedTe.getStorageHost());
        assertNotSame(te, retrievedTe);
    }

    @Test
    public void searchMissingVolumeByLocationAndCheckIfCreated() {
        class TestInputData {
            final String testHost;
            final String testName;

            public TestInputData(String testHost, String testName) {
                this.testHost = testHost;
                this.testName = testName;
            }
        }
        TestInputData[] testInputData = new TestInputData[] {
                new TestInputData(null, "testVol"),
                new TestInputData("127.0.0.1", "testVol"),
                new TestInputData("127.0.0.2", "testVol"),
                new TestInputData(null, "testVol1"),
                new TestInputData("127.0.0.1", "testVol1"),
                new TestInputData("127.0.0.2", "testVol1")
        };
        for (TestInputData td : testInputData) {
            JacsStorageVolume justCreated = testDao.getStorageByHostAndNameAndCreateIfNotFound(td.testHost, td.testName);
            assertNotNull(justCreated);
            testData.add(justCreated);
            JacsStorageVolume retrievedTe = testDao.findById(justCreated.getId());
            assertNotNull(retrievedTe);
            assertThat(retrievedTe.getName(), equalTo(justCreated.getName()));
            assertNotSame(justCreated, retrievedTe);
            if (td.testHost == null) {
                assertNull(justCreated.getStorageHost());
                assertNull(retrievedTe.getStorageHost());
            } else {
                assertThat(justCreated.getStorageHost(), equalTo(td.testHost));
                assertThat(retrievedTe.getStorageHost(), equalTo(justCreated.getStorageHost()));
            }
        }
    }

    @Test
    public void searchExistingVolumeByLocationAndCheckNoNewOneIsCreated() {
        String testHost = "127.0.0.1";
        String testVolumeName = "testVol";
        JacsStorageVolume te = persistEntity(testDao, createTestEntity(testHost, 100, testVolumeName, "/tmp", 100L));
        JacsStorageVolume existingVolume = testDao.getStorageByHostAndNameAndCreateIfNotFound(testHost, testVolumeName);
        assertNotNull(existingVolume);
        assertNotSame(te, existingVolume);
        PageResult<JacsStorageVolume> allVolumes = testDao.findAll(new PageRequest());
        assertThat(allVolumes.getResultList().stream().filter(v -> testHost.equals(v.getStorageHost()) && testVolumeName.equals(v.getName())).count(), equalTo(1L));
    }

    @Test
    public void searchForMatchingVolumes() {
        List<JacsStorageVolume> sharedVolumes = ImmutableList.of(
                persistEntity(testDao, createTestEntity(null, 0, "sv1", "/sv1", 10L)),
                persistEntity(testDao, createTestEntity(null, 0, "sv2", "/sv2", 20L)),
                persistEntity(testDao, createTestEntity(null, 0, "sv3", "/sv3", 30L))
        );
        List<JacsStorageVolume> localVolumes = ImmutableList.of(
                persistEntity(testDao, createTestEntity("h1", 10, "v1", "/v1", 10L)),
                persistEntity(testDao, createTestEntity("h2", 10, "v2", "/v2", 20L)),
                persistEntity(testDao, createTestEntity("h3", 10, "v3", "/v3", 30L))
        );
        Map<StorageQuery, String[]> queriesWithExpectedResults =
                ImmutableMap.of(
                        new StorageQuery().setShared(true).setLocalToAnyHost(true), // local doesn't matter if shared is true
                        new String[]{"sv1", "sv2", "sv3"},
                        new StorageQuery().setLocalToAnyHost(true),
                        new String[]{"v1", "v2", "v3"}
                );
        queriesWithExpectedResults.forEach((q, volumeNames) -> {
            long count = testDao.countMatchingVolumes(q);
            List<JacsStorageVolume> volumes = testDao.findMatchingVolumes(q, new PageRequest()).getResultList();
            assertThat(count, equalTo((long) volumeNames.length));
            assertThat(volumes.stream().map(sv -> sv.getName()).collect(Collectors.toList()), containsInAnyOrder(volumeNames));
        });
    }

    private JacsStorageVolume createTestEntity(String host, int port, String volumeName, String volumePath, Long available) {
        JacsStorageVolume v = new JacsStorageVolume();
        v.setStorageHost(host);
        v.setName(volumeName);
        v.setVolumePath(volumePath);
        if (StringUtils.isNotBlank(host)) {
            v.setStorageServiceURL("http://" + host);
            v.setStorageServiceTCPPortNo(port);
        } else {
            v.setShared(true);
        }
        v.setAvailableSpaceInBytes(available);
        testData.add(v);
        return v;
    }

}
