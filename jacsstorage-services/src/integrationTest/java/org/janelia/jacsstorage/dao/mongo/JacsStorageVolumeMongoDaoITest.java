package org.janelia.jacsstorage.dao.mongo;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
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
        JacsStorageVolume te = persistEntity(testDao, createTestEntity("127.0.0.1", 100, "testVol", "/tmp", "/tmp", 100L));
        te.setVolumePermissions(new HashSet<>(EnumSet.of(JacsStoragePermission.READ, JacsStoragePermission.WRITE)));
        JacsStorageVolume retrievedTe = testDao.findById(te.getId());
        assertThat(retrievedTe.getName(), equalTo(te.getName()));
        assertNotSame(te, retrievedTe);
    }

    @Test
    public void updateTestEntity() {
        JacsStorageVolume te = persistEntity(testDao, createTestEntity("127.0.0.1", 100, "testVol", "/tmp", "/tmp",100L));
        Set<JacsStoragePermission> volumePermissions = EnumSet.of(JacsStoragePermission.READ, JacsStoragePermission.WRITE);
        JacsStorageVolume updatedEntity = testDao.update(te.getId(), ImmutableMap.of("volumePermissions", new SetFieldValueHandler<>(volumePermissions)));
        JacsStorageVolume retrievedTe = testDao.findById(te.getId());
        assertThat(retrievedTe.getName(), equalTo(updatedEntity.getName()));
        assertThat(retrievedTe.getVolumePermissions(), equalTo(volumePermissions));
        assertThat(updatedEntity.getVolumePermissions(), equalTo(volumePermissions));
    }

    @Test
    public void saveTestEntityWithNoHost() {
        JacsStorageVolume te = persistEntity(testDao, createTestEntity(null, 0, "testVol", "/tmp", "/tmp",100L));
        JacsStorageVolume retrievedTe = testDao.findById(te.getId());
        assertThat(retrievedTe.getName(), equalTo(te.getName()));
        assertNull(retrievedTe.getStorageAgentId());
        assertNotSame(te, retrievedTe);
    }

    @Test
    public void searchMissingVolumeByLocationAndCheckIfCreated() {
        class TestInputData {
            private final String testHost;
            private final String testName;

            private TestInputData(String testHost, String testName) {
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
            JacsStorageVolume justCreated = testDao.createStorageVolumeIfNotFound(td.testName, td.testHost);
            assertNotNull(justCreated);
            testData.add(justCreated);
            JacsStorageVolume retrievedTe = testDao.findById(justCreated.getId());
            assertNotNull(retrievedTe);
            assertThat(retrievedTe.getName(), equalTo(justCreated.getName()));
            assertNotSame(justCreated, retrievedTe);
            JacsStorageVolume again = testDao.createStorageVolumeIfNotFound(td.testName, td.testHost);
            assertNotNull(again);
            assertNotSame(justCreated, again);
            assertEquals(justCreated.getId(), again.getId());
            assertEquals(justCreated.getCreated(), again.getCreated());
            assertThat(again.getModified(), greaterThan(justCreated.getModified()));
            if (td.testHost == null) {
                assertNull(justCreated.getStorageAgentId());
                assertNull(retrievedTe.getStorageAgentId());
            } else {
                assertThat(justCreated.getStorageAgentId(), equalTo(td.testHost));
                assertThat(retrievedTe.getStorageAgentId(), equalTo(justCreated.getStorageAgentId()));
            }
        }
    }

    @Test
    public void searchExistingVolumeByLocationAndCheckNoNewOneIsCreated() {
        String testHost = "127.0.0.1";
        String testVolumeName = "testVol";
        JacsStorageVolume te = persistEntity(testDao, createTestEntity(testHost, 100, testVolumeName, "/tmp", "/tmp", 100L));
        JacsStorageVolume existingVolume = testDao.createStorageVolumeIfNotFound(testVolumeName, testHost);
        assertNotNull(existingVolume);
        assertNotSame(te, existingVolume);
        PageResult<JacsStorageVolume> allVolumes = testDao.findAll(new PageRequest());
        assertThat(allVolumes.getResultList().stream().filter(v -> testHost.equals(v.getStorageAgentId()) && testVolumeName.equals(v.getName())).count(), equalTo(1L));
    }

    @Test
    public void searchForMatchingVolumes() {
        // shared volumes
        persistEntity(testDao, createTestEntity(null, 0, "sv1", "/sv1/folder", "/p1",10L));
        persistEntity(testDao, createTestEntity(null, 0, "sv2", "/sv2/${username}", "/p2",20L));
        persistEntity(testDao, createTestEntity(null, 0, "sv3", "/sv3", "/p3",30L));
        // local volumes
        persistEntity(testDao, createTestEntity("h1", 10, "v1", "/v1", "/lp1",10L));
        persistEntity(testDao, createTestEntity("h2", 10, "v2", "/v2", "/lp2",20L));
        persistEntity(testDao, createTestEntity("h3", 10, "v3", "/v3", "/lp3",30L));
        Map<StorageQuery, String[]> queriesWithExpectedResults =
                ImmutableMap.<StorageQuery, String[]>builder()
                        .put(new StorageQuery().setShared(true), // local doesn't matter if shared is true
                                new String[]{"sv1", "sv2", "sv3"})
                        .put(new StorageQuery().setLocalToAnyAgent(true),
                                new String[]{"v1", "v2", "v3"})
                        .put(new StorageQuery().addStorageAgentId("h1:10").addStorageAgentId("h3:10"),
                                new String[]{"v1", "v3"})
                        .put(new StorageQuery().setDataStoragePath("/sv1/folder"),
                                new String[]{"sv1"})
                        .put(new StorageQuery().setDataStoragePath("/sv1/folder/subfolder"),
                                new String[]{"sv1"})
                        .put(new StorageQuery().setDataStoragePath("/p1/subfolder"),
                                new String[]{"sv1"})
                        .put(new StorageQuery().setDataStoragePath("/sv1/myfolder/has/this/data"),
                                new String[]{})
                        .put(new StorageQuery().setDataStoragePath("/sv2/myusername/has/this/data"),
                                new String[]{"sv2"})
                        .put(new StorageQuery().setDataStoragePath("/sv2/myusername"),
                                new String[]{"sv2"})
                        .put(new StorageQuery().setDataStoragePath("/p2/mydata"),
                                new String[]{"sv2"})
                        .put(new StorageQuery().setDataStoragePath("/sv2"),
                                new String[]{})
                        .put(new StorageQuery().setDataStoragePath("/nonmatch"),
                                new String[]{})
                        .build()
                ;
        queriesWithExpectedResults.forEach((q, volumeNames) -> {
            long count = testDao.countMatchingVolumes(q);
            List<JacsStorageVolume> volumes = testDao.findMatchingVolumes(q, new PageRequest()).getResultList();
            assertThat(q.toString(), count, equalTo((long) volumeNames.length));
            if (volumeNames.length > 0) {
                assertThat(q.toString(), volumes.stream().map(sv -> sv.getName()).collect(Collectors.toList()), containsInAnyOrder(volumeNames));
            }
        });
    }

    @Test
    public void searchForMatchingVolumesWithPageOffset() {
        persistEntity(testDao,
                createTestEntity(
                        "h1",
                        10,
                        "v1",
                        "/v1",
                        "/p1",
                        10L));
        persistEntity(testDao,
                createTestEntity(
                        "" +
                        "h2",
                        10,
                        "v2",
                        "/v2",
                        "/p2",
                        20L));
        persistEntity(testDao,
                createTestEntity(
                        "h3",
                        10,
                        "v3",
                        "/v3",
                        "/p3",
                        30L));
        StorageQuery storageQuery = new StorageQuery().setLocalToAnyAgent(true);
        // keep in mind the results are ordered by virtual path descending
        Map<Integer, String[]> offsetsWithExpectedResults =
                ImmutableMap.of(
                        0, new String[]{"v3"},
                        1, new String[]{"v2"},
                        2, new String[]{"v1"},
                        3, new String[]{}
                );
        offsetsWithExpectedResults.forEach((offset, volumeNames) -> {
            PageRequest pageRequest = new PageRequest();
            pageRequest.setFirstPageOffset(offset);
            pageRequest.setPageSize(1);
            List<JacsStorageVolume> volumes = testDao.findMatchingVolumes(
                    storageQuery,
                    pageRequest).getResultList();
            assertThat(volumes.stream().map(sv -> sv.getName()).collect(Collectors.toList()), containsInAnyOrder(volumeNames));
        });
    }

    private JacsStorageVolume createTestEntity(String host, int port, String volumeName,
                                               String storageRootTemplate,
                                               String storageVirtualPath,
                                               Long available) {
        JacsStorageVolume v = new JacsStorageVolume();
        if (host != null) v.setStorageAgentId(host + ":" + port);
        v.setName(volumeName);
        v.setStorageRootTemplate(storageRootTemplate);
        v.setStorageVirtualPath(storageVirtualPath);
        if (StringUtils.isNotBlank(host)) {
            v.setStorageServiceURL("http://" + host);
        } else {
            v.setShared(true);
        }
        v.setAvailableSpaceInBytes(available);
        v.setActiveFlag(true);
        testData.add(v);
        return v;
    }

}
