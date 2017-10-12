package org.janelia.jacsstorage.dao.mongo;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequestBuilder;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class JacsBundleMongoDaoITest extends AbstractMongoDaoITest {

    private List<JacsStorageVolume> testVolumes = new ArrayList<>();
    private List<JacsBundle> testData = new ArrayList<>();
    private JacsStorageVolumeDao testVolumeDao;
    private JacsBundleDao testDao;

    @Before
    public void setUp() {
        testVolumeDao = new JacsStorageVolumeMongoDao(testMongoDatabase, idGenerator);
        testDao = new JacsBundleMongoDao(testMongoDatabase, idGenerator);
    }

    @After
    public void tearDown() {
        // delete the data that was created for testing
        deleteAll(testDao, testData);
        deleteAll(testVolumeDao, testVolumes);
    }

    @Test
    public void findByNullId() {
        assertNull(testDao.findById(null));
    }

    @Test
    public void saveTestEntity() {
        JacsBundle te = persistEntity(testDao, createTestEntity("user", "d1", null, "/tmp", 100L, ImmutableMap.of("f1", 1, "f2", "v2")));
        JacsBundle retrievedTe = testDao.findById(te.getId());
        assertThat(retrievedTe.getName(), equalTo(te.getName()));
        assertNotSame(te, retrievedTe);
    }

    @Test
    public void findByNameAndOwner() {
        String testUser = "user";
        String testName = "test";
        JacsBundle te = persistEntity(testDao, createTestEntity(testUser, testName, null, "/tmp", 100L, ImmutableMap.of("f1", 1, "f2", "v2")));
        JacsBundle retrievedTe = testDao.findByOwnerAndName(testUser, testName);
        assertThat(retrievedTe.getName(), equalTo(te.getName()));
        assertNotSame(te, retrievedTe);
    }

    @Test
    public void findMatchingDataBundles() {
        String testUser = "user";
        String testName = "test";
        JacsStorageVolume storageVolume = persistEntity(testVolumeDao, createTestVolume("testVolume", "testLocation", "test:1000",  "mountPoint", 100L, 100L));
        persistEntity(testDao, createTestEntity(testUser, testName, storageVolume.getId(), "/tmp", 100L, ImmutableMap.of("f1", 1, "f2", "v2")));
        ImmutableMap<JacsBundle, Integer> testData = ImmutableMap.of(
                new JacsBundleBuilder().build(), 1,
                new JacsBundleBuilder().storageVolumeId(storageVolume.getId()).build(), 1,
                new JacsBundleBuilder().owner("anotherUser").build(), 0
        );
        testData.forEach((filter, expectedResult) -> {
            assertThat(testDao.findMatchingDataBundles(
                    filter,
                    new PageRequestBuilder().build()
            ).getResultList(), hasSize(expectedResult));
        });
    }

    @Test
    public void findMatchingDataBundlesUsingAggregationOps() {
        String testUser = "user";
        String testName = "test";
        JacsStorageVolume storageVolume = persistEntity(testVolumeDao, createTestVolume("testVolume", "testLocation", "test:1000","mountPoint", 100L, 100L));
        persistEntity(testDao, createTestEntity(testUser, testName, storageVolume.getId(), "/tmp", 100L, ImmutableMap.of("f1", 1, "f2", "v2")));
        ImmutableMap<JacsBundle, Integer> testData = ImmutableMap.of(
                new JacsBundleBuilder().location("testLocation").build(), 1
        );
        testData.forEach((filter, expectedResult) -> {
            assertThat(testDao.findMatchingDataBundles(
                    filter,
                    new PageRequestBuilder().build()
            ).getResultList(), hasSize(expectedResult));
        });
    }

    private JacsStorageVolume createTestVolume(String name, String location, String mountHostIP, String mountPoint, Long capacity, Long available) {
        JacsStorageVolume v = new JacsStorageVolume();
        v.setName(name);
        v.setLocation(location);
        v.setMountHostIP(mountHostIP);
        v.setMountPoint(mountPoint);
        v.setCapacityInMB(capacity);
        v.setAvailableInMB(available);
        testVolumes.add(v);
        return v;
    }

    private JacsBundle createTestEntity(String owner, String name, Number storageVolumeId, String path, Long size, Map<String, Object> metadata) {
        JacsBundle d = new JacsBundle();
        d.setOwner(owner);
        d.setName(name);
        d.setPath(path);
        d.setStorageVolumeId(storageVolumeId);
        d.setUsedSpaceInKB(size);
        metadata.forEach(d::addMetadataField);
        testData.add(d);
        return d;
    }

}
