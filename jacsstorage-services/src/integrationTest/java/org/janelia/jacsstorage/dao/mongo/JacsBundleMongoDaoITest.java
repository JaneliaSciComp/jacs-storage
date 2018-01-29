package org.janelia.jacsstorage.dao.mongo;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequestBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Base64;
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
        JacsBundle te = persistEntity(testDao, createTestEntity("user:user", "d1", null, "/tmp", 100L, 216, ImmutableMap.of("f1", 1, "f2", "v2")));
        JacsBundle retrievedTe = testDao.findById(te.getId());
        assertThat(retrievedTe.getName(), equalTo(te.getName()));
        assertNotSame(te, retrievedTe);
    }

    @Test
    public void findByNameAndOwnerKey() {
        String testUser = "user:user";
        String testName = "test";
        JacsBundle te = persistEntity(testDao, createTestEntity(testUser, testName, null, "/tmp", 100L, 128, ImmutableMap.of("f1", 1, "f2", "v2")));
        JacsBundle retrievedTe = testDao.findByOwnerKeyAndName(testUser, testName);
        assertThat(retrievedTe.getName(), equalTo(te.getName()));
        assertNotSame(te, retrievedTe);
    }

    @Test
    public void findMatchingDataBundles() {
        String testUser = "user:user";
        String testName = "test";
        JacsStorageVolume storageVolume = persistEntity(testVolumeDao, createTestVolume("testLocation", 1000,"testVol",  "mountPoint", 100L));
        persistEntity(testDao, createTestEntity(testUser, testName, storageVolume.getId(), "/tmp", 100L, 0, ImmutableMap.of("f1", 1, "f2", "v2")));
        ImmutableMap<JacsBundle, Integer> testData = ImmutableMap.of(
                new JacsBundleBuilder().build(), 1,
                new JacsBundleBuilder().storageVolumeId(storageVolume.getId()).build(), 1,
                new JacsBundleBuilder().ownerKey("user:anotherUser").build(), 0
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
        String testHost = "testHost";
        String testUser = "user:user";
        String testName = "test";
        JacsStorageVolume storageVolume = persistEntity(testVolumeDao, createTestVolume(testHost, 1000,"vol", "mountPoint", 100L));
        persistEntity(testDao, createTestEntity(testUser, testName, storageVolume.getId(), "/tmp", 100L, 512, ImmutableMap.of("f1", 1, "f2", "v2")));
        ImmutableMap<JacsBundle, Integer> testData = ImmutableMap.of(
                new JacsBundleBuilder().storageHost(testHost).build(), 1
        );
        testData.forEach((filter, expectedResult) -> {
            assertThat(testDao.findMatchingDataBundles(
                    filter,
                    new PageRequestBuilder().build()
            ).getResultList(), hasSize(expectedResult));
        });
    }

    @Test
    public void countMatchingDataBundlesUsingAggregationOps() {
        String testHost = "testHost";
        String testUser = "user:user";
        String testName = "test";
        JacsStorageVolume storageVolume = persistEntity(testVolumeDao, createTestVolume(testHost, 1000,"vol", "mountPoint", 100L));
        persistEntity(testDao, createTestEntity(testUser, testName, storageVolume.getId(), "/tmp", 100L, 512, ImmutableMap.of("f1", 1, "f2", "v2")));
        ImmutableMap<JacsBundle, Long> testData = ImmutableMap.of(
                new JacsBundleBuilder().storageHost(testHost).build(), 1L
        );
        testData.forEach((filter, expectedResult) -> {
            assertThat(testDao.countMatchingDataBundles(filter), equalTo(expectedResult));
        });
    }

    @Test
    public void updateChecksum() {
        String testUser = "user:user";
        JacsBundle te = persistEntity(testDao, createTestEntity(testUser, "d1", null, "/tmp", 100L, 0, ImmutableMap.of("f1", 1, "f2", "v2")));
        te.setChecksum(createChecksum(256));
        testDao.update(te, ImmutableMap.of("checksum", new SetFieldValueHandler<>(te.getChecksum())));
        JacsBundle retrievedTe = testDao.findById(te.getId());
        assertThat(retrievedTe.getName(), equalTo(te.getName()));
        assertNotSame(te, retrievedTe);
    }

    private JacsStorageVolume createTestVolume(String host, int port, String volumeName, String storageRootDir, Long available) {
        JacsStorageVolume v = new JacsStorageVolume();
        v.setStorageHost(host);
        v.setName(volumeName);
        v.setStorageRootDir(storageRootDir);
        if (StringUtils.isNotBlank(host)) {
            v.setStorageServiceURL("http://" + host);
        }
        v.setAvailableSpaceInBytes(available);
        testVolumes.add(v);
        return v;
    }

    private JacsBundle createTestEntity(String ownerKey, String name, Number storageVolumeId, String path, Long size, int checksumLength, Map<String, Object> metadata) {
        JacsBundle d = new JacsBundle();
        d.setOwnerKey(ownerKey);
        d.setName(name);
        d.setPath(path);
        d.setStorageVolumeId(storageVolumeId);
        d.setUsedSpaceInBytes(size);
        d.setChecksum(createChecksum(checksumLength));
        metadata.forEach(d::addMetadataField);
        testData.add(d);
        return d;
    }

    private String createChecksum(int length) {
        if (length > 0)
            return Base64.getEncoder().encodeToString(RandomUtils.nextBytes(length));
        else
            return null;
    }
}
