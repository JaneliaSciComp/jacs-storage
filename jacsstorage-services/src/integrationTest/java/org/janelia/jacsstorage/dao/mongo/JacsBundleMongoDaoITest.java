package org.janelia.jacsstorage.dao.mongo;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequestBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

public class JacsBundleMongoDaoITest extends AbstractMongoDaoITest {

    private final UniformRandomProvider rng = RandomSource.create(RandomSource.XO_RO_SHI_RO_128_PP);
    private final List<JacsStorageVolume> testVolumes = new ArrayList<>();
    private final List<JacsBundle> testData = new ArrayList<>();
    private JacsStorageVolumeDao testVolumeDao;
    private JacsBundleDao testDao;

    @BeforeEach
    public void setUp() {
        testVolumeDao = new JacsStorageVolumeMongoDao(testMongoDatabase, idGenerator);
        testDao = new JacsBundleMongoDao(testMongoDatabase, idGenerator);
    }

    @AfterEach
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
        JacsBundle te = persistEntity(testDao, createTestEntity("user:user", "d1", null, "/tmp", 100L, 216));
        JacsBundle retrievedTe = testDao.findById(te.getId());
        assertThat(retrievedTe.getName(), equalTo(te.getName()));
        assertNotSame(te, retrievedTe);
    }

    @Test
    public void findByNameAndOwnerKey() {
        String testUser = "user:user";
        String testName = "test";
        JacsBundle te = persistEntity(testDao, createTestEntity(testUser, testName, null, "/tmp", 100L, 128));
        JacsBundle retrievedTe = testDao.findByOwnerKeyAndName(testUser, testName);
        assertThat(retrievedTe.getName(), equalTo(te.getName()));
        assertNotSame(te, retrievedTe);
    }

    @Test
    public void findMatchingDataBundles() {
        String testUser = "user:user";
        String testName = "test";
        JacsStorageVolume storageVolume = persistEntity(testVolumeDao, createTestVolume("testLocation", 1000,"testVol",  "mountPoint", 100L));
        persistEntity(testDao, createTestEntity(testUser, testName, storageVolume.getId(), "/tmp", 100L, 0));
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
        int testPort = 1000;
        String testUser = "user:user";
        String testName = "test";
        String testVolumeRootDir = "/mountPoint";
        JacsStorageVolume storageVolume = persistEntity(testVolumeDao, createTestVolume(testHost, testPort,"vol", testVolumeRootDir, 100L));
        JacsBundle testBundle1 = persistEntity(testDao, createTestEntity(testUser, testName + "1", storageVolume.getId(), "/tmp", 100L, 512));
        JacsBundle testBundle2 = persistEntity(testDao, createTestEntity(testUser, testName + "2", storageVolume.getId(), "/tmp", 100L, 512));
        ImmutableMap.Builder<JacsBundle, Integer> testDataBuilder = ImmutableMap.<JacsBundle, Integer>builder()
                .put(new JacsBundleBuilder()
                        .storageAgentId(testHost + ":" + testPort).build(), 2)
                .put(new JacsBundleBuilder()
                        .path(testVolumeRootDir + "/" + testBundle1.getId().toString()).build(), 1)
                .put(new JacsBundleBuilder()
                        .path(testVolumeRootDir + "/" + testBundle2.getId().toString()).build(), 1)
                .put(new JacsBundleBuilder()
                        .storageAgentId(testHost + ":" + testPort)
                        .name(testName + "1")
                        .path(testVolumeRootDir + "/" + testBundle1.getId().toString()).build(), 1)
                .put(new JacsBundleBuilder()
                        .dataBundleId(testBundle2.getId())
                        .storageAgentId(testHost + ":" + testPort)
                        .name(testName + "2")
                        .path(testVolumeRootDir + "/" + testBundle2.getId().toString()).build(), 1)
                .put(new JacsBundleBuilder()
                        .dataBundleId(testBundle1.getId())
                        .name(testName + "1").build(), 1)
                .put(new JacsBundleBuilder()
                        .storageAgentId(testHost + ":" + testPort)
                        .name(testName + "1")
                        .path(testVolumeRootDir + "/" + testBundle2.getId().toString()).build(), 0)
                ;

        testDataBuilder.build().forEach((filter, expectedResult) -> {
            assertThat(filter.toString(), testDao.findMatchingDataBundles(
                    filter,
                    new PageRequestBuilder().build()
            ).getResultList(), hasSize(expectedResult));
        });
    }

    @Test
    public void countMatchingDataBundlesUsingAggregationOps() {
        String testHost = "testHost";
        int testPort = 1000;
        String testUser = "user:user";
        String testName = "test";
        String testVolumeRootDir = "/mountPoint";
        JacsStorageVolume storageVolume = persistEntity(testVolumeDao, createTestVolume(testHost, testPort,"vol", testVolumeRootDir, 100L));
        JacsBundle testBundle1 = persistEntity(testDao, createTestEntity(testUser, testName + "1", storageVolume.getId(), "/tmp", 100L, 512));
        JacsBundle testBundle2 = persistEntity(testDao, createTestEntity(testUser, testName + "2", storageVolume.getId(), "/tmp", 100L, 512));
        ImmutableMap.Builder<JacsBundle, Long> testDataBuilder = ImmutableMap.<JacsBundle, Long>builder()
                .put(new JacsBundleBuilder()
                        .storageAgentId(testHost + ":" + testPort).build(), 2L)
                .put(new JacsBundleBuilder()
                        .path(testVolumeRootDir + "/" + testBundle1.getId().toString()).build(), 1L)
                .put(new JacsBundleBuilder()
                        .path(testVolumeRootDir + "/" + testBundle2.getId().toString()).build(), 1L)
                .put(new JacsBundleBuilder()
                        .storageAgentId(testHost + ":" + testPort)
                        .name(testName + "1")
                        .path(testVolumeRootDir + "/" + testBundle1.getId().toString()).build(), 1L)
                .put(new JacsBundleBuilder()
                        .storageAgentId(testHost + ":" + testPort)
                        .name(testName + "2")
                        .path(testVolumeRootDir + "/" + testBundle2.getId().toString()).build(), 1L)
                .put(new JacsBundleBuilder()
                        .name(testName + "1").build(), 1L)
                .put(new JacsBundleBuilder()
                        .storageAgentId(testHost + ":" + testPort)
                        .name(testName + "1")
                        .path(testVolumeRootDir + "/" + testBundle2.getId().toString()).build(), 0L);

        testDataBuilder.build().forEach((filter, expectedResult) -> {
            assertThat(testDao.countMatchingDataBundles(filter), equalTo(expectedResult));
        });
    }

    @Test
    public void updateChecksum() {
        String testUser = "user:user";
        JacsBundle te = persistEntity(testDao, createTestEntity(testUser, "d1", null, "/tmp", 100L, 0));
        String checksum = createChecksum(256);
        JacsBundle updatedEntity = testDao.update(te.getId(), ImmutableMap.of(
                "checksum", new SetFieldValueHandler<>(checksum),
                "modified", new SetFieldValueHandler<>(new Date()))
        );
        JacsBundle retrievedTe = testDao.findById(te.getId());
        assertThat(retrievedTe.getName(), equalTo(updatedEntity.getName()));
        assertNull(te.getChecksum());
        assertThat(retrievedTe.getChecksum(), equalTo(checksum));
        assertNotSame(te, retrievedTe);
        assertNotSame(updatedEntity, retrievedTe);
    }

    private JacsStorageVolume createTestVolume(String host, int port, String volumeName, String storageRootDir, Long available) {
        JacsStorageVolume v = new JacsStorageVolume();
        if (host != null) v.setStorageAgentId(host + ":" + port);
        v.setName(volumeName);
        v.setStorageRootTemplate(storageRootDir);
        v.setStorageVirtualPath(storageRootDir);
        if (StringUtils.isNotBlank(host)) {
            v.setStorageServiceURL("http://" + host + (port > 0 ? ":" + port : ""));
        }
        v.setAvailableSpaceInBytes(available);
        testVolumes.add(v);
        return v;
    }

    private JacsBundle createTestEntity(String ownerKey, String name, Number storageVolumeId, String path, Long size, int checksumLength) {
        JacsBundle d = new JacsBundle();
        d.setOwnerKey(ownerKey);
        d.setName(name);
        d.setPath(path);
        d.setStorageVolumeId(storageVolumeId);
        d.setUsedSpaceInBytes(size);
        d.setChecksum(createChecksum(checksumLength));
        testData.add(d);
        return d;
    }

    private String createChecksum(int length) {
        if (length > 0) {
            byte[] bytes = new byte[length];
            rng.nextBytes(bytes);
            return Base64.getEncoder().encodeToString(bytes);
        } else
            return null;
    }
}
