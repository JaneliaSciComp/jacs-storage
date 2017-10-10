package org.janelia.jacsstorage.dao.mongo;

import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class JacsStorageVolumeMongoDaoITest extends AbstractMongoDaoITest<JacsStorageVolume> {

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
        JacsStorageVolume te = persistEntity(testDao, createTestEntity("v1", "127.0.0.1", "/tmp", 100L, 20L));
        JacsStorageVolume retrievedTe = testDao.findById(te.getId());
        assertThat(retrievedTe.getName(), equalTo(te.getName()));
        assertNotSame(te, retrievedTe);
    }

    @Test
    public void searchMissingVolumeByLocationAndCheckIfCreated() {
        JacsStorageVolume justCreated = testDao.getStorageByLocationAndCreateIfNotFound("newlyCreatedLocation");
        assertNotNull(justCreated);
        testData.add(justCreated);
        JacsStorageVolume retrievedTe = testDao.findById(justCreated.getId());
        assertNotNull(retrievedTe);
        assertThat(retrievedTe.getLocation(), equalTo(justCreated.getLocation()));
        assertNotSame(justCreated, retrievedTe);
    }

    @Test
    public void searchExistingVolumeByLocationAndCheckNoNewOneIsCreated() {
        String testLocation = "127.0.0.1";
        JacsStorageVolume te = persistEntity(testDao, createTestEntity("v1", testLocation, "/tmp", 100L, 20L));
        JacsStorageVolume existingVolume = testDao.getStorageByLocationAndCreateIfNotFound(testLocation);
        assertNotNull(existingVolume);
        assertNotSame(te, existingVolume);
        PageResult<JacsStorageVolume> allVolumes = testDao.findAll(new PageRequest());
        assertThat(allVolumes.getResultList().stream().filter(v -> testLocation.equals(v.getLocation())).count(), equalTo(1L));
    }

    private JacsStorageVolume createTestEntity(String name, String location, String mountPoint, Long capacity, Long available) {
        JacsStorageVolume v = new JacsStorageVolume();
        v.setName(name);
        v.setLocation(location);
        v.setMountPoint(mountPoint);
        v.setCapacityInMB(capacity);
        v.setAvailableInMB(available);
        testData.add(v);
        return v;
    }

}
