package org.janelia.jacsstorage.dao.mongo;

import org.janelia.jacsstorage.dao.JacsVolumeDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsVolume;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class JacsVolumeMongoDaoITest extends AbstractMongoDaoITest<JacsVolume> {

    private List<JacsVolume> testData = new ArrayList<>();
    private JacsVolumeDao testDao;

    @Before
    public void setUp() {
        testDao = new JacsVolumeMongoDao(testMongoDatabase, idGenerator);
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
        JacsVolume te = persistEntity(testDao, createTestEntity("v1", "/tmp", 100L, 20L));
        JacsVolume retrievedTe = testDao.findById(te.getId());
        assertThat(retrievedTe.getName(), equalTo(te.getName()));
        assertNotSame(retrievedTe, te);
    }

    private JacsVolume createTestEntity(String name, String mountPoint, Long capacity, Long available) {
        JacsVolume v = new JacsVolume();
        v.setName(name);
        v.setMountPoint(mountPoint);
        v.setCapacityInMB(capacity);
        v.setAvailableInMB(available);
        testData.add(v);
        return v;
    }

}
