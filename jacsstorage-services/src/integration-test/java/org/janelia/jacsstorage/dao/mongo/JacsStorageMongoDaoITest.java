package org.janelia.jacsstorage.dao.mongo;

import org.janelia.jacsstorage.dao.JacsStorageDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class JacsStorageMongoDaoITest extends AbstractMongoDaoITest<JacsStorage> {

    private List<JacsStorage> testData = new ArrayList<>();
    private JacsStorageDao testDao;

    @Before
    public void setUp() {
        testDao = new JacsStorageMongoDao(testMongoDatabase, idGenerator);
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
        JacsStorage te = persistEntity(testDao, createTestEntity("l1"));
        JacsStorage retrievedTe = testDao.findById(te.getId());
        assertThat(retrievedTe.getLocation(), equalTo(te.getLocation()));
        assertNotSame(retrievedTe, te);
    }

    private JacsStorage createTestEntity(String location) {
        JacsStorage s = new JacsStorage();
        s.setLocation(location);
        testData.add(s);
        return s;
    }

}
