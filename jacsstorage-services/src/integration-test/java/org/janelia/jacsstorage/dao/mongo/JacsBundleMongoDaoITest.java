package org.janelia.jacsstorage.dao.mongo;

import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class JacsBundleMongoDaoITest extends AbstractMongoDaoITest<JacsBundle> {

    private List<JacsBundle> testData = new ArrayList<>();
    private JacsBundleDao testDao;

    @Before
    public void setUp() {
        testDao = new JacsBundleMongoDao(testMongoDatabase, idGenerator);
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
        JacsBundle te = persistEntity(testDao, createTestEntity("d1", "/tmp", 100L));
        JacsBundle retrievedTe = testDao.findById(te.getId());
        assertThat(retrievedTe.getName(), equalTo(te.getName()));
        assertNotSame(retrievedTe, te);
    }

    private JacsBundle createTestEntity(String name, String path, Long size) {
        JacsBundle d = new JacsBundle();
        d.setName(name);
        d.setPath(path);
        d.setUsedSpaceInKB(size);
        testData.add(d);
        return d;
    }

}
