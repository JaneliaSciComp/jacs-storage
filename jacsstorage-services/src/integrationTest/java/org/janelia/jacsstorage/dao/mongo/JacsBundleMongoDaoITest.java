package org.janelia.jacsstorage.dao.mongo;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.datarequest.PageRequestBuilder;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
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
        JacsBundle te = persistEntity(testDao, createTestEntity("user", "d1", "/tmp", 100L, ImmutableMap.of("f1", 1, "f2", "v2")));
        JacsBundle retrievedTe = testDao.findById(te.getId());
        assertThat(retrievedTe.getName(), equalTo(te.getName()));
        assertNotSame(te, retrievedTe);
    }

    @Test
    public void findByNameAndOwner() {
        String testUser = "user";
        String testName = "test";
        JacsBundle te = persistEntity(testDao, createTestEntity(testUser, testName, "/tmp", 100L, ImmutableMap.of("f1", 1, "f2", "v2")));
        JacsBundle retrievedTe = testDao.findByOwnerAndName(testUser, testName);
        assertThat(retrievedTe.getName(), equalTo(te.getName()));
        assertNotSame(te, retrievedTe);
    }

    @Test
    public void findMatchingDataBundles() {
        String testUser = "user";
        String testName = "test";
        persistEntity(testDao, createTestEntity(testUser, testName, "/tmp", 100L, ImmutableMap.of("f1", 1, "f2", "v2")));
        assertThat(testDao.findMatchingDataBundles(
                new JacsBundleBuilder().build(),
                new PageRequestBuilder().build()
        ).getResultList(), hasSize(1));
        assertThat(testDao.findMatchingDataBundles(
                new JacsBundleBuilder().owner("anotherUser").build(),
                new PageRequestBuilder().build()
        ).getResultList(), hasSize(0));
    }

    private JacsBundle createTestEntity(String owner, String name, String path, Long size, Map<String, Object> metadata) {
        JacsBundle d = new JacsBundle();
        d.setOwner(owner);
        d.setName(name);
        d.setPath(path);
        d.setUsedSpaceInKB(size);
        metadata.forEach(d::addMetadataField);
        testData.add(d);
        return d;
    }

}
