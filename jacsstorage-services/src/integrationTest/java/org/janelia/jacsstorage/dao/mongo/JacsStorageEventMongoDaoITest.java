package org.janelia.jacsstorage.dao.mongo;

import org.janelia.jacsstorage.dao.JacsStorageEventDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageEvent;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageEventBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class JacsStorageEventMongoDaoITest extends AbstractMongoDaoITest {

    private List<JacsStorageEvent> testData = new ArrayList<>();
    private JacsStorageEventDao testDao;

    @Before
    public void setUp() {
        testDao = new JacsStorageEventMongoDao(testMongoDatabase, idGenerator);
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
        JacsStorageEvent te = persistEntity(testDao, createTestEntity(new JacsStorageEventBuilder()
                .eventName("EVENT_NAME")
                .eventDescription("event description")
                .eventData(new JacsBundleBuilder().dataBundleId(2L).name("d1").storageFormat(JacsStorageFormat.DATA_DIRECTORY).build())
                .build()));
        JacsStorageEvent retrievedTe = testDao.findById(te.getId());
        assertThat(retrievedTe.getEventName(), equalTo(te.getEventName()));
        assertNotSame(te, retrievedTe);
    }

    private JacsStorageEvent createTestEntity(JacsStorageEvent v) {
        testData.add(v);
        return v;
    }

}
