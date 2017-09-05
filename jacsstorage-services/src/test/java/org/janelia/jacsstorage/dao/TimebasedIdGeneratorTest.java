package org.janelia.jacsstorage.dao;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;

public class TimebasedIdGeneratorTest {
    private TimebasedIdGenerator idGenerator;

    @Before
    public void setUp() {
        idGenerator = new TimebasedIdGenerator(0);
    }

    @Test
    public void generateLargeListOfIds() {
        List<Number> idList = idGenerator.generateIdList(16384);
        assertThat(ImmutableSet.copyOf(idList), hasSize(idList.size()));
    }
}
