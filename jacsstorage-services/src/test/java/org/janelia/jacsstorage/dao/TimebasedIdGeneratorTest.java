package org.janelia.jacsstorage.dao;

import java.util.List;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

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
