package org.janelia.jacsstorage.model.support;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class JacsSubjectHelperTest {

    @Test
    public void subjectKey() {
        class TestData {
            private final String testKey;
            private final String expectedType;
            private final String expectedName;

            private TestData(String testKey, String expectedType, String expectedName) {
                this.testKey = testKey;
                this.expectedType = expectedType;
                this.expectedName = expectedName;
            }
        }
        TestData[] testData = new TestData[] {
                new TestData("", null, null),
                new TestData("user:jacs", "user", "jacs"),
                new TestData("group:jacs", "group", "jacs"),
                new TestData(":jacs", "user", "jacs"),
                new TestData("jacs", "user", "jacs"),
                new TestData("group:", "group", ""),
                new TestData(" group : ", "group", ""),
                new TestData(" group : jacs ", "group", "jacs")
        };
        int testIndex = 1;
        for (TestData td : testData) {
            assertThat("Test get name " + testIndex, JacsSubjectHelper.getNameFromSubjectKey(td.testKey), equalTo(td.expectedName));
            assertThat("Test get type " + testIndex, JacsSubjectHelper.getTypeFromSubjectKey(td.testKey), equalTo(td.expectedType));
            testIndex++;
        }
    }

}
