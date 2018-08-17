package org.janelia.jacsstorage.expr;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;

public class ExprHelperTest {

    @Test
    public void evalExpr() {
        class TestData {
            final String expr;
            final String expectedResult;
            final Map<String, Object> context;

            TestData(String expr, String expectedResult, Map<String, Object> context) {
                this.expr = expr;
                this.expectedResult = expectedResult;
                this.context = context;
            }
        }
        TestData[] testData = new TestData[]{
                new TestData(
                        "unchanged",
                        "unchanged",
                        ImmutableMap.of()
                ),
                new TestData(
                        "unchanged",
                        "unchanged",
                        ImmutableMap.of("unchanged", "changed value")
                ),
                new TestData(
                        "${name}",
                        "my name",
                        ImmutableMap.of("name", "my name")
                ),
                new TestData(
                        "/p1/p2/p3/${name}/p4",
                        "/p1/p2/p3/my name/p4",
                        ImmutableMap.of("name", "my name")
                ),
                new TestData(
                        "/p1/p2/p3/${name}/p4",
                        "/p1/p2/p3/${name}/p4",
                        ImmutableMap.of()
                ),
                new TestData(
                        "/p1/p2/p3/${name}/p4",
                        "/p1/p2/p3/${name}/p4",
                        ImmutableMap.of(
                                "p1", "p1 val",
                                "p2", "p2 val",
                                "p3", "p3 val",
                                "p4", "p4 val"
                        )
                ),
                new TestData(
                        "${name}",
                        "${name}",
                        ImmutableMap.of("myname", "my name")
                )
        };
        for (TestData td : testData) {
            String result = ExprHelper.eval(td.expr, td.context);
            assertEquals(td.expr + "->" + td.expectedResult, td.expectedResult, result);
        }
    }

    @Test
    public void matchExpr() {
        class TestData {
            final String pattern;
            final String value;
            final boolean match;

            TestData(String pattern, String value, boolean match) {
                this.pattern = pattern;
                this.value = value;
                this.match = match;
            }
        }
        TestData[] testData = new TestData[]{
                new TestData(
                        "/p1/p2/p3/${name}/p4",
                        "/p1/p2/p3/my name/p4",
                        true
                ),
                new TestData(
                        "/p1/p2/p3${name}",
                        "/p1/p2/p3my name",
                        true
                ),
                new TestData(
                        "/p1/p2/p3",
                        "/p1/p2/p3/my name/p4",
                        true
                ),
                new TestData(
                        "/p1/p2/p3",
                        "/p1/p2/p4/my name",
                        false
                ),
                new TestData(
                        "/p1/p2/${name}/p3",
                        "/p1/p2/p2.2/p3/my name",
                        true
                ),
                new TestData(
                        "/p1/p2/${name}/p3/${name}/p4",
                        "/p1/p2/p2.2/p3/p2.2/p4/p5",
                        true
                ),
                new TestData(
                        "/p1/p2/${name}/p3/${name}/p4",
                        "/p1/p2/name1/p3/name2/p4/p5",
                        false // failure due to conflict
                ),
                new TestData(
                        "/p1/p2/${name}/p3",
                        "/p1/p2/p3/my name",
                        false
                ),
                new TestData(
                        "/p1/p2/${name}",
                        "/p1/p2/p3/p5/p6",
                        true
                )
        };
        for (TestData td : testData) {
            MatchingResult result = ExprHelper.match(td.pattern, td.value);
            assertEquals(td.pattern + " - " + td.value, td.match, result.isMatchFound());
        }
    }

}
