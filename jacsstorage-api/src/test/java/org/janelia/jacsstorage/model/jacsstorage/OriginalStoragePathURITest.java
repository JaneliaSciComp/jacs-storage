package org.janelia.jacsstorage.model.jacsstorage;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class OriginalStoragePathURITest {

    @Test
    public void absoluteStoragePathURI() {
        Map<String, String> testData = ImmutableMap.<String, String>builder()
                .put("jade:///jade_dev/2567842844077203473", "/jade_dev/2567842844077203473")
                .put("jade://jade_dev/2567842844077203473", "/jade_dev/2567842844077203473")
                .put("jade:///jade_dev/2567842844077203473/mips/image-1/image-1_all.png", "/jade_dev/2567842844077203473/mips/image-1/image-1_all.png")
                .put("//jade_dev/2567842844077203473", "/jade_dev/2567842844077203473")
                .put("///jade_dev/2567842844077203473", "/jade_dev/2567842844077203473")
                .build();
        testData.forEach((k, v) -> {
            OriginalStoragePathURI storagePathURI = OriginalStoragePathURI.createAbsolutePathURI(k);
            assertThat(k, storagePathURI.getStoragePath(), equalTo(v));
        });
    }
}
