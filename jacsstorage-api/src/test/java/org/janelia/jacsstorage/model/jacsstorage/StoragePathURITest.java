package org.janelia.jacsstorage.model.jacsstorage;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class StoragePathURITest {

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
            StoragePathURI storagePathURI = StoragePathURI.createAbsolutePathURI(k);
            assertThat(k, storagePathURI.getStoragePath(), equalTo(v));
        });
    }
}
