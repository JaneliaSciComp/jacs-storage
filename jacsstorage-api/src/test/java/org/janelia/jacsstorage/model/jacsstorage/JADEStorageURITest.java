package org.janelia.jacsstorage.model.jacsstorage;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class JADEStorageURITest {

    @Test
    public void decodeStoragePathURI() {
        class TestData {
            final String uriDesc;
            final String host;
            final String userKey;
            final String userPassword;
            final String key;
            final JacsStorageType storageType;

            TestData(String uriDesc, String host, String userKey, String userPassword, String key, JacsStorageType storageType) {
                this.uriDesc = uriDesc;
                this.host = host;
                this.userKey = userKey;
                this.userPassword = userPassword;
                this.key = key;
                this.storageType = storageType;
            }

        }
        TestData[] testData = new TestData[] {
                new TestData(
                        "",
                        "",
                        "",
                        "",
                        "",
                        JacsStorageType.FILE_SYSTEM
                ),
                new TestData(
                        "jade://https://NNQ20KNJ2YCWWMPE:IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM@s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "s3.us-east-1.lyvecloud.seagate.com",
                        "NNQ20KNJ2YCWWMPE",
                        "IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM",
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.S3
                ),
                new TestData(
                        "jade://https://s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "s3.us-east-1.lyvecloud.seagate.com",
                        "",
                        "",
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.S3
                ),
                new TestData(
                        "jade://s3://scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "scicompsoft-public",
                        "",
                        "",
                        "/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.S3
                ),
                new TestData(
                        "s3://scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "scicompsoft-public",
                        "",
                        "",
                        "/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.S3
                ),
                new TestData(
                        "https://NNQ20KNJ2YCWWMPE:IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM@s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "s3.us-east-1.lyvecloud.seagate.com",
                        "NNQ20KNJ2YCWWMPE",
                        "IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM",
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.S3
                ),
                new TestData(
                        "jade://NNQ20KNJ2YCWWMPE:IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM@s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "s3.us-east-1.lyvecloud.seagate.com",
                        "NNQ20KNJ2YCWWMPE",
                        "IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM",
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.S3
                ),
                new TestData(
                        "jade:///scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "",
                        "",
                        "",
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM
                ),
                new TestData(
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "",
                        "",
                        "",
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM
                ),
        };

        for (TestData td : testData) {
            JADEStorageURI storagePathURI = JADEStorageURI.createStoragePathURI(td.uriDesc);
            assertThat("Checking storage key " + td.uriDesc, storagePathURI.getStorageKey(), equalTo(td.key));
            assertThat("Checking host " + td.host, storagePathURI.getStorageHost(), equalTo(td.host));
            assertThat("Checking access key " + td.uriDesc, storagePathURI.getUserAccessKey(), equalTo(td.userKey));
            assertThat("Checking secret key " + td.uriDesc, storagePathURI.getUserSecretKey(), equalTo(td.userPassword));
        }
    }
}
