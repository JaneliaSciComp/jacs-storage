package org.janelia.jacsstorage.model.jacsstorage;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class JADEStorageURITest {

    @Test
    public void decodeStoragePathURI() {
        class TestData {
            final String uriDesc;
            final String expectedHost;
            final String expectedUserKey;
            final String expectedUserPassword;
            final String expectedKey;
            final JacsStorageType expectedStorageType;
            final String expectedJadeStorage;

            TestData(String uriDesc, String expectedHost, String expectedUserKey, String expectedUserPassword, String expectedKey,
                     JacsStorageType expectedStorageType, String expectedJadeStorage) {
                this.uriDesc = uriDesc;
                this.expectedHost = expectedHost;
                this.expectedUserKey = expectedUserKey;
                this.expectedUserPassword = expectedUserPassword;
                this.expectedKey = expectedKey;
                this.expectedStorageType = expectedStorageType;
                this.expectedJadeStorage = expectedJadeStorage;
            }

        }
        TestData[] testData = new TestData[] {
                new TestData(
                        "",
                        "",
                        "",
                        "",
                        "",
                        JacsStorageType.FILE_SYSTEM,
                        ""
                ),
                new TestData(
                        "file:///scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "",
                        "",
                        "",
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM,
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "file://scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "scicompsoft-public",
                        "",
                        "",
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM,
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "jade://https://NNQ20KNJ2YCWWMPE:IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM@s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "s3.us-east-1.lyvecloud.seagate.com",
                        "NNQ20KNJ2YCWWMPE",
                        "IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM",
                        "scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.S3,
                        "https://NNQ20KNJ2YCWWMPE:IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM@s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "jade://https://NNQ20KNJ2YCWWMPE@s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "s3.us-east-1.lyvecloud.seagate.com",
                        "NNQ20KNJ2YCWWMPE",
                        "",
                        "scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.S3,
                        "https://NNQ20KNJ2YCWWMPE@s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "jade://https://s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "s3.us-east-1.lyvecloud.seagate.com",
                        "",
                        "",
                        "scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.S3,
                        "https://s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "jade://s3://scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "scicompsoft-public",
                        "",
                        "",
                        "scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.S3,
                        "s3://scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "s3://scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "scicompsoft-public",
                        "",
                        "",
                        "scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.S3,
                        "s3://scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "https://NNQ20KNJ2YCWWMPE:IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM@s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "s3.us-east-1.lyvecloud.seagate.com",
                        "NNQ20KNJ2YCWWMPE",
                        "IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM",
                        "scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.S3,
                        "https://NNQ20KNJ2YCWWMPE:IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM@s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "jade://NNQ20KNJ2YCWWMPE:IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM@s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "s3.us-east-1.lyvecloud.seagate.com",
                        "NNQ20KNJ2YCWWMPE",
                        "IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM",
                        "/s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM,
                        "/s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "jade://s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "s3.us-east-1.lyvecloud.seagate.com",
                        "",
                        "",
                        "/s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM,
                        "/s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "jade:///scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "",
                        "",
                        "",
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM,
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "jade://scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "scicompsoft-public",
                        "",
                        "",
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM,
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "",
                        "",
                        "",
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM,
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "",
                        "",
                        "",
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM,
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
        };

        for (TestData td : testData) {
            JADEStorageURI storagePathURI = JADEStorageURI.createStoragePathURI(td.uriDesc);
            assertThat("Checking storage key " + td.uriDesc, storagePathURI.getStorageKey(), equalTo(td.expectedKey));
            assertThat("Checking host " + td.expectedHost, storagePathURI.getStorageHost(), equalTo(td.expectedHost));
            assertThat("Checking access key " + td.uriDesc, storagePathURI.getStorageType(), equalTo(td.expectedStorageType));
            assertThat("Checking access key " + td.uriDesc, storagePathURI.getUserAccessKey(), equalTo(td.expectedUserKey));
            assertThat("Checking secret key " + td.uriDesc, storagePathURI.getUserSecretKey(), equalTo(td.expectedUserPassword));
            assertThat("Checking storage " + td.uriDesc, storagePathURI.getJadeStorage(), equalTo(td.expectedJadeStorage));
        }
    }
}
