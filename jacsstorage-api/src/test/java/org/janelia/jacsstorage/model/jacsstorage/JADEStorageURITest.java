package org.janelia.jacsstorage.model.jacsstorage;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class JADEStorageURITest {

    @Test
    public void decodeStoragePathURI() {
        class TestData {
            final String uriDesc;
            final String accessKey;
            final String secretKey;
            final String expectedHost;
            final String expectedJADEKey;
            final String expectedContentKey;
            final JacsStorageType expectedStorageType;
            final String expectedJadeStorage;

            TestData(String uriDesc, String accessKey, String secretKey, String expectedHost,
                     String expectedJADEKey, String expectedContentKey,
                     JacsStorageType expectedStorageType, String expectedJadeStorage) {
                this.uriDesc = uriDesc;
                this.expectedHost = expectedHost;
                this.accessKey = accessKey;
                this.secretKey = secretKey;
                this.expectedJADEKey = expectedJADEKey;
                this.expectedContentKey = expectedContentKey;
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
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM,
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "file://scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "",
                        "",
                        "scicompsoft-public",
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM,
                        "/scicompsoft-public/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "https://NNQ20KNJ2YCWWMPE:IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM@s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public1",
                        "NNQ20KNJ2YCWWMPE",
                        "IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM",
                        "s3.us-east-1.lyvecloud.seagate.com",
                        "/scicompsoft-public1",
                        "",
                        JacsStorageType.S3,
                        "https://s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public1"
                ),
                new TestData(
                        "jade://https://NNQ20KNJ2YCWWMPE:IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM@s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public2/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "NNQ20KNJ2YCWWMPE",
                        "IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM",
                        "s3.us-east-1.lyvecloud.seagate.com",
                        "/scicompsoft-public2/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.S3,
                        "https://s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public2/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "jade://https://NNQ20KNJ2YCWWMPE@s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public3/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "NNQ20KNJ2YCWWMPE",
                        "",
                        "s3.us-east-1.lyvecloud.seagate.com",
                        "/scicompsoft-public3/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.S3,
                        "https://s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public3/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "jade://https://s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public4/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "",
                        "",
                        "s3.us-east-1.lyvecloud.seagate.com",
                        "/scicompsoft-public4/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.S3,
                        "https://s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public4/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "jade://s3://scicompsoft-public5/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "",
                        "",
                        "scicompsoft-public5",
                        "/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.S3,
                        "s3://scicompsoft-public5/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "s3://scicompsoft-public6/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "",
                        "",
                        "scicompsoft-public6",
                        "/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.S3,
                        "s3://scicompsoft-public6/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "https://NNQ20KNJ2YCWWMPE:IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM@s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public7/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "NNQ20KNJ2YCWWMPE",
                        "IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM",
                        "s3.us-east-1.lyvecloud.seagate.com",
                        "/scicompsoft-public7/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.S3,
                        "https://s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public7/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "jade://NNQ20KNJ2YCWWMPE:IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM@s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public8/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "NNQ20KNJ2YCWWMPE",
                        "IID4TNAS3OXI2UUAAKK21CCYHJRAP3JM",
                        "s3.us-east-1.lyvecloud.seagate.com",
                        "/s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public8/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "/s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public8/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM,
                        "/s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public8/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "jade://s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public9/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "",
                        "",
                        "s3.us-east-1.lyvecloud.seagate.com",
                        "/s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public9/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "/s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public9/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM,
                        "/s3.us-east-1.lyvecloud.seagate.com/scicompsoft-public9/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "jade:///scicompsoft-public10/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "",
                        "",
                        "",
                        "/scicompsoft-public10/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "/scicompsoft-public10/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM,
                        "/scicompsoft-public10/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "jade://scicompsoft-public11/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "",
                        "",
                        "scicompsoft-public11",
                        "/scicompsoft-public11/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "/scicompsoft-public11/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM,
                        "/scicompsoft-public11/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "/scicompsoft-public12/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "",
                        "",
                        "",
                        "/scicompsoft-public12/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "/scicompsoft-public12/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM,
                        "/scicompsoft-public12/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "scicompsoft-public13/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "",
                        "",
                        "",
                        "/scicompsoft-public13/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "/scicompsoft-public13/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM,
                        "/scicompsoft-public13/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "s3://scicompsoft-public14",
                        "",
                        "",
                        "scicompsoft-public14",
                        "",
                        "",
                        JacsStorageType.S3,
                        "s3://scicompsoft-public14"
                ),
                new TestData(
                        "s3://scicompsoft-public15/",
                        "",
                        "",
                        "scicompsoft-public15",
                        "",
                        "",
                        JacsStorageType.S3,
                        "s3://scicompsoft-public15"
                ),
                new TestData(
                        "scicompsoft-public16/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "",
                        "",
                        "",
                        "/scicompsoft-public16/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "/scicompsoft-public16/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM,
                        "/scicompsoft-public16/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
                new TestData(
                        "//scicompsoft-public17/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "",
                        "",
                        "scicompsoft-public17",
                        "/scicompsoft-public17/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        "/scicompsoft-public17/scicompsoft/flynp/pipeline_info/software_versions.yml",
                        JacsStorageType.FILE_SYSTEM,
                        "/scicompsoft-public17/scicompsoft/flynp/pipeline_info/software_versions.yml"
                ),
        };

        for (TestData td : testData) {
            JADEStorageURI storagePathURI = JADEStorageURI.createStoragePathURI(
                    td.uriDesc,
                    new JADEStorageOptions()
                            .setAccessKey(td.accessKey)
                            .setSecretKey(td.secretKey)
            );
            assertThat("Checking JADE key " + td.uriDesc, storagePathURI.getJADEKey(), equalTo(td.expectedJADEKey));
            assertThat("Checking content key " + td.uriDesc, storagePathURI.getContentKey(), equalTo(td.expectedContentKey));
            assertThat("Checking host " + td.expectedHost, storagePathURI.getStorageHost(), equalTo(td.expectedHost));
            assertThat("Checking access key " + td.uriDesc, storagePathURI.getStorageType(), equalTo(td.expectedStorageType));
            assertThat("Checking access key " + td.uriDesc, storagePathURI.getStorageOptions().getAccessKey(null), equalTo(td.accessKey));
            assertThat("Checking secret key " + td.uriDesc, storagePathURI.getStorageOptions().getSecretKey(null), equalTo(td.secretKey));
            assertThat("Checking storage " + td.uriDesc, storagePathURI.getJadeStorage(), equalTo(td.expectedJadeStorage));
        }
    }
}
