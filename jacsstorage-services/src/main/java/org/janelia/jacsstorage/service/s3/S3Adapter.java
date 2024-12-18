package org.janelia.jacsstorage.service.s3;

import java.net.URI;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageOptions;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;

public class S3Adapter {
    private static final Logger LOG = LoggerFactory.getLogger(S3Adapter.class);
    private static final long MB = 1024L * 1024L * 1024L;

    private final String bucket;
    private final String region;
    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final S3Client s3Client;
    private final S3AsyncClient asyncS3Client;

    S3Adapter(String bucket, String endpoint, String region, String accessKey, String secretKey) {
        this.bucket = bucket;
        this.endpoint = endpoint;
        this.region = region;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        S3ClientBuilder s3ClientBuilder = S3Client.builder();
        S3CrtAsyncClientBuilder asyncS3ClientBuilder = S3AsyncClient.crtBuilder();
        if (StringUtils.isNotBlank(region)) {
            Region s3Region = Region.of(region);
            s3ClientBuilder.region(s3Region);
            asyncS3ClientBuilder.region(s3Region);
        }
        if (StringUtils.isNotBlank(endpoint)) {
            URI endpointURI = URI.create(endpoint);
            s3ClientBuilder.endpointOverride(endpointURI);
            asyncS3ClientBuilder.endpointOverride(endpointURI);
        }
        AwsCredentialsProvider credentialsProvider;
        if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
            credentialsProvider = () -> AwsBasicCredentials.create(accessKey, secretKey);
        } else {
            credentialsProvider = AwsCredentialsProviderChain.of(
                    ProfileCredentialsProvider.create(),
                    SystemPropertyCredentialsProvider.create(),
                    EnvironmentVariableCredentialsProvider.create(),
                    AnonymousCredentialsProvider.create()
            );
        }
        s3ClientBuilder.credentialsProvider(credentialsProvider);

        S3Configuration s3Configuration = S3Configuration.builder()
                .checksumValidationEnabled(true)
                .pathStyleAccessEnabled(true)
                .chunkedEncodingEnabled(true)
                .multiRegionEnabled(true)
                .build();

        this.s3Client = s3ClientBuilder
                .serviceConfiguration(s3Configuration)
                .build();

        this.asyncS3Client = asyncS3ClientBuilder
                .initialReadBufferSizeInBytes(16 * MB)
                .build();
    }

    public JADEStorageURI getStorageURI() {
        StringBuilder uriBuilder = new StringBuilder();
        URI endpointURI;
        String appendedBucket;
        if (StringUtils.isNotBlank(endpoint)) {
            endpointURI = URI.create(endpoint);
            appendedBucket = '/' + bucket;
        } else {
            endpointURI = URI.create("s3://" + bucket);
            appendedBucket = "";
        }
        uriBuilder.append(endpointURI.getScheme()).append("://");
        uriBuilder.append(endpointURI.getHost())
                .append(appendedBucket);
        return JADEStorageURI.createStoragePathURI(
                uriBuilder.toString(),
                new JADEStorageOptions()
                        .setAccessKey(accessKey)
                        .setSecretKey(secretKey)
                        .setAWSRegion(region)
        );
    }

    public String getBucket() {
        return bucket;
    }

    public S3Client getS3Client() {
        return s3Client;
    }

    public S3AsyncClient getAsyncS3Client() {
        return asyncS3Client;
    }
}
