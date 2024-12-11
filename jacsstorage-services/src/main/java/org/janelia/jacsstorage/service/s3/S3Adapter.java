package org.janelia.jacsstorage.service.s3;

import java.net.URI;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageOptions;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class S3Adapter {
    private final String bucket;
    private final String region;
    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final S3Client s3Client;

    public S3Adapter(String bucket, String endpoint, String region, String accessKey, String secretKey) {
        this.bucket = bucket;
        this.endpoint = endpoint;
        this.region = region;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        S3ClientBuilder s3ClientBuilder = S3Client.builder();
        if (StringUtils.isNotBlank(region)) {
            s3ClientBuilder.region(Region.of(region));
        }
        if (StringUtils.isNotBlank(endpoint)) {
            s3ClientBuilder.endpointOverride(URI.create(endpoint));
        }
        if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
            s3ClientBuilder.credentialsProvider(() -> AwsBasicCredentials.create(accessKey, secretKey));
        } else {
            s3ClientBuilder.credentialsProvider(
                    AwsCredentialsProviderChain.of(
                            DefaultCredentialsProvider.create(),
                            AnonymousCredentialsProvider.create() // chaining anonymous credentials to be able to access open buckets if no credentials are set
                    )
            );
        }
        this.s3Client = s3ClientBuilder.build();
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
}
