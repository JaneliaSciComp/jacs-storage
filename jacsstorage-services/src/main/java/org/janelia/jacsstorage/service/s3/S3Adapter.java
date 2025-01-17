package org.janelia.jacsstorage.service.s3;

import java.net.URI;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.model.jacsstorage.JADEOptions;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;

public class S3Adapter {
    private static final long MB = 1024L * 1024L * 1024L;

    private final String bucket;
    private final String endpoint;
    private final JADEOptions s3Options;
    private final S3Client s3Client;
    private final S3AsyncClient asyncS3Client;

    public S3Adapter(String bucket, String endpoint, JADEOptions s3Options) {
        this.bucket = bucket;
        this.endpoint = endpoint;
        this.s3Options = s3Options;
        S3ClientBuilder s3ClientBuilder = S3Client.builder();
        S3CrtAsyncClientBuilder asyncS3ClientBuilder = S3AsyncClient.crtBuilder();
        if (StringUtils.isNotBlank(s3Options.getAWSRegion())) {
            Region s3Region = Region.of(s3Options.getAWSRegion());
            s3ClientBuilder.region(s3Region);
            asyncS3ClientBuilder.region(s3Region);
        }
        if (StringUtils.isNotBlank(endpoint)) {
            URI endpointURI = URI.create(endpoint);
            s3ClientBuilder.endpointOverride(endpointURI);
            asyncS3ClientBuilder.endpointOverride(endpointURI);
        }
        AwsCredentialsProvider credentialsProvider;
        // when credentials (AWS accessKey and secretKey) are provided we only use the static credentials provider
        // otherwise we chain multiple providers
        if (StringUtils.isNotBlank(s3Options.getAccessKey()) && StringUtils.isNotBlank(s3Options.getSecretKey())) {
            credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(s3Options.getAccessKey(), s3Options.getSecretKey()));
        } else {
            AwsCredentialsProviderChain.Builder credentialsProviderBuilder = AwsCredentialsProviderChain.builder()
                    .reuseLastProviderEnabled(false);
            boolean tryAnonymousAccessFirst = isTryAnonymousFirst(s3Options);
            if (tryAnonymousAccessFirst) {
                credentialsProviderBuilder.addCredentialsProvider(AnonymousCredentialsProvider.create());
            }
            credentialsProviderBuilder
                    .addCredentialsProvider(ProfileCredentialsProvider.create())
                    .addCredentialsProvider(SystemPropertyCredentialsProvider.create())
                    .addCredentialsProvider(EnvironmentVariableCredentialsProvider.create())
                    .addCredentialsProvider(InstanceProfileCredentialsProvider.create());
            if (!tryAnonymousAccessFirst) {
                credentialsProviderBuilder.addCredentialsProvider(AnonymousCredentialsProvider.create());
            }
            credentialsProvider = credentialsProviderBuilder.build();
        }
        s3ClientBuilder.credentialsProvider(credentialsProvider);

        S3Configuration s3Configuration = S3Configuration.builder()
                .pathStyleAccessEnabled(s3Options.getPathStyleBucket())
                .build();

        this.s3Client = s3ClientBuilder
                .serviceConfiguration(s3Configuration)
                .build();

        this.asyncS3Client = asyncS3ClientBuilder
                .initialReadBufferSizeInBytes(16 * MB)
                .forcePathStyle(s3Options.getPathStyleBucket())
                .build();
    }

    private boolean isTryAnonymousFirst(JADEOptions s3Options) {
        Boolean tryAnonymousAccessFirst = s3Options.getTryAnonymousAccessFirst();
        return tryAnonymousAccessFirst != null && tryAnonymousAccessFirst;
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
                JADEOptions.createFromOptions(s3Options)
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
