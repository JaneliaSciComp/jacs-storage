package org.janelia.jacsstorage.service.s3;

import java.net.URI;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.model.jacsstorage.JADEOptions;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;

public class S3Adapter {
    private static final long MB = 1024L * 1024L * 1024L;

    private final String bucket;
    private final String endpoint;
    private final JADEOptions s3Options;
    private final S3Client syncS3Client;
    private final S3AsyncClient asyncS3Client;

    S3Adapter(String bucket, String endpoint, JADEOptions s3Options) {
        this.bucket = bucket;
        this.endpoint = endpoint;
        this.s3Options = s3Options;
        AwsCredentialsProvider credentialsProvider = createCredentialsProvider(s3Options);
        this.asyncS3Client = createAsyncClient(endpoint, credentialsProvider, s3Options);
        this.syncS3Client = createSyncClient(endpoint, credentialsProvider, s3Options);
    }

    private S3AsyncClient createAsyncClient(String endpoint, AwsCredentialsProvider credentialsProvider, JADEOptions s3Options) {
        S3AsyncClientBuilder asyncS3ClientBuilder = S3AsyncClient.builder();
        if (StringUtils.isNotBlank(s3Options.getAWSRegion())) {
            Region s3Region = Region.of(s3Options.getAWSRegion());
            asyncS3ClientBuilder.region(s3Region);
        }
        if (StringUtils.isNotBlank(endpoint)) {
            URI endpointURI = URI.create(endpoint);
            asyncS3ClientBuilder.endpointOverride(endpointURI);
        }
        asyncS3ClientBuilder.credentialsProvider(credentialsProvider);
        return asyncS3ClientBuilder
                .crossRegionAccessEnabled(true)
                .forcePathStyle(s3Options.getPathStyleBucket())
                .multipartEnabled(true)
                .build();
    }

    private S3Client createSyncClient(String endpoint, AwsCredentialsProvider credentialsProvider, JADEOptions s3Options) {
        S3ClientBuilder s3ClientBuilder = S3Client.builder();
        if (StringUtils.isNotBlank(s3Options.getAWSRegion())) {
            Region s3Region = Region.of(s3Options.getAWSRegion());
            s3ClientBuilder.region(s3Region);
        }
        if (StringUtils.isNotBlank(endpoint)) {
            URI endpointURI = URI.create(endpoint);
            s3ClientBuilder.endpointOverride(endpointURI);
        }
        s3ClientBuilder.credentialsProvider(credentialsProvider);

        S3Configuration s3Configuration = S3Configuration.builder()
                .multiRegionEnabled(true)
                .pathStyleAccessEnabled(s3Options.getPathStyleBucket())
                .build();

        return s3ClientBuilder
                .serviceConfiguration(s3Configuration)
                .build();
    }

    private AwsCredentialsProvider createCredentialsProvider(JADEOptions s3Options) {
        AwsCredentialsProvider credentialsProvider;
        // when credentials (AWS accessKey and secretKey) are provided we only use the static credentials provider
        // otherwise we chain multiple providers
        if (StringUtils.isNotBlank(s3Options.getAccessKey()) && StringUtils.isNotBlank(s3Options.getSecretKey())) {
            credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(s3Options.getAccessKey(), s3Options.getSecretKey()));
        } else {
            AwsCredentialsProviderChain.Builder credentialsProviderBuilder = AwsCredentialsProviderChain.builder();
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
        return credentialsProvider;
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

    public S3Client getSyncS3Client() {
        return syncS3Client;
    }

    public S3AsyncClient getAsyncS3Client() {
        return asyncS3Client;
    }
}
