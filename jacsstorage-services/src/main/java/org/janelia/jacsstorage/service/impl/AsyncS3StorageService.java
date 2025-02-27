package org.janelia.jacsstorage.service.impl;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.janelia.jacsstorage.coreutils.ComparatorUtils;
import org.janelia.jacsstorage.coreutils.IOStreamUtils;
import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.service.ContentException;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.NoContentFoundException;
import org.janelia.jacsstorage.service.s3.S3Adapter;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.BlockingInputStreamAsyncRequestBody;
import software.amazon.awssdk.core.async.ResponsePublisher;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedDownload;
import software.amazon.awssdk.transfer.s3.model.Download;
import software.amazon.awssdk.transfer.s3.model.DownloadRequest;

public class AsyncS3StorageService extends AbstractS3StorageService {

    private final static Logger LOG = LoggerFactory.getLogger(AsyncS3StorageService.class);

    AsyncS3StorageService(S3Adapter s3Adapter) {
        super(s3Adapter);
    }

    @Override
    public boolean canAccess(String contentLocation) {
        String s3Location = adjustLocation(contentLocation);
        LOG.debug("Check access to {}", s3Location);
        // we cannot simply do a head request because that only works for existing objects
        // and contentLocation may be a prefix
        ListObjectsV2Request initialRequest = ListObjectsV2Request.builder()
                .bucket(s3Adapter.getBucket())
                .prefix(s3Location)
                .maxKeys(1)
                .build();
        Publisher<Boolean> contentIsAccessible = s3Adapter.getAsyncS3Client().listObjectsV2Paginator(initialRequest)
                .map(r -> !r.contents().isEmpty())
                .limit(1);
        return Mono.from(contentIsAccessible).block();
    }

    @Override
    public ContentNode getObjectNode(String contentLocation) {
        String s3Location = adjustLocation(contentLocation);

        HeadObjectRequest contentRequest = HeadObjectRequest.builder()
                .bucket(s3Adapter.getBucket())
                .key(s3Location)
                .build();

        CompletableFuture<ContentNode> contentPromise = s3Adapter.getAsyncS3Client().headObject(contentRequest)
                .thenApply(contentResponse -> createObjectNode(contentLocation, contentResponse.contentLength(), contentResponse.lastModified()))
                .whenComplete((n, e) -> {
                    if (e != null) {
                        if (e instanceof NoSuchBucketException || e instanceof NoSuchKeyException) {
                            throw new NoContentFoundException(e);
                        } else {
                            throw new ContentException(e);
                        }
                    }
                });

        return contentPromise.join();
    }

    @Override
    List<ContentNode> listPrefixNodes(String s3Location, ContentAccessParams contentAccessParams) {
        LOG.debug("List prefix nodes at {}", s3Location);

        return processSubPrefixes(s3Location, s3Location, contentAccessParams, 0)
                .toStream()
                .collect(Collectors.toList());
    }

    private Flux<ContentNode> processSubPrefixes(String basePrefix,
                                                 String prefix,
                                                 ContentAccessParams contentAccessParams,
                                                 int level) {
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(s3Adapter.getBucket())
                .prefix(prefix)
                .delimiter("/")
                .build();

        ListObjectsV2Publisher listObjectsV2Publisher = s3Adapter.getAsyncS3Client().listObjectsV2Paginator(listRequest);

        return Flux.from(listObjectsV2Publisher)
                .index()
                .flatMap(indexedResponse -> Flux.concat(
                        // add requested prefix if this is the first time it is seen
                        indexedResponse.getT1() == 0 && level == 0 &&
                        (!indexedResponse.getT2().commonPrefixes().isEmpty() || !indexedResponse.getT2().contents().isEmpty()) &&
                        prefix.endsWith("/") &&
                        contentAccessParams.checkDepth(getPathDepth(basePrefix, prefix)) &&
                        contentAccessParams.matchEntry(prefix)
                                ? Flux.just(createPrefixNode(prefix)) // check if the current prefix ends with '/' and if doesn't create a node for it
                                : Flux.empty(),
                        // add sub-folders
                        Flux.fromIterable(indexedResponse.getT2().commonPrefixes())
                                .map(CommonPrefix::prefix)
                                .filter(contentAccessParams::matchEntry)
                                .map(this::createPrefixNode),
                        // recurse into sub-folders
                        Flux.fromIterable(indexedResponse.getT2().commonPrefixes())
                                .map(CommonPrefix::prefix)
                                .filter(p -> contentAccessParams.checkDepth(getPathDepth(basePrefix, p)))
                                .flatMap(p -> processSubPrefixes(basePrefix, p, contentAccessParams, level + 1))
                ));
    }

    @Override
    List<ContentNode> listObjectNodes(String s3Location, ContentAccessParams contentAccessParams) {
        LOG.debug("List object nodes at {}", s3Location);

        return processAllNodes(s3Location, s3Location, contentAccessParams, 0)
                .skip(Math.max(0, contentAccessParams.getStartEntryIndex()))
                .take(contentAccessParams.getEntriesCount() > 0
                        ? contentAccessParams.getEntriesCount()
                        : Long.MAX_VALUE)
                .toStream()
                .collect(Collectors.toList());
    }

    private Flux<ContentNode> processAllNodes(String basePrefix,
                                              String prefix,
                                              ContentAccessParams contentAccessParams,
                                              int level) {
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(s3Adapter.getBucket())
                .prefix(prefix)
                .delimiter("/")
                .build();

        ListObjectsV2Publisher listObjectsV2Publisher = s3Adapter.getAsyncS3Client().listObjectsV2Paginator(listRequest);

        return Flux.from(listObjectsV2Publisher)
                .index()
                .flatMap(indexedResponse -> {
                    List<S3Object> s3Objects1 = indexedResponse.getT2().contents();
                    if (level == 0 && !s3Objects1.isEmpty() && s3Objects1.get(0).key().equals(prefix)) {
                        // if there is an exact key match only return that object
                        return Flux.just(createObjectNode(s3Objects1.get(0)));
                    } else {
                        List<S3Object> sortedS3Objects = new ArrayList<>(s3Objects1);
                        sortedS3Objects.sort((s1, s2) -> ComparatorUtils.naturalCompare(s1.key(), s2.key(), true));
                        return Flux.concat(
                        // add requested prefix if this is the first time it is seen
                        indexedResponse.getT1() == 0 && level == 0 &&
                                        (!indexedResponse.getT2().commonPrefixes().isEmpty() || !sortedS3Objects.isEmpty()) &&
                                prefix.endsWith("/") &&
                                contentAccessParams.checkDepth(getPathDepth(basePrefix, prefix)) &&
                                contentAccessParams.matchEntry(prefix)
                                ? Flux.just(createPrefixNode(prefix)) // check if the current prefix ends with '/' and if doesn't create a node for it
                                : Flux.empty(),
                        // add sub-folders
                        Flux.fromIterable(indexedResponse.getT2().commonPrefixes())
                                .map(CommonPrefix::prefix)
                                .filter(contentAccessParams::matchEntry)
                                .map(this::createPrefixNode),
                        // add objects
                                Flux.fromIterable(sortedS3Objects)
                                .filter(o -> contentAccessParams.matchEntry(o.key()))
                                .map(this::createObjectNode),
                        // recurse into subfolders
                        Flux.fromIterable(indexedResponse.getT2().commonPrefixes())
                                .map(CommonPrefix::prefix)
                                .filter(p -> contentAccessParams.checkDepth(getPathDepth(basePrefix, p)))
                                .flatMap(p -> processAllNodes(basePrefix, p, contentAccessParams, level + 1))
                        );
                    }

                });
    }

    @Override
    public InputStream getContentInputStream(String contentLocation) {
        String s3Location = adjustLocation(contentLocation);
        LOG.debug("Get content {}:{}", s3Adapter.getBucket(), s3Location);

        S3TransferManager transferManager = S3TransferManager.builder()
                .s3Client(s3Adapter.getAsyncS3Client())
                .build();

        Download<ResponseBytes<GetObjectResponse>> download = transferManager.download(
                DownloadRequest.builder()
                        .getObjectRequest(b -> b.bucket(s3Adapter.getBucket()).key(s3Location))
                        .responseTransformer(AsyncResponseTransformer.toBytes())
                        .build());
        CompletedDownload<ResponseBytes<GetObjectResponse>> completedDownload = download.completionFuture().join();
        return completedDownload.result().asInputStream();    }

    @Override
    public long streamContentToOutput(String contentLocation, OutputStream outputStream) {
        String s3Location = adjustLocation(contentLocation);
        LOG.debug("Stream from {}:{} to another output stream", s3Adapter.getBucket(), s3Location);

        S3TransferManager transferManager = S3TransferManager.builder()
                .s3Client(s3Adapter.getAsyncS3Client())
                .build();

        Download<ResponsePublisher<GetObjectResponse>> download = transferManager.download(
                DownloadRequest.builder()
                        .getObjectRequest(b -> b
                                .bucket(s3Adapter.getBucket())
                                .key(s3Location))
                        .responseTransformer(AsyncResponseTransformer.toPublisher())
                        .build());
        CompletedDownload<ResponsePublisher<GetObjectResponse>> completedDownload = download.completionFuture().join();
        ResponsePublisher<GetObjectResponse> responsePublisher = completedDownload.result();
        AtomicLong nbytes = new AtomicLong();
        CompletableFuture<Void> drainPublisherFuture = responsePublisher.subscribe(buffer -> nbytes.addAndGet(IOStreamUtils.copyFrom(buffer.array(), outputStream)));
        drainPublisherFuture.join();
        return nbytes.get();
    }

    @Override
    public long writeContent(String contentLocation, InputStream inputStream) {
        String s3Location = adjustLocation(contentLocation);

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(s3Adapter.getBucket())
                .key(s3Location)
                .build();

        // prepare the body - indicating that stream will be provided later
        BlockingInputStreamAsyncRequestBody body = AsyncRequestBody.forBlockingInputStream(null);

        CompletableFuture<PutObjectResponse> putContentPromise = s3Adapter.getAsyncS3Client().putObject(putObjectRequest, body);
        // write the content
        long nbytes = body.writeInputStream(inputStream);
        putContentPromise.join();
        return nbytes;
    }

    @Override
    public void deleteContent(String contentLocation) {
        String s3Location = adjustLocation(contentLocation);

        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(s3Adapter.getBucket())
                .key(s3Location)
                .build();

        CompletableFuture<Void> deletePromise = s3Adapter.getAsyncS3Client().deleteObject(deleteObjectRequest)
                .whenComplete((deleteResponse, ex) -> {
                    if (ex != null) {
                        throw new ContentException("Error deleting content at " + contentLocation, ex);
                    }
                })
                .thenAccept(response -> {
                });

        deletePromise.join();
    }
}
