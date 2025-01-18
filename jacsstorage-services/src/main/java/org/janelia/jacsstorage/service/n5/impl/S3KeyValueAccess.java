package org.janelia.jacsstorage.service.n5.impl;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.s3.S3Adapter;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5URI;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

/**
 * This is based on <a href="https://github.com/saalfeldlab/n5-aws-s3/blob/master/src/main/java/org/janelia/saalfeldlab/n5/s3/AmazonS3KeyValueAccess.java">
 * AmazonS3KeyValueAccess from saalfeldlab github repository</a>
 *
 * The reason for this is that the saalfeldlab implementation is using the old S3 API.
 *
 * So far we only support read operations so all writes will raise an UnsupportedOperationException.
 */
public class S3KeyValueAccess implements KeyValueAccess {

    private final S3Adapter s3Adapter;
    private final String basePrefix;

    S3KeyValueAccess(S3Adapter s3Adapter, String basePrefix) {
        this.s3Adapter = s3Adapter;
        this.basePrefix = basePrefix;
    }

    @Override
    public String[] components(String path) {
        /* If the path is a valid URI with a scheme then use it to get the key. Otherwise,
         * use the path directly, assuming it's a path only */
        String key;
        URI uri = URI.create(path);
        String scheme = uri.getScheme();
        if (StringUtils.isNotBlank(scheme)) {
            key = uri.getPath();
        } else {
            key = path;
        }
        final String[] baseComponents = StringUtils.removeStart(key, '/').split("/");
        if (baseComponents.length <= 1)
            return baseComponents;
        return Arrays.stream(baseComponents)
                .filter(x -> !x.isEmpty())
                .toArray(String[]::new);
    }

    @Override
    public String compose(String... components) {
        if (components == null || components.length == 0)
            return null;

        return normalize(
                Arrays.stream(components)
                        .filter(x -> !x.isEmpty())
                        .collect(Collectors.joining("/"))
        );
    }

    @Override
    public String parent(String path) {
        final String[] components = components(path);
        final String[] parentComponents = Arrays.copyOf(components, components.length - 1);

        return compose(parentComponents);
    }

    @Override
    public String relativize(String path, String base) {
        try {
            /* Must pass absolute path to `uri`. if it already is, this is redundant, and has no impact on the result.
             * 	It's not true that the inputs are always referencing absolute paths, but it doesn't matter in this
             * 	case, since we only care about the relative portion of `path` to `base`, so the result always
             * 	ignores the absolute prefix anyway. */
            final URI baseAsUri = uri("/" + base);
            final URI pathAsUri = uri("/" + path);
            final URI relativeUri = baseAsUri.relativize(pathAsUri);
            return relativeUri.getPath();
        } catch (final URISyntaxException e) {
            throw new N5Exception("Cannot relativize path (" + path + ") with base (" + base + ")", e);
        }
    }

    @Override
    public String normalize(String path) {
        return N5URI.normalizeGroupPath(path);
    }

    /**
     * Create a URI that is the result of resolving the `normalPath` against the containerURI.
     * NOTE: {@link URI#resolve(URI)} always removes the last member of the receiver URIs path.
     * That is undesirable behavior here, as we want to potentially keep the containerURI's
     * full path, and just append `normalPath`. However, it's more complicated, as `normalPath`
     * can also contain leading overlap with the trailing members of `containerURI.getPath()`.
     * To properly resolve the two paths, we generate {@link Path}s from the results of {@link URI#getPath()}
     * and use {@link Path#resolve(Path)}, which results in a guaranteed absolute path, with the
     * desired path resolution behavior. That then is used to construct a new {@link URI}.
     * Any query or fragment portions are ignored. Scheme and Authority are always
     * inherited from containerURI.
     *
     * @param normalPath EITHER a normalized path, or a valid URI
     * @return the URI generated from resolving normalPath against containerURI
     * @throws URISyntaxException if the given normal path is not a valid URI
     */
    @Override
    public URI uri(String normalPath) throws URISyntaxException {
        JADEStorageURI storageURI = s3Adapter.getStorageURI();

        return uriResolve(URI.create(storageURI.getJadeStorage()), normalPath);
    }

    private URI uriResolve(URI uri, String normalPath) throws URISyntaxException {
        if (normalize(normalPath).equals(normalize("/")))
            return uri;

        final Path containerPath = Paths.get(uri.getPath());
        final Path givenPath = Paths.get(new URI(normalPath).getPath());

        final Path resolvedPath = containerPath.resolve(givenPath);
        final String[] pathParts = new String[resolvedPath.getNameCount() + 1];
        pathParts[0] = "/";
        for (int i = 0; i < resolvedPath.getNameCount(); i++) {
            pathParts[i + 1] = resolvedPath.getName(i).toString();
        }
        final String normalResolvedPath = compose(pathParts);

        return new URI(uri.getScheme(), uri.getAuthority(), normalResolvedPath, null, null);
    }

    @Override
    public boolean exists(String normalPath) {
        return isFile(normalPath) || isDirectory(normalPath);
    }

    @Override
    public boolean isDirectory(String normalPath) {
        String s3Key = s3Adapter.getStorageURI().resolve(normalPath).getContentKey();
        // append '/' (if normalPath is not root) to force looking for a prefix not an object
        String key = StringUtils.removeStart(StringUtils.appendIfMissing(s3Key, "/"), '/');
        ListObjectsV2Iterable iterableContent = queryIfExists(key);
        for (ListObjectsV2Response r : iterableContent ) {
            if (r.contents().size() > 0 || r.commonPrefixes().size() > 0) {
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    @Override
    public boolean isFile(String normalPath) {
        String s3Key = s3Adapter.getStorageURI().resolve(normalPath).getContentKey();
        ListObjectsV2Iterable iterableContent = queryIfExists(s3Key);
        for (ListObjectsV2Response r : iterableContent ) {
            if (r.contents().size() > 0) {
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    @Override
    public LockedChannel lockForReading(String normalPath) throws IOException {
        String key = s3Adapter.getStorageURI().resolve(normalPath).getContentKey();
        return new S3ObjectChannel(key);
    }

    @Override
    public LockedChannel lockForWriting(String normalPath) throws IOException {
        throw new UnsupportedOperationException("Write operations are not implemented");
    }

    @Override
    public String[] listDirectories(String normalPath) throws IOException {
        String s3Key = s3Adapter.getStorageURI().resolve(normalPath).getContentKey();
        String prefix = StringUtils.removeStart(StringUtils.appendIfMissing(s3Key, "/"), '/');
        return listAllPrefixes(prefix).toArray(new String[0]);
    }

    @Override
    public String[] list(String normalPath) throws IOException {
        String s3Key = s3Adapter.getStorageURI().resolve(normalPath).getContentKey();
        return listPrefixesAndObjects(s3Key).toArray(new String[0]);
    }

    @Override
    public void createDirectories(String normalPath) throws IOException {
        throw new UnsupportedOperationException("Write operations are not implemented");
    }

    @Override
    public void delete(String normalPath) throws IOException {
        throw new UnsupportedOperationException("Write operations are not implemented");
    }

    private List<String> listAllPrefixes(String s3Prefix)  {
        List<String> allPrefixes = new ArrayList<>();
        ListObjectsV2Request initialRequest = ListObjectsV2Request.builder()
                .bucket(s3Adapter.getBucket())
                .prefix(s3Prefix)
                .delimiter("/")
                .build();
        ListObjectsV2Iterable listObjectsResponses = s3Adapter.getSyncS3Client().listObjectsV2Paginator(initialRequest);
        for (ListObjectsV2Response r : listObjectsResponses) {
            for (CommonPrefix commonPrefix : r.commonPrefixes()) {
                String relativePath = relativize(commonPrefix.prefix(), s3Prefix);
                if (StringUtils.isNotEmpty(relativePath)) {
                    allPrefixes.add(relativePath);
                }
            }
        }
        return allPrefixes;
    }

    private List<String> listPrefixesAndObjects(String s3Location) {
        Queue<String> prefixQueue = new LinkedList<>();
        prefixQueue.add(s3Location);

        List<String> allResults = new ArrayList<>();
        while (!prefixQueue.isEmpty()) {
            String currentPrefix = prefixQueue.poll();

            ListObjectsV2Request initialRequest = ListObjectsV2Request.builder()
                    .bucket(s3Adapter.getBucket())
                    .prefix(currentPrefix)
                    .delimiter("/")
                    .build();
            ListObjectsV2Iterable listObjectsResponses = s3Adapter.getSyncS3Client().listObjectsV2Paginator(initialRequest);
            for (ListObjectsV2Response r : listObjectsResponses) {

                for (S3Object s3Object : r.contents()) {
                    String relativePath = relativize(s3Object.key(), s3Location);
                    if (StringUtils.isEmpty(relativePath)) {
                        allResults.add(relativePath);
                    }
                }

                for (CommonPrefix commonPrefix : r.commonPrefixes()) {
                    prefixQueue.add(commonPrefix.prefix());
                    String relativePath = relativize(commonPrefix.prefix(), s3Location);
                    if (StringUtils.isEmpty(relativePath)) {
                        allResults.add(relativePath);
                    }
                }
            }
        }
        return allResults;
    }

    private ListObjectsV2Iterable queryIfExists(final String keyOrPrefix) {
        ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                .bucket(s3Adapter.getBucket())
                .prefix(keyOrPrefix)
                .delimiter("/")
                .maxKeys(1)
                .build();

        return s3Adapter.getSyncS3Client().listObjectsV2Paginator(listObjectsRequest);
    }

    private class S3ObjectChannel implements LockedChannel {

        private final String objectKey;
        private final List<Closeable> resources = new ArrayList<>();

        private S3ObjectChannel(String objectKey) {
            this.objectKey = objectKey;
        }

        @Override
        public Reader newReader() {
            final InputStreamReader reader = new InputStreamReader(createInputStream(), StandardCharsets.UTF_8);
            synchronized (resources) {
                resources.add(reader);
            }
            return reader;
        }

        @Override
        public InputStream newInputStream() {
            final InputStream inputStream = createInputStream();
            synchronized (resources) {
                resources.add(inputStream);
            }
            return inputStream;
        }

        private InputStream createInputStream() {
            try {
                GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                        .bucket(s3Adapter.getBucket())
                        .key(objectKey)
                        .build();
                return s3Adapter.getSyncS3Client().getObject(getObjectRequest, ResponseTransformer.toInputStream());
            } catch (NoSuchKeyException e) {
                throw new N5Exception.N5NoSuchKeyException(objectKey, e);
            }
        }

        @Override
        public Writer newWriter() {
            throw new UnsupportedOperationException("Write operations not supported");
        }

        @Override
        public OutputStream newOutputStream() {
            throw new UnsupportedOperationException("Write operations not supported");
        }

        @Override
        public void close() throws IOException {
            synchronized (resources) {
                for (final Closeable resource : resources)
                    resource.close();
                resources.clear();
            }
        }
    }

}
