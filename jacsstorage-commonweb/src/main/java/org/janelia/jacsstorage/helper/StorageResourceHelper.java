package org.janelia.jacsstorage.helper;

import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacsstorage.coreutils.PathUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.StoragePathURI;
import org.janelia.jacsstorage.model.jacsstorage.StorageRelativePath;
import org.janelia.jacsstorage.model.support.JacsSubjectHelper;
import org.janelia.jacsstorage.rest.Constants;
import org.janelia.jacsstorage.rest.ErrorResponse;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriBuilder;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Timed
public class StorageResourceHelper {
    private static final Logger LOG = LoggerFactory.getLogger(StorageResourceHelper.class);

    private final DataStorageService dataStorageService;
    private final StorageLookupService storageLookupService;
    private final StorageVolumeManager storageVolumeManager;

    public StorageResourceHelper(DataStorageService dataStorageService, StorageLookupService storageLookupService, StorageVolumeManager storageVolumeManager) {
        this.dataStorageService = dataStorageService;
        this.storageLookupService = storageLookupService;
        this.storageVolumeManager = storageVolumeManager;
    }

    public Response.ResponseBuilder handleResponseForFullDataPathParam(StoragePathURI storagePathURI,
                                                                       BiFunction<JacsBundle, String, Response.ResponseBuilder> bundleBasedResponseHandler,
                                                                       BiFunction<JacsStorageVolume, StorageRelativePath, Response.ResponseBuilder> volumeBasedResponseHandler) {
        return handleResponseForFullDataPathParam(
                storagePathURI,
                bundleBasedResponseHandler,
                volumeBasedResponseHandler,
                () -> Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No managed volume found for " + storagePathURI))
                        .type(MediaType.APPLICATION_JSON)
        );
    }

    public Response.ResponseBuilder handleResponseForFullDataPathParam(StoragePathURI storagePathURI,
                                                                       BiFunction<JacsBundle, String, Response.ResponseBuilder> bundleBasedResponseHandler,
                                                                       BiFunction<JacsStorageVolume, StorageRelativePath, Response.ResponseBuilder> volumeBasedResponseHandler,
                                                                       Supplier<Response.ResponseBuilder> storageNotFoundHandler) {
        return getStorageVolumeForURI(storagePathURI)
                .map(storageVolume -> handleResponseForFullDataPathParam(storageVolume,
                        storagePathURI,
                        bundleBasedResponseHandler,
                        volumeBasedResponseHandler))
                .orElseGet(storageNotFoundHandler)
                ;
    }

    private Response.ResponseBuilder handleResponseForFullDataPathParam(JacsStorageVolume storageVolume,
                                                                        StoragePathURI storagePathURI,
                                                                        BiFunction<JacsBundle, String, Response.ResponseBuilder> bundleBasedResponseHandler,
                                                                        BiFunction<JacsStorageVolume, StorageRelativePath, Response.ResponseBuilder> volumeBasedResponseHandler) {
        if (storagePathURI.isEmpty()) {
            LOG.warn("No storage path {} has been specified for {}", storagePathURI, storageVolume);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("Empty storage path: " + storagePathURI))
                    .type(MediaType.APPLICATION_JSON)
                    ;
        }
        return storageVolume.getStoragePathRelativeToStorageRoot(storagePathURI.getStoragePath())
                .map(storageRelativeFileDataPath -> {
                    // check if the first path component after the storage prefix is a bundle ID
                    Path dataPath = Paths.get(storageRelativeFileDataPath.getPath());
                    int fileDataPathComponents = dataPath.getNameCount();
                    JacsBundle dataBundle = null;
                    try {
                        if (fileDataPathComponents > 0) {
                            String bundleIdComponent = dataPath.getName(0).toString();
                            Number bundleId = new BigInteger(bundleIdComponent);
                            dataBundle = storageLookupService.getDataBundleById(bundleId);
                        }
                    } catch (NumberFormatException e) {
                        LOG.debug("Path {} is not a data bundle - first component is not numeric", storageRelativeFileDataPath);
                    }
                    if (dataBundle == null) {
                        return volumeBasedResponseHandler.apply(storageVolume, storageRelativeFileDataPath);
                    } else {
                        String dataEntryPath = fileDataPathComponents > 1
                                ? dataPath.subpath(1, fileDataPathComponents).toString()
                                : "";
                        return bundleBasedResponseHandler.apply(dataBundle, dataEntryPath);
                    }
                })
                .orElseGet(() -> {
                    LOG.warn("Could not find the path for {} relative to {}", storagePathURI, storageVolume);
                    return Response
                            .status(Response.Status.NOT_FOUND)
                            .entity(new ErrorResponse("Empty storage path: " + storagePathURI))
                            .type(MediaType.APPLICATION_JSON)
                            ;
                });
    }

    public Optional<JacsStorageVolume> getStorageVolumeForURI(StoragePathURI dirStorageURI) {
        if (dirStorageURI.isEmpty()) {
            return Optional.empty();
        } else {
            return getStorageVolumeForDir(dirStorageURI.getStoragePath());
        }
    }

    private Optional<JacsStorageVolume> getStorageVolumeForDir(String dataPath) {
        try {
            JacsStorageVolume storageVolume = retrieveStorageVolumeForDir(dataPath);
            if (storageVolume != null)
                return Optional.of(storageVolume);
            else
                return Optional.empty();
        } catch (Exception e) {
            LOG.error("Error retrieving volume for {}", dataPath, e);
            return Optional.empty();
        }
    }

    private JacsStorageVolume retrieveStorageVolumeForDir(String dirName) {
        List<JacsStorageVolume> storageVolumes = storageVolumeManager.findVolumes(new StorageQuery().setDataStoragePath(dirName));
        if (storageVolumes.isEmpty()) {
            LOG.warn("No volume found to match {}", dirName);
        } else if (storageVolumes.size() > 1) {
            // This case may be tricky especially for locally mounted volumes (non-shared) when the path
            // exists only on certain volumes. In order to find out we would actually have to go to the actual agent
            // to find if the directory exists but we are not doing that yet
            // E.g. we have 2 non-shared volumes served by node1 and node2 and both use the same root directory /data/jacsstorage
            // Now if one searches for a directory /data/jacsstorage/dirOnNode2Only, which is only available on node1
            // the master may redirect the caller to the wrong agent - node1 instead of node2 because at this point it has no information
            // whether the directory really exists
            LOG.debug("More than one storage volumes found for {} -> {}", dirName, storageVolumes);
        } else {
            LOG.debug("Found exactly one volume {} to for {}", storageVolumes, dirName);
        }
        return storageVolumes.stream().findAny().orElse(null);
    }

    public Response.ResponseBuilder checkContentFromFile(JacsStorageVolume storageVolume, StorageRelativePath dataEntryName, boolean dirOnly) {
        if (!storageVolume.hasPermission(JacsStoragePermission.READ)) {
            return Response.status(Response.Status.FORBIDDEN)
                    .header("Content-Length", 0);
        }
        return storageVolume.getDataStorageAbsolutePath(dataEntryName)
                .filter(dataEntryPath -> Files.exists(dataEntryPath) && (!dirOnly || Files.isDirectory(dataEntryPath)))
                .map(dataEntryPath -> {
                    long fileSize;
                    if (Files.isRegularFile(dataEntryPath)) {
                        fileSize = PathUtils.getSize(dataEntryPath, 0);
                    } else {
                        fileSize = 0;
                    }
                    return Response.ok()
                            .header("Content-Length", fileSize);
                })
                .orElseGet(() -> Response
                        .status(Response.Status.NOT_FOUND)
                        .header("Content-Length", 0))
                ;
    }

    public Response.ResponseBuilder retrieveContentFromFile(JacsStorageVolume storageVolume, ContentFilterParams filterParams, StorageRelativePath dataEntryName) {
        if (!storageVolume.hasPermission(JacsStoragePermission.READ)) {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolume.getName() + " to read " + dataEntryName))
                    .type(MediaType.APPLICATION_JSON)
                    ;
        }
        return storageVolume.getDataStorageAbsolutePath(dataEntryName)
                .filter(dataEntryPath -> Files.exists(dataEntryPath))
                .map(dataEntryPath -> {
                    JacsStorageFormat storageFormat = Files.isRegularFile(dataEntryPath) ? JacsStorageFormat.SINGLE_DATA_FILE : JacsStorageFormat.DATA_DIRECTORY;
                    long fileSize = dataStorageService.estimateDataEntrySize(dataEntryPath, "", storageFormat, filterParams);
                    StreamingOutput fileStream = output -> {
                        try {
                            dataStorageService.retrieveDataStream(dataEntryPath, storageFormat, filterParams, output);
                        } catch (Exception e) {
                            LOG.error("Error streaming data file content for {}", dataEntryPath, e);
                            throw new WebApplicationException("Error streaming content for " + dataEntryName, e, Response.Status.INTERNAL_SERVER_ERROR);
                        }
                    };
                    return Response
                            .ok(fileStream, MediaType.APPLICATION_OCTET_STREAM)
                            .header("Content-Length", fileSize)
                            .header("Content-Disposition", "attachment; filename = " + dataEntryPath.getFileName())
                            ;
                })
                .orElseGet(() -> Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No path found for " + dataEntryName + " on volume " + storageVolume.getName()))
                        .type(MediaType.APPLICATION_JSON))
                ;
    }

    public Response.ResponseBuilder retrieveContentInfoFromFile(JacsStorageVolume storageVolume, StorageRelativePath dataEntryName) {
        if (!storageVolume.hasPermission(JacsStoragePermission.READ)) {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolume.getName() + " to read " + dataEntryName))
                    .type(MediaType.APPLICATION_JSON)
                    ;
        }
        return storageVolume.getDataStorageAbsolutePath(dataEntryName)
                .filter(dataEntryPath -> Files.exists(dataEntryPath))
                .map(dataEntryPath -> {
                    JacsStorageFormat storageFormat = Files.isRegularFile(dataEntryPath) ? JacsStorageFormat.SINGLE_DATA_FILE : JacsStorageFormat.DATA_DIRECTORY;
                    return Response
                            .ok(dataStorageService.getDataEntryInfo(dataEntryPath, "", storageFormat))
                            ;
                })
                .orElseGet(() -> Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No path found for " + dataEntryName + " on volume " + storageVolume.getName()))
                        .type(MediaType.APPLICATION_JSON))
                ;

    }

    public Response.ResponseBuilder checkContentFromDataBundle(JacsBundle dataBundle, String dataEntryPath, boolean collectionOnly) {
        return dataStorageService.streamDataEntries(dataBundle.getRealStoragePath(), dataEntryPath, dataBundle.getStorageFormat(), 1)
                .findFirst()
                .filter(dn -> !collectionOnly || dn.isCollectionFlag())
                .map(dn -> Response.ok().header("Content-Length", dn.getSize()))
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND).header("Content-Length", 0))
                ;
    }

    public Response.ResponseBuilder retrieveContentFromDataBundle(JacsBundle dataBundle, ContentFilterParams filterParams, String dataEntryPath) {
        long fileSize = dataStorageService.estimateDataEntrySize(dataBundle.getRealStoragePath(), dataEntryPath, dataBundle.getStorageFormat(), filterParams);
        StreamingOutput bundleStream = output -> {
            try {
                dataStorageService.readDataEntryStream(
                        dataBundle.getRealStoragePath(),
                        dataEntryPath,
                        dataBundle.getStorageFormat(),
                        filterParams,
                        output);
            } catch (Exception e) {
                LOG.error("Error streaming data file content for {}:{}", dataBundle, dataEntryPath, e);
                throw new WebApplicationException(e);
            }
        };
        return Response
                .ok(bundleStream, MediaType.APPLICATION_OCTET_STREAM)
                .header("Content-Length", fileSize)
                .header("Content-Disposition", "attachment; filename = " + JacsSubjectHelper.getNameFromSubjectKey(dataBundle.getOwnerKey()) + "-" + dataBundle.getName() + "/" + dataEntryPath)
                ;
    }

    public Response.ResponseBuilder retrieveContentInfoFromDataBundle(JacsBundle dataBundle, String dataEntryPath) {
        return Response
                .ok(dataStorageService.getDataEntryInfo(dataBundle.getRealStoragePath(), dataEntryPath, dataBundle.getStorageFormat()))
                ;
    }

    public Response.ResponseBuilder listContentFromDataBundle(JacsBundle dataBundle, URI baseURI, String dataEntryPath, int depth, long offset, long length) {
        Stream<DataNodeInfo> dataBundleContentStream;
        if (length > 0) {
            dataBundleContentStream = dataStorageService.streamDataEntries(dataBundle.getRealStoragePath(), dataEntryPath, dataBundle.getStorageFormat(), depth)
                    .skip(offset >= 0 ? offset : 0)
                    .limit(length);
        } else {
            dataBundleContentStream = dataStorageService.streamDataEntries(dataBundle.getRealStoragePath(), dataEntryPath, dataBundle.getStorageFormat(), depth)
                    .skip(offset >= 0 ? offset : 0);
        }
        return Response
                .ok(dataBundleContentStream.peek(dn -> {
                    dn.setNumericStorageId(dataBundle.getId());
                    dn.setStorageRootPathURI(dataBundle.getStorageURI());
                    dn.setNodeAccessURL(UriBuilder.fromUri(baseURI)
                            .path(Constants.AGENTSTORAGE_URI_PATH)
                            .path(dataBundle.getId().toString())
                            .path("data_content")
                            .path(dn.getNodeRelativePath())
                            .build()
                            .toString());
                    dn.setNodeInfoURL(UriBuilder.fromUri(baseURI)
                            .path(Constants.AGENTSTORAGE_URI_PATH)
                            .path(dataBundle.getId().toString())
                            .path("data_info")
                            .path(dn.getNodeRelativePath())
                            .build()
                            .toString());
                }).collect(Collectors.toList()), MediaType.APPLICATION_JSON)
                ;
    }

    public Response.ResponseBuilder listContentFromPath(JacsStorageVolume storageVolume, URI baseURI, StorageRelativePath dataEntryName, int depth, long offset, long length) {
        if (!storageVolume.hasPermission(JacsStoragePermission.READ)) {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolume.getName() + " to list " + dataEntryName))
                    .type(MediaType.APPLICATION_JSON)
                    ;
        }
        return storageVolume.getDataStorageAbsolutePath(dataEntryName)
                .filter(dataEntryPath -> Files.exists(dataEntryPath))
                .map(dataEntryPath -> {
                    JacsStorageFormat storageFormat = Files.isRegularFile(dataEntryPath) ? JacsStorageFormat.SINGLE_DATA_FILE : JacsStorageFormat.DATA_DIRECTORY;
                    Stream<DataNodeInfo> entriesStream;
                    if (length > 0) {
                        entriesStream = dataStorageService.streamDataEntries(dataEntryPath, "", storageFormat, depth)
                                .skip(offset >= 0 ? offset : 0)
                                .limit(length);
                    } else {
                        entriesStream = dataStorageService.streamDataEntries(dataEntryPath, "", storageFormat, depth)
                                .skip(offset >= 0 ? offset : 0);
                    }
                    return Response
                            .ok(entriesStream
                                            .map(dn -> {
                                                String dataNodeAbsolutePath = dataEntryPath.resolve(dn.getNodeRelativePath()).toString();
                                                Path dataNodeRelativePath = storageVolume.getPathRelativeToBaseStorageRoot(dataNodeAbsolutePath);
                                                DataNodeInfo newDataNode = new DataNodeInfo();
                                                newDataNode.setStorageId(dn.getStorageId());
                                                newDataNode.setStorageRootLocation(storageVolume.getBaseStorageRootDir());
                                                newDataNode.setStorageRootPathURI(storageVolume.getStorageURI());
                                                newDataNode.setNodeAccessURL(UriBuilder.fromUri(baseURI)
                                                        .path(Constants.AGENTSTORAGE_URI_PATH)
                                                        .path("storage_volume")
                                                        .path(storageVolume.getId().toString())
                                                        .path("data_content")
                                                        .path(dataNodeRelativePath.toString())
                                                        .build().toString());
                                                newDataNode.setNodeInfoURL(UriBuilder.fromUri(baseURI)
                                                        .path(Constants.AGENTSTORAGE_URI_PATH)
                                                        .path("storage_volume")
                                                        .path(storageVolume.getId().toString())
                                                        .path("data_info")
                                                        .path(dataNodeRelativePath.toString())
                                                        .build().toString());
                                                newDataNode.setNodeRelativePath(dataNodeRelativePath.toString());
                                                newDataNode.setSize(dn.getSize());
                                                newDataNode.setMimeType(dn.getMimeType());
                                                newDataNode.setCollectionFlag(dn.isCollectionFlag());
                                                newDataNode.setCreationTime(dn.getCreationTime());
                                                newDataNode.setLastModified(dn.getLastModified());
                                                return newDataNode;
                                            })
                                            .collect(Collectors.toList()),
                                    MediaType.APPLICATION_JSON)
                            ;
                })
                .orElseGet(() -> Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No path found for " + dataEntryName + " on volume " + storageVolume.getName()))
                        .type(MediaType.APPLICATION_JSON))
                ;
    }

    public Response.ResponseBuilder storeDataBundleContent(JacsBundle dataBundle, URI baseURI, String dataEntryName, Consumer<Long> dataBundleUpdater, InputStream contentStream) {
        Path storagePath = dataBundle.getRealStoragePath();
        LOG.info("Create {} on {}", dataEntryName, storagePath);
        long newFileEntrySize = dataStorageService.writeDataEntryStream(storagePath, dataEntryName, dataBundle.getStorageFormat(), contentStream);
        dataBundleUpdater.accept(newFileEntrySize);
        URI newContentURI = UriBuilder.fromUri(baseURI)
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path(dataBundle.getId().toString())
                .path("data_content")
                .path(dataEntryName)
                .build();
        DataNodeInfo newDataNode = new DataNodeInfo();
        newDataNode.setNumericStorageId(dataBundle.getId());
        newDataNode.setStorageRootLocation(dataBundle.getRealStoragePath().toString());
        newDataNode.setStorageRootPathURI(dataBundle.getStorageURI());
        newDataNode.setNodeAccessURL(newContentURI.toString());
        newDataNode.setNodeInfoURL(UriBuilder.fromUri(baseURI)
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path(dataBundle.getId().toString())
                .path("data_info")
                .path(dataEntryName)
                .build().toString());
        newDataNode.setNodeRelativePath(dataEntryName);
        newDataNode.setCollectionFlag(false);
        return Response
                .created(newContentURI)
                .entity(newDataNode);
    }

    public Response.ResponseBuilder storeFileContent(JacsStorageVolume storageVolume, URI baseURI, StorageRelativePath dataEntryName, InputStream contentStream) {
        if (!storageVolume.hasPermission(JacsStoragePermission.WRITE)) {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolume.getName() + " to list " + dataEntryName))
                    .type(MediaType.APPLICATION_JSON)
                    ;
        }
        return storageVolume.getDataStorageAbsolutePath(dataEntryName)
                .map(dataEntryPath -> {
                    LOG.info("Create {} on {}", dataEntryPath, storageVolume);
                    dataStorageService.writeDataEntryStream(dataEntryPath, "", JacsStorageFormat.SINGLE_DATA_FILE, contentStream);
                    Path dataNodeRelativePath = storageVolume.getPathRelativeToBaseStorageRoot(dataEntryPath.toString());
                    URI newContentURI = UriBuilder.fromUri(baseURI)
                            .path(Constants.AGENTSTORAGE_URI_PATH)
                            .path("storage_volume")
                            .path(storageVolume.getId().toString())
                            .path("data_content")
                            .path(dataNodeRelativePath.toString())
                            .build();
                    DataNodeInfo newDataNode = new DataNodeInfo();
                    newDataNode.setStorageRootLocation(storageVolume.getBaseStorageRootDir());
                    newDataNode.setStorageRootPathURI(storageVolume.getStorageURI());
                    newDataNode.setNodeAccessURL(newContentURI.toString());
                    newDataNode.setNodeInfoURL(UriBuilder.fromUri(baseURI)
                            .path(Constants.AGENTSTORAGE_URI_PATH)
                            .path("storage_volume")
                            .path(storageVolume.getId().toString())
                            .path("data_info")
                            .path(dataNodeRelativePath.toString())
                            .build().toString());
                    newDataNode.setNodeRelativePath(dataNodeRelativePath.toString());
                    newDataNode.setCollectionFlag(false);
                    return Response
                            .created(newContentURI)
                            .entity(newDataNode);

                })
                .orElseGet(() -> Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("Could not create entry " + dataEntryName + " on volume " + storageVolume.getName()))
                        .type(MediaType.APPLICATION_JSON))
                ;
    }

    public Response.ResponseBuilder removeContentFromDataBundle(JacsBundle dataBundle, String dataEntryName, JacsCredentials credentials, Consumer<Long> dataBundleUpdater) {
        try {
            if (!credentials.getSubjectKey().equals(dataBundle.getOwnerKey()) || dataBundle.isLinkedStorage()) {
                return Response
                        .status(Response.Status.FORBIDDEN)
                        .entity(new ErrorResponse("No delete permission from " + dataBundle.getName() + " for " + credentials.getSubjectKey()))
                        .type(MediaType.APPLICATION_JSON)
                        ;
            }
            Path storagePath = dataBundle.getRealStoragePath();
            LOG.info("Delete {} from {}", dataEntryName, storagePath);
            long freedEntrySize = dataStorageService.deleteStorageEntry(storagePath, dataEntryName, dataBundle.getStorageFormat());
            dataBundleUpdater.accept(freedEntrySize);
            return Response.noContent();
        } catch (Exception e) {
            LOG.warn("Error while trying to delete {} from {}", dataEntryName, dataBundle, e);
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Delete bundle entry error: " + dataBundle.getId() + "/" + dataEntryName))
                    .type(MediaType.APPLICATION_JSON)
                    ;
        }
    }

    public Response.ResponseBuilder removeFileContentFromVolume(JacsStorageVolume storageVolume, StorageRelativePath dataEntryName) {
        if (!storageVolume.hasPermission(JacsStoragePermission.DELETE)) {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No delete permission for volume " + storageVolume.getName() + " to delete " + dataEntryName))
                    .type(MediaType.APPLICATION_JSON)
                    ;
        }
        return storageVolume.getDataStorageAbsolutePath(dataEntryName)
                .filter(dataEntryPath -> Files.exists(dataEntryPath))
                .map(dataEntryPath -> {
                    try {
                        LOG.info("Delete {}({}) from {}", dataEntryName, dataEntryPath, storageVolume);
                        dataStorageService.deleteStoragePath(dataEntryPath);
                        return Response.noContent();
                    } catch (Exception e) {
                        LOG.warn("File error while trying to delete {}({}) from {}", dataEntryName, dataEntryPath, storageVolume);
                        return Response
                                .status(Response.Status.INTERNAL_SERVER_ERROR)
                                .entity(new ErrorResponse("Internal error while trying to delete " + dataEntryName + " from " + storageVolume))
                                .type(MediaType.APPLICATION_JSON)
                                ;
                    }
                })
                .orElseGet(() -> Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("Invalid storage volume " + storageVolume.getName() + " for deleting " + dataEntryName))
                        .type(MediaType.APPLICATION_JSON))
                ;
    }
}
