package org.janelia.jacsstorage.helper;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.coreutils.PathUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.expr.ExprHelper;
import org.janelia.jacsstorage.expr.MatchingResult;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.StoragePathURI;
import org.janelia.jacsstorage.rest.Constants;
import org.janelia.jacsstorage.rest.ErrorResponse;
import org.janelia.jacsstorage.model.support.JacsSubjectHelper;
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
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Timed
public class StorageResourceHelper {
    private static final Logger LOG = LoggerFactory.getLogger(StorageResourceHelper.class);

    private static final int N_VOL_DIR_COMPONENTS = 4;
    private static final Cache<String, JacsStorageVolume> VOLUMES_BY_PATH_CACHE = CacheBuilder.newBuilder()
            .maximumSize(100)
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .build();

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
                                                                       BiFunction<JacsStorageVolume, String, Response.ResponseBuilder> volumeBasedResponseHandler) {
        return handleResponseForFullDataPathParam(
                storagePathURI,
                bundleBasedResponseHandler,
                volumeBasedResponseHandler,
                () -> Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No managed volume found for " + storagePathURI))
        );
    }

    public Response.ResponseBuilder handleResponseForFullDataPathParam(StoragePathURI storagePathURI,
                                                                       BiFunction<JacsBundle, String, Response.ResponseBuilder> bundleBasedResponseHandler,
                                                                       BiFunction<JacsStorageVolume, String, Response.ResponseBuilder> volumeBasedResponseHandler,
                                                                       Supplier<Response.ResponseBuilder> storageNotFoundHandler) {
        return getStorageVolumeForURI(storagePathURI)
                .map(storageVolume -> handleResponseForFullDataPathParam(storageVolume,
                        storagePathURI,
                        bundleBasedResponseHandler,
                        volumeBasedResponseHandler))
                .orElseGet(() -> Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No managed volume found for " + storagePathURI)))
                ;
    }

    private Response.ResponseBuilder handleResponseForFullDataPathParam(JacsStorageVolume storageVolume,
                                                                        StoragePathURI storagePathURI,
                                                                        BiFunction<JacsBundle, String, Response.ResponseBuilder> bundleBasedResponseHandler,
                                                                        BiFunction<JacsStorageVolume, String, Response.ResponseBuilder> volumeBasedResponseHandler) {
        if (storagePathURI.isEmpty()) {
            LOG.warn("No storage path {} has been specified for {}", storagePathURI, storageVolume);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("Empty storage path: " + storagePathURI))
                    ;
        }
        return storageVolume.getRelativePathToBaseStorageRoot(storagePathURI.getStoragePath())
                .map(storageRelativeFileDataPath -> {
                    // check if the first path component after the storage prefix is a bundle ID
                    int fileDataPathComponents = storageRelativeFileDataPath.getNameCount();
                    JacsBundle dataBundle = null;
                    try {
                        if (fileDataPathComponents > 0) {
                            String bundleIdComponent = storageRelativeFileDataPath.getName(0).toString();
                            Number bundleId = new BigInteger(bundleIdComponent);
                            dataBundle = storageLookupService.getDataBundleById(bundleId);
                        }
                    } catch (NumberFormatException e) {
                        LOG.debug("Path {} is not a data bundle - first component is not numeric", storageRelativeFileDataPath);
                    }
                    if (dataBundle == null) {
                        return volumeBasedResponseHandler.apply(storageVolume, storageRelativeFileDataPath.toString());
                    } else {
                        String dataEntryPath = fileDataPathComponents > 1
                                ? storageRelativeFileDataPath.subpath(1, fileDataPathComponents).toString()
                                : "";
                        return bundleBasedResponseHandler.apply(dataBundle, dataEntryPath);
                    }
                })
                .orElseGet(() -> {
                    LOG.warn("Could not find the path for {} relative to {}", storagePathURI, storageVolume);
                    return Response
                            .status(Response.Status.NOT_FOUND)
                            .entity(new ErrorResponse("Empty storage path: " + storagePathURI))
                            ;
                });
    }

    public Optional<JacsStorageVolume> getStorageVolumeForURI(StoragePathURI dirStorageURI) {
        if (dirStorageURI.isEmpty()) {
            return Optional.empty();
        } else {
            Path storagePath = Paths.get(dirStorageURI.getStoragePath());
            int storagePathComponents = storagePath.getNameCount();
            String storagePathDirName;
            if (storagePathComponents < N_VOL_DIR_COMPONENTS) {
                storagePathDirName = storagePath.toString();
            } else {
                if (storagePath.getRoot() == null) {
                    storagePathDirName = storagePath.subpath(0, N_VOL_DIR_COMPONENTS).toString();
                } else {
                    storagePathDirName = storagePath.getRoot().resolve(storagePath.subpath(0, N_VOL_DIR_COMPONENTS)).toString();
                }
            }
            return getCachedStorageVolumeForDir(storagePathDirName);
        }
    }

    private Optional<JacsStorageVolume> getCachedStorageVolumeForDir(String dirName) {
        try {
            JacsStorageVolume storageVolume = VOLUMES_BY_PATH_CACHE.get(dirName, () -> retrieveStorageVolumeForDir(dirName));
            return Optional.of(storageVolume);
        } catch (Exception e) {
            LOG.error("Error retrieving volume for {}", dirName, e);
            return Optional.empty();
        }
    }

    private JacsStorageVolume retrieveStorageVolumeForDir(String dirName) {
        List<JacsStorageVolume> storageVolumes = storageVolumeManager.getManagedVolumes(new StorageQuery().setDataStoragePath(dirName));
        if (storageVolumes.isEmpty()) {
            LOG.warn("No volume found to match {}", dirName);
            return null;
        } else if (storageVolumes.size() > 1) {
            LOG.warn("More than one storage volumes found for {} -> {}", dirName, storageVolumes);
        }
        return storageVolumes.get(0);
    }

    public Response.ResponseBuilder checkContentFromFile(JacsStorageVolume storageVolume, String dataEntryName) {
        if (!storageVolume.hasPermission(JacsStoragePermission.READ)) {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolume.getName() + " to read " + dataEntryName))
                    ;
        }
        Path dataEntryPath = storageVolume.getFullDataPathFromBaseStorageRoot(dataEntryName);
        if (Files.exists(dataEntryPath)) {
            return Response
                    .ok()
                    .header("Content-Length", PathUtils.getSize(Paths.get(storageVolume.getBaseStorageRootDir()).resolve(dataEntryPath)));
        } else {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No path found for " + dataEntryName + " on volume " + storageVolume.getName()))
                    ;
        }
    }

    public Response.ResponseBuilder retrieveContentFromFile(JacsStorageVolume storageVolume, String dataEntryName) {
        if (!storageVolume.hasPermission(JacsStoragePermission.READ)) {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolume.getName() + " to read " + dataEntryName))
                    ;
        }
        Path dataEntryPath = storageVolume.getFullDataPathFromBaseStorageRoot(dataEntryName);
        if (Files.exists(dataEntryPath)) {
            JacsStorageFormat storageFormat = Files.isRegularFile(dataEntryPath) ? JacsStorageFormat.SINGLE_DATA_FILE : JacsStorageFormat.DATA_DIRECTORY;
            long fileSize = PathUtils.getSize(dataEntryPath);
            StreamingOutput fileStream = output -> {
                try {
                    dataStorageService.retrieveDataStream(dataEntryPath, storageFormat, output);
                } catch (Exception e) {
                    LOG.error("Error streaming data file content for {}", dataEntryPath, e);
                    throw new WebApplicationException(e);
                }
            };
            return Response
                    .ok(fileStream, MediaType.APPLICATION_OCTET_STREAM)
                    .header("Content-Length", fileSize)
                    .header("Content-Disposition", "attachment; filename = " + dataEntryPath.getFileName())
                    ;

        } else {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No path found for " + dataEntryName + " on volume " + storageVolume.getName()))
                    ;
        }

    }

    public Response.ResponseBuilder retrieveContentFromDataBundle(JacsBundle dataBundle, String dataEntryPath) {
        long fileSize = PathUtils.getSize(dataBundle.getRealStoragePath().resolve(StringUtils.defaultIfBlank(dataEntryPath, "")));
        StreamingOutput bundleStream = output -> {
            try {
                dataStorageService.readDataEntryStream(
                        dataBundle.getRealStoragePath(),
                        dataEntryPath,
                        dataBundle.getStorageFormat(),
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

    public Response.ResponseBuilder listContentFromDataBundle(JacsBundle dataBundle, URI baseURI, String dataEntryPath, int depth) {
        List<DataNodeInfo> dataBundleContent = dataStorageService.listDataEntries(dataBundle.getRealStoragePath(), dataEntryPath, dataBundle.getStorageFormat(), depth);
        if (CollectionUtils.isNotEmpty(dataBundleContent)) {
            dataBundleContent.forEach(dn -> {
                dn.setNumericStorageId(dataBundle.getId());
                dn.setStorageRootPathURI(dataBundle.getStorageURI());
                dn.setNodeAccessURL(UriBuilder.fromUri(baseURI)
                        .path(Constants.AGENTSTORAGE_URI_PATH)
                        .path(dataBundle.getId().toString())
                        .path("entry_content")
                        .path(dn.getNodeRelativePath())
                        .build()
                        .toString());
            });
        }
        return Response
                .ok(dataBundleContent, MediaType.APPLICATION_JSON)
                ;
    }

    public Response.ResponseBuilder listContentFromPath(JacsStorageVolume storageVolume, URI baseURI, String dataEntryName, int depth) {
        if (!storageVolume.hasPermission(JacsStoragePermission.READ)) {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolume.getName() + " to list " + dataEntryName))
                    ;
        }
        Path dataEntryPath = Paths.get(dataEntryName);
        Path dataPath = storageVolume.getFullDataPathFromBaseStorageRoot(dataEntryName);
        if (Files.exists(dataPath)) {
            JacsStorageFormat storageFormat = Files.isRegularFile(dataPath) ? JacsStorageFormat.SINGLE_DATA_FILE : JacsStorageFormat.DATA_DIRECTORY;
            List<DataNodeInfo> entries = dataStorageService.listDataEntries(dataPath, "", storageFormat, depth);
            return Response
                    .ok(entries.stream()
                                    .map(dn -> {
                                        String dataNodePathRelativeToVolRoot = dataEntryPath.resolve(dn.getNodeRelativePath()).toString();
                                        URI dataNodeAccessURI = UriBuilder.fromUri(baseURI)
                                                .path(Constants.AGENTSTORAGE_URI_PATH)
                                                .path("storage_volume")
                                                .path(storageVolume.getId().toString())
                                                .path(dataNodePathRelativeToVolRoot)
                                                .build();
                                        DataNodeInfo newDataNode = new DataNodeInfo();
                                        newDataNode.setStorageId(dn.getStorageId());
                                        newDataNode.setStorageRootLocation(storageVolume.getBaseStorageRootDir());
                                        newDataNode.setStorageRootPathURI(storageVolume.getStorageURI());
                                        newDataNode.setNodeAccessURL(dataNodeAccessURI.toString());
                                        newDataNode.setNodeRelativePath(dataNodePathRelativeToVolRoot);
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
        } else {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No path found for " + dataEntryPath + " on volume " + storageVolume.getName()))
                    ;
        }
    }

    public Response.ResponseBuilder storeDataBundleContent(JacsBundle dataBundle, URI baseURI, String dataEntryName, Consumer<Long> dataBundleUpdater, InputStream contentStream) {
        Path storagePath = dataBundle.getRealStoragePath();
        LOG.info("Create {} on {}", dataEntryName, storagePath);
        long newFileEntrySize = dataStorageService.writeDataEntryStream(storagePath, dataEntryName, dataBundle.getStorageFormat(), contentStream);
        dataBundleUpdater.accept(newFileEntrySize);
        URI newContentURI = UriBuilder.fromUri(baseURI)
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path(dataBundle.getId().toString())
                .path("entry_content")
                .path(dataEntryName)
                .build();
        DataNodeInfo newDataNode = new DataNodeInfo();
        newDataNode.setNumericStorageId(dataBundle.getId());
        newDataNode.setStorageRootLocation(dataBundle.getRealStoragePath().toString());
        newDataNode.setStorageRootPathURI(dataBundle.getStorageURI());
        newDataNode.setNodeAccessURL(newContentURI.toString());
        newDataNode.setNodeRelativePath(dataEntryName);
        newDataNode.setCollectionFlag(false);
        return Response
                .created(newContentURI)
                .entity(newDataNode);
    }

    public Response.ResponseBuilder storeFileContent(JacsStorageVolume storageVolume, URI baseURI, String dataEntryName, InputStream contentStream) {
        if (!storageVolume.hasPermission(JacsStoragePermission.WRITE)) {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolume.getName() + " to list " + dataEntryName))
                    ;
        }
        Path dataFilePath = storageVolume.getFullDataPathFromBaseStorageRoot(dataEntryName);
        LOG.info("Create {} on {}", dataFilePath, storageVolume);
        dataStorageService.writeDataEntryStream(dataFilePath, "", JacsStorageFormat.SINGLE_DATA_FILE, contentStream);
        URI newContentURI = UriBuilder.fromUri(baseURI)
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path("storage_volume")
                .path(storageVolume.getId().toString())
                .path(dataEntryName)
                .build();
        DataNodeInfo newDataNode = new DataNodeInfo();
        newDataNode.setStorageRootLocation(storageVolume.getBaseStorageRootDir());
        newDataNode.setStorageRootPathURI(storageVolume.getStorageURI());
        newDataNode.setNodeAccessURL(newContentURI.toString());
        newDataNode.setNodeRelativePath(dataEntryName);
        newDataNode.setCollectionFlag(false);
        return Response
                .created(newContentURI)
                .entity(newDataNode);
    }

    public Response.ResponseBuilder removeContentFromDataBundle(JacsBundle dataBundle, String dataEntryName, Consumer<Long> dataBundleUpdater) {
        try {
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
                    ;
        }
    }

    public Response.ResponseBuilder removeFileContentFromVolume(JacsStorageVolume storageVolume, String dataEntryName) {
        if (!storageVolume.hasPermission(JacsStoragePermission.DELETE)) {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No delete permission for volume " + storageVolume.getName() + " to delete " + dataEntryName))
                    ;
        }
        Path dataPath = storageVolume.getFullDataPathFromBaseStorageRoot(dataEntryName);
        if (Files.exists(dataPath)) {
            try {
                LOG.info("Delete {}({}) from {}", dataEntryName, dataPath, storageVolume);
                dataStorageService.deleteStoragePath(dataPath);
                return Response.noContent();
            } catch (Exception e) {
                LOG.warn("File error while trying to delete {}({}) from {}", dataEntryName, dataPath, storageVolume);
                return Response
                        .status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(new ErrorResponse("Internal error while trying to delete " + dataEntryName + " from " + storageVolume))
                        ;
            }
        } else {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("Invalid storage volume " + storageVolume.getName() + " for deleting " + dataEntryName))
                    ;
        }
    }
}
