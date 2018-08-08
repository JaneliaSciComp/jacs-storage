package org.janelia.jacsstorage.helper;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.coreutils.PathUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.StoragePathURI;
import org.janelia.jacsstorage.rest.Constants;
import org.janelia.jacsstorage.rest.ErrorResponse;
import org.janelia.jacsstorage.security.JacsSubjectHelper;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
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
                                                                       BiFunction<JacsStorageVolume, String, Response.ResponseBuilder> fileBasedResponseHandler) {
        return getStorageVolumeForURI(storagePathURI)
                .map(storageVolume -> handleResponseForFullDataPathParam(storageVolume,
                        storagePathURI,
                        bundleBasedResponseHandler,
                        fileBasedResponseHandler))
                .orElseGet(() -> Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No managed volume found for " + storagePathURI)))
                ;
    }

    private Response.ResponseBuilder handleResponseForFullDataPathParam(JacsStorageVolume storageVolume,
                                                                        StoragePathURI storagePathURI,
                                                                        BiFunction<JacsBundle, String, Response.ResponseBuilder> bundleBasedResponseHandler,
                                                                        BiFunction<JacsStorageVolume, String, Response.ResponseBuilder> fileBasedResponseHandler) {
        // check if the first path component after the storage prefix is a bundle ID
        java.nio.file.Path storageRelativeFileDataPath = storagePathURI.asPath()
                .map(p -> storageVolume.getStorageRelativePath(p.toString()))
                .orElse(null);
        if (storageRelativeFileDataPath == null) {
            LOG.debug("Path {} is not a relative path to {} ", storagePathURI, storageVolume);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No path found for " + storagePathURI))
                    ;
        }
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
            return fileBasedResponseHandler.apply(storageVolume, storageRelativeFileDataPath.toString());
        } else {
            String dataEntryPath = fileDataPathComponents > 1
                    ? storageRelativeFileDataPath.subpath(1, fileDataPathComponents).toString()
                    : "";
            return bundleBasedResponseHandler.apply(dataBundle, dataEntryPath);
        }
    }

    public Optional<JacsStorageVolume> getStorageVolumeForURI(StoragePathURI dirStorageURI) {
        return dirStorageURI.asPath()
                .flatMap(dir -> {
                    int dirComponents = dir.getNameCount();
                    String dirKey;
                    if (dirComponents < N_VOL_DIR_COMPONENTS) {
                        dirKey = dir.toString();
                    } else {
                        if (dir.getRoot() == null) {
                            dirKey = dir.subpath(0, N_VOL_DIR_COMPONENTS).toString();
                        } else {
                            dirKey = dir.getRoot().resolve(dir.subpath(0, N_VOL_DIR_COMPONENTS)).toString();
                        }
                    }
                    return getCachedStorageVolumeForDir(dirKey);
                });
    }

    private Optional<JacsStorageVolume> getCachedStorageVolumeForDir(String dirName) {
        try {
            JacsStorageVolume storageVolume = VOLUMES_BY_PATH_CACHE.get(dirName, new Callable<JacsStorageVolume>() {
                @Override
                public JacsStorageVolume call() {
                    return retrieveStorageVolumeForDir(dirName);
                }
            });
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

    public Response.ResponseBuilder checkContentFromFile(JacsStorageVolume storageVolume, String dataEntryPath) {
        if (Files.exists(Paths.get(storageVolume.getStorageRootDir()).resolve(dataEntryPath))) {
            return Response
                    .ok()
                    .header("Content-Length", PathUtils.getSize(Paths.get(storageVolume.getStorageRootDir()).resolve(dataEntryPath)));
        } else if (Files.exists(Paths.get(storageVolume.getStoragePathPrefix()).resolve(dataEntryPath))) {
            return Response
                    .ok()
                    .header("Content-length", PathUtils.getSize(Paths.get(storageVolume.getStoragePathPrefix()).resolve(dataEntryPath)));
        } else {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No path found for " + dataEntryPath + " on volume " + storageVolume.getName()))
                    ;
        }
    }

    public Response.ResponseBuilder retrieveContentFromFile(JacsStorageVolume storageVolume, String dataEntryPath) {
        if (!storageVolume.hasPermission(JacsStoragePermission.READ)) {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolume.getName() + " to read " + dataEntryPath))
                    ;
        } else if (Files.exists(Paths.get(storageVolume.getStorageRootDir()).resolve(dataEntryPath))) {
            return retrieveContentFromFile(Paths.get(storageVolume.getStorageRootDir()).resolve(dataEntryPath));
        } else if (Files.exists(Paths.get(storageVolume.getStoragePathPrefix()).resolve(dataEntryPath))) {
            return retrieveContentFromFile(Paths.get(storageVolume.getStoragePathPrefix()).resolve(dataEntryPath));
        } else {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No path found for " + dataEntryPath + " on volume " + storageVolume.getName()))
                    ;
        }
    }

    private Response.ResponseBuilder retrieveContentFromFile(Path filePath) {
        JacsStorageFormat storageFormat = Files.isRegularFile(filePath) ? JacsStorageFormat.SINGLE_DATA_FILE : JacsStorageFormat.DATA_DIRECTORY;
        long fileSize = PathUtils.getSize(filePath);
        StreamingOutput fileStream = output -> {
            try {
                dataStorageService.retrieveDataStream(filePath, storageFormat, output);
            } catch (Exception e) {
                LOG.error("Error streaming data file content for {}", filePath, e);
                throw new WebApplicationException(e);
            }
        };
        return Response
                .ok(fileStream, MediaType.APPLICATION_OCTET_STREAM)
                .header("Content-Length", fileSize)
                .header("Content-Disposition", "attachment; filename = " + filePath.toFile().getName())
                ;
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

    public Response.ResponseBuilder listContentFromPath(JacsStorageVolume storageVolume, URI baseURI, String dataEntryPath, int depth) {
        if (!storageVolume.hasPermission(JacsStoragePermission.READ)) {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolume.getName() + " to list " + dataEntryPath))
                    ;
        } else if (Files.exists(Paths.get(storageVolume.getStorageRootDir(), dataEntryPath))) {
            return listContentFromPath(storageVolume, baseURI, Paths.get(storageVolume.getStorageRootDir(), dataEntryPath), depth);
        } else if (Files.exists(Paths.get(storageVolume.getStoragePathPrefix(), dataEntryPath))) {
            return listContentFromPath(storageVolume, baseURI, Paths.get(storageVolume.getStoragePathPrefix(), dataEntryPath), depth);
        } else {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No path found for " + dataEntryPath + " on volume " + storageVolume.getName()))
                    ;
        }
    }

    private Response.ResponseBuilder listContentFromPath(JacsStorageVolume storageVolume, URI baseURI, Path path, int depth) {
        JacsStorageFormat storageFormat = Files.isRegularFile(path) ? JacsStorageFormat.SINGLE_DATA_FILE : JacsStorageFormat.DATA_DIRECTORY;
        List<DataNodeInfo> entries = dataStorageService.listDataEntries(path, "", storageFormat, depth);
        Path pathRelativeToVolRoot = Paths.get(storageVolume.getStorageRootDir()).relativize(path);
        return Response
                .ok(entries.stream()
                                .map(dn -> {
                                    String dataNodePathRelativeToVolRoot = pathRelativeToVolRoot.resolve(dn.getNodeRelativePath()).toString();
                                    URI dataNodeAccessURI = UriBuilder.fromUri(baseURI)
                                            .path(Constants.AGENTSTORAGE_URI_PATH)
                                            .path("storage_volume")
                                            .path(storageVolume.getId().toString())
                                            .path(dataNodePathRelativeToVolRoot)
                                            .build();
                                    DataNodeInfo newDataNode = new DataNodeInfo();
                                    newDataNode.setStorageId(dn.getStorageId());
                                    newDataNode.setStorageRootLocation(storageVolume.getStorageRootDir());
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
    }

    public Response.ResponseBuilder storeDataBundleContent(JacsBundle dataBundle, URI baseURI, String dataEntryPath, Consumer<Long> dataBundleUpdater, InputStream contentStream) {
        Path storagePath = dataBundle.getRealStoragePath();
        LOG.info("Create {} on {}", dataEntryPath, storagePath);
        long newFileEntrySize = dataStorageService.writeDataEntryStream(storagePath, dataEntryPath, dataBundle.getStorageFormat(), contentStream);
        dataBundleUpdater.accept(newFileEntrySize);
        URI newContentURI = UriBuilder.fromUri(baseURI)
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path(dataBundle.getId().toString())
                .path("entry_content")
                .path(dataEntryPath)
                .build();
        DataNodeInfo newDataNode = new DataNodeInfo();
        newDataNode.setNumericStorageId(dataBundle.getId());
        newDataNode.setStorageRootLocation(dataBundle.getRealStoragePath().toString());
        newDataNode.setStorageRootPathURI(dataBundle.getStorageURI());
        newDataNode.setNodeAccessURL(newContentURI.toString());
        newDataNode.setNodeRelativePath(dataEntryPath);
        newDataNode.setCollectionFlag(false);
        return Response
                .created(newContentURI)
                .entity(newDataNode);
    }

    public Response.ResponseBuilder storeFileContent(JacsStorageVolume storageVolume, URI baseURI, String dataEntryPath, InputStream contentStream) {
        if (!storageVolume.hasPermission(JacsStoragePermission.WRITE)) {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolume.getName() + " to list " + dataEntryPath))
                    ;
        } else if (Files.exists(Paths.get(storageVolume.getStorageRootDir()))) {
            return storeFileContent(storageVolume, baseURI, Paths.get(storageVolume.getStorageRootDir(), dataEntryPath), contentStream);
        } else if (Files.exists(Paths.get(storageVolume.getStoragePathPrefix()))) {
            return storeFileContent(storageVolume, baseURI, Paths.get(storageVolume.getStoragePathPrefix(), dataEntryPath), contentStream);
        } else {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("Invalid storage volume " + storageVolume.getName() + " for storing " + dataEntryPath))
                    ;
        }
    }

    private Response.ResponseBuilder storeFileContent(JacsStorageVolume storageVolume, URI baseURI, Path filePath, InputStream contentStream) {
        LOG.info("Create {} on {}", filePath, storageVolume);
        dataStorageService.writeDataEntryStream(filePath, "", JacsStorageFormat.SINGLE_DATA_FILE, contentStream);
        String pathRelativeToVolRoot = Paths.get(storageVolume.getStorageRootDir()).relativize(filePath).toString();
        URI newContentURI = UriBuilder.fromUri(baseURI)
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path("storage_volume")
                .path(storageVolume.getId().toString())
                .path(pathRelativeToVolRoot)
                .build();
        DataNodeInfo newDataNode = new DataNodeInfo();
        newDataNode.setStorageRootLocation(storageVolume.getStorageRootDir());
        newDataNode.setStorageRootPathURI(storageVolume.getStorageURI());
        newDataNode.setNodeAccessURL(newContentURI.toString());
        newDataNode.setNodeRelativePath(pathRelativeToVolRoot);
        newDataNode.setCollectionFlag(false);
        return Response
                .created(newContentURI)
                .entity(newDataNode);
    }

    public Response.ResponseBuilder removeContentFromDataBundle(JacsBundle dataBundle, String dataEntryPath, Consumer<Long> dataBundleUpdater) {
        try {
            Path storagePath = dataBundle.getRealStoragePath();
            LOG.info("Delete {} from {}", dataEntryPath, storagePath);
            long freedEntrySize = dataStorageService.deleteStorageEntry(storagePath, dataEntryPath, dataBundle.getStorageFormat());
            dataBundleUpdater.accept(freedEntrySize);
            return Response.noContent();
        } catch (Exception e) {
            LOG.warn("Error while trying to delete {} from {}", dataEntryPath, dataBundle, e);
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Delete bundle entry error: " + dataBundle.getId() + "/" + dataEntryPath))
                    ;
        }
    }

    public Response.ResponseBuilder removeFileContentFromVolume(JacsStorageVolume storageVolume, String dataEntryPath) {
        if (!storageVolume.hasPermission(JacsStoragePermission.DELETE)) {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No delete permission for volume " + storageVolume.getName() + " to delete " + dataEntryPath))
                    ;
        } else if (Files.exists(Paths.get(storageVolume.getStorageRootDir(), dataEntryPath))) {
            return removeFileFromVolume(storageVolume, Paths.get(storageVolume.getStorageRootDir(), dataEntryPath));
        } else if (Files.exists(Paths.get(storageVolume.getStoragePathPrefix(), dataEntryPath))) {
            return removeFileFromVolume(storageVolume, Paths.get(storageVolume.getStorageRootDir(), dataEntryPath));
        } else {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("Invalid storage volume " + storageVolume.getName() + " for deleting " + dataEntryPath))
                    ;
        }
    }

    private Response.ResponseBuilder removeFileFromVolume(JacsStorageVolume storageVolume, Path filePath) {
        try {
            LOG.info("Delete {} from {}", filePath, storageVolume);
            dataStorageService.deleteStoragePath(filePath);
            return Response.noContent();
        } catch (Exception e) {
            LOG.warn("File error while trying to delete {} from {}", filePath, storageVolume);
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Internal error while trying to delete " + filePath + " from " + storageVolume))
                    ;
        }
    }
}
