package org.janelia.jacsstorage.helper;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.rest.ErrorResponse;
import org.janelia.jacsstorage.security.JacsSubjectHelper;
import org.janelia.jacsstorage.service.StorageContentReader;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class StorageResourceHelper {
    private static final Logger LOG = LoggerFactory.getLogger(StorageResourceHelper.class);

    private final StorageContentReader storageContentReader;
    private final StorageLookupService storageLookupService;
    private final StorageVolumeManager storageVolumeManager;

    public StorageResourceHelper(StorageContentReader storageContentReader, StorageLookupService storageLookupService, StorageVolumeManager storageVolumeManager) {
        this.storageContentReader = storageContentReader;
        this.storageLookupService = storageLookupService;
        this.storageVolumeManager = storageVolumeManager;
    }

    @Timed
    public Supplier<Response> handleResponseForFullDataPathParam(String fullDataPathParam,
                                                                 BiFunction<JacsBundle, String, Response> bundleBasedResponseHandler,
                                                                 BiFunction<JacsStorageVolume, String, Response> fileBasedResponseHandler) {
        String fullDataPathName = StringUtils.prependIfMissing(fullDataPathParam, "/");
        List<JacsStorageVolume> storageVolumes = storageVolumeManager.getManagedVolumes(new StorageQuery().setDataStoragePath(fullDataPathName));
        if (storageVolumes.isEmpty()) {
            LOG.warn("No volume found to match {}", fullDataPathName);
            return () -> Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No managed volume found for " + fullDataPathName))
                    .build();
        } else if (storageVolumes.size() > 1) {
            LOG.warn("More than one storage volumes found for {} -> {}", fullDataPathName, storageVolumes);
        }
        JacsStorageVolume selectedVolume = storageVolumes.get(0);
        // check if the first path component after the storage prefix is a bundle ID
        java.nio.file.Path storageRelativeFileDataPath = selectedVolume.getStorageRelativePath(fullDataPathName);
        if (storageRelativeFileDataPath == null) {
            LOG.warn("Path {} is not a relative path to {} ", fullDataPathName, selectedVolume);
            return () -> Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No relative path found for " + fullDataPathName))
                    .build();
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
            return () -> fileBasedResponseHandler.apply(selectedVolume, storageRelativeFileDataPath.toString());
        } else {
            String dataEntryPath = fileDataPathComponents > 1
                    ? storageRelativeFileDataPath.subpath(1, fileDataPathComponents).toString()
                    : "";
            JacsBundle selectedDataBundle = dataBundle;
            return () -> bundleBasedResponseHandler.apply(selectedDataBundle, dataEntryPath);
        }
    }

    public Response checkContentFromFile(JacsStorageVolume storageVolume, String dataEntryPath) {
        if (Files.exists(Paths.get(storageVolume.getStorageRootDir()).resolve(dataEntryPath))) {
            return Response
                    .ok()
                    .build();
        } else if (Files.exists(Paths.get(storageVolume.getStoragePathPrefix()).resolve(dataEntryPath))) {
            return Response
                    .ok()
                    .build();
        } else {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No path found for " + dataEntryPath + " on volume " + storageVolume.getName()))
                    .build();
        }
    }

    public Response retrieveContentFromFile(JacsStorageVolume storageVolume, String dataEntryPath) {
        if (Files.exists(Paths.get(storageVolume.getStorageRootDir()).resolve(dataEntryPath))) {
            return retrieveContentFromFile(Paths.get(storageVolume.getStorageRootDir()).resolve(dataEntryPath));
        } else if (Files.exists(Paths.get(storageVolume.getStoragePathPrefix()).resolve(dataEntryPath))) {
            return retrieveContentFromFile(Paths.get(storageVolume.getStoragePathPrefix()).resolve(dataEntryPath));
        } else {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No path found for " + dataEntryPath + " on volume " + storageVolume.getName()))
                    .build();
        }
    }

    private Response retrieveContentFromFile(Path filePath) {
        JacsStorageFormat storageFormat = Files.isRegularFile(filePath) ? JacsStorageFormat.SINGLE_DATA_FILE : JacsStorageFormat.DATA_DIRECTORY;
        StreamingOutput fileStream = output -> {
            try {
                storageContentReader.retrieveDataStream(filePath, storageFormat, output);
            } catch (Exception e) {
                LOG.error("Error streaming data file content for {}", filePath, e);
                throw new WebApplicationException(e);
            }
        };
        return Response
                .ok(fileStream, MediaType.APPLICATION_OCTET_STREAM)
                .header("content-disposition","attachment; filename = " + filePath.toFile().getName())
                .build();
    }

    public Response retrieveContentFromDataBundle(JacsBundle dataBundle, String dataEntryPath) {
        StreamingOutput bundleStream = output -> {
            try {
                storageContentReader.readDataEntryStream(
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
                .header("content-disposition","attachment; filename = " + JacsSubjectHelper.getNameFromSubjectKey(dataBundle.getOwnerKey()) + "-" + dataBundle.getName() + "/" + dataEntryPath)
                .build();
    }

}
