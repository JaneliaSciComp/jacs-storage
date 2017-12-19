package org.janelia.jacsstorage.helper;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.rest.ErrorResponse;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class StorageResourceHelper {
    private static final Logger LOG = LoggerFactory.getLogger(StorageResourceHelper.class);

    private final StorageLookupService storageLookupService;
    private final StorageVolumeManager storageVolumeManager;

    public StorageResourceHelper(StorageLookupService storageLookupService, StorageVolumeManager storageVolumeManager) {
        this.storageLookupService = storageLookupService;
        this.storageVolumeManager = storageVolumeManager;
    }

    public Response retrieveFileContent(String fullFileNameParam,
                                        Supplier<Response> conflictResponseHandler,
                                        BiFunction<JacsBundle, String, Response> bundleBasedResponseHandler,
                                        BiFunction<JacsStorageVolume, Path, Response> fileBasedResponseHandler) {
        String fullFileName = StringUtils.prependIfMissing(fullFileNameParam, "/");
        List<JacsStorageVolume> storageVolumes = storageVolumeManager.getManagedVolumes(new StorageQuery().setDataStoragePath(fullFileName));
        JacsStorageVolume selectedVolume = null;
        if (storageVolumes.isEmpty()) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No managed volume found for " + fullFileNameParam))
                    .build();
        } else if (storageVolumes.size() > 1 && conflictResponseHandler != null) {
            Response conflictResponse = conflictResponseHandler.get();
            if (conflictResponse != null) {
                return conflictResponse;
            }
        }
        selectedVolume = storageVolumes.get(0);
        // check if the first path component after the storage prefix is a bundle ID
        java.nio.file.Path storagePathPrefix = Paths.get(selectedVolume.getStoragePathPrefix());
        java.nio.file.Path storageRelativeFileDataPath = storagePathPrefix.relativize(Paths.get(fullFileName));
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
            return fileBasedResponseHandler.apply(selectedVolume, storageRelativeFileDataPath);
        } else {
            String dataEntryPath = fileDataPathComponents > 1
                    ? storageRelativeFileDataPath.subpath(1, fileDataPathComponents).toString()
                    : "";
            return bundleBasedResponseHandler.apply(dataBundle, dataEntryPath);
        }
    }
}
