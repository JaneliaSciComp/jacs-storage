package org.janelia.jacsstorage.rest;

import com.google.common.collect.ImmutableMap;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.StorageRelativePath;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.saalfeldlab.n5.N5DatasetDiscoverer;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5TreeNode;
import org.janelia.saalfeldlab.n5.metadata.*;
import org.janelia.saalfeldlab.n5.metadata.canonical.CanonicalMetadataParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Executors;

@Api(value = "Agent storage API for N5 file structures")
@Timed
@Path(Constants.AGENTSTORAGE_URI_PATH)
public class N5StorageResource {

    private static final Logger LOG = LoggerFactory.getLogger(N5StorageResource.class);

    public static final N5MetadataParser<?>[] n5vGroupParsers = new N5MetadataParser[]{
            new N5CosemMultiScaleMetadata.CosemMultiScaleParser(),
            new N5ViewerMultiscaleMetadataParser(),
            new CanonicalMetadataParser(),
            new N5ViewerMultichannelMetadata.N5ViewerMultichannelMetadataParser()
    };

    public static final N5MetadataParser<?>[] n5vParsers = new N5MetadataParser[] {
            new N5CosemMetadataParser(),
            new N5SingleScaleMetadataParser(),
            new CanonicalMetadataParser(),
            new N5GenericSingleScaleMetadataParser()
    };

    @Inject @LocalInstance
    private StorageVolumeManager storageVolumeManager;


    @ApiOperation(value = "Discover N5 data sets in the given path and return a tree of N5TreeNodes")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The operation was successful"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or invalid file path"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("storage_volume/{storageVolumeId}/n5tree/{storageRelativePath:.+}")
    public Response retrieveDataInfoFromStorageVolume(@PathParam("storageVolumeId") Long storageVolumeId,
                                                      @PathParam("storageRelativePath") String storageRelativeFilePath) {
        LOG.debug("Retrieve N5 data sets from volume {}:{}", storageVolumeId, storageRelativeFilePath);

        JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
        if (storageVolume == null) {
            LOG.warn("No accessible volume found for {}", storageVolumeId);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No accessible volume found for " + storageVolumeId))
                    .build();
        }
        if (storageVolume.hasPermission(JacsStoragePermission.READ)) {
            java.nio.file.Path absolutePath = storageVolume.getDataStorageAbsolutePath(StorageRelativePath.pathRelativeToBaseRoot(storageRelativeFilePath)).orElse(null);
            if (absolutePath == null) {
                return Response
                        .serverError()
                        .entity(ImmutableMap.of("errormessage", "Could not resolve relative path: "+storageRelativeFilePath))
                        .build();
            }
            try {
                N5Reader n5Reader = new N5FSReader(absolutePath.toString());
                N5DatasetDiscoverer datasetDiscoverer = new N5DatasetDiscoverer(
                        n5Reader,
                        Executors.newCachedThreadPool(),
                        Arrays.asList(n5vParsers),
                        Arrays.asList(n5vGroupParsers));
                N5TreeNode n5RootNode = datasetDiscoverer.discoverAndParseRecursive("/");
                return Response
                        .ok(n5RootNode, MediaType.APPLICATION_JSON)
                        .build();
            }
            catch (IOException e) {
                String errorMessage = "Error discovering N5 content at "+absolutePath;
                LOG.error(errorMessage, e);
                return Response
                        .serverError()
                        .entity(ImmutableMap.of("errormessage", errorMessage))
                        .build();
            }

        } else {
            LOG.warn("Attempt to get info about {} from volume {} but the volume does not allow READ", storageRelativeFilePath, storageVolumeId);
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolumeId))
                    .build();
        }
    }
}
