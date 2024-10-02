package org.janelia.jacsstorage.webdav.utils;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.rest.Constants;
import org.janelia.jacsstorage.webdav.propfind.Multistatus;
import org.janelia.jacsstorage.webdav.propfind.PropContainer;
import org.janelia.jacsstorage.webdav.propfind.PropfindResponse;
import org.janelia.jacsstorage.webdav.propfind.Propstat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WebdavUtils {
    private static final Logger LOG = LoggerFactory.getLogger(WebdavUtils.class);

    private static int MAX_ALLOWED_DEPTH = 20;

    public static int getDepth(String depth) {
        if (StringUtils.isBlank(depth) || "infinity".equalsIgnoreCase(depth)) {
            return MAX_ALLOWED_DEPTH;
        } else {
            try {
                int depthValue = Integer.valueOf(depth);
                if (depthValue > 1) {
                    throw new IllegalArgumentException("Illegal depth value - allowed values: {0, 1, infinity}");
                }
                return depthValue;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    public static Multistatus convertStorageVolumes(List<JacsStorageVolume> storageVolumes, String defaultPropResourceURL) {
        LOG.debug("Create multistatus response for {} with {}", storageVolumes, defaultPropResourceURL);
        Multistatus ms = new Multistatus();
        ms.getResponse().addAll(storageVolumes.stream()
                .map(storageVolume -> {
                    String storageServiceURL = StringUtils.appendIfMissing(storageVolume.getStorageServiceURL(), "/");
                    LOG.debug("Storage service URL for {}: {}, root: {}, binding: {}, ",
                            storageVolume, storageVolume.getStorageRootLocation(), storageVolume.getStorageVirtualPath(), storageServiceURL);
                    PropContainer propContainer = new PropContainer();
                    propContainer.setDisplayname(storageVolume.getName());
                    propContainer.setEtag(storageVolume.getStorageVirtualPath());
                    propContainer.setCreationDate(storageVolume.getCreated());
                    propContainer.setLastmodified(storageVolume.getModified());
                    propContainer.setResourceType("collection");
                    // set custom properties
                    propContainer.setStorageBindName(storageVolume.getStorageVirtualPath());
                    propContainer.setStorageRootDir(storageVolume.getStorageRootLocation());

                    Propstat propstat = new Propstat();
                    propstat.setPropContainer(propContainer);

                    PropfindResponse propfindResponse = new PropfindResponse();
                    propfindResponse.setPropstat(propstat);

                    if (StringUtils.isNotBlank(storageServiceURL)) {
                        propstat.setStatus("HTTP/1.1 200 OK");
                        propfindResponse.setHref(storageServiceURL + Constants.AGENTSTORAGE_URI_PATH);
                    } else {
                        propstat.setStatus("HTTP/1.1 404 Not Found");
                        propfindResponse.setHref(defaultPropResourceURL);
                    }
                    return propfindResponse;
                })
                .collect(Collectors.toList()));
        return ms;
    }

    public static Multistatus convertNodeList(Stream<DataNodeInfo> nodeInfoStream, Function<DataNodeInfo, DataNodeInfo> nodeInfoUpdater, Function<DataNodeInfo, String> nodeInfoToUriMapper) {
        Multistatus ms = new Multistatus();
        ms.getResponse().addAll(nodeInfoStream
                .map(nodeInfoUpdater)
                .map(nodeInfo -> {
                    PropContainer propContainer = new PropContainer();
                    propContainer.setEtag(nodeInfo.getNodeRelativePath());
                    propContainer.setContentType(nodeInfo.getMimeType());
                    propContainer.setContentLength(String.valueOf(nodeInfo.getSize()));
                    propContainer.setCreationDate(nodeInfo.getCreationTime());
                    propContainer.setLastmodified(nodeInfo.getLastModified());
                    if (nodeInfo.isCollectionFlag()) {
                        propContainer.setResourceType("collection");
                    }
                    // set custom properties
                    propContainer.setStorageEntryName(nodeInfo.getNodeRelativePath());
                    propContainer.setStorageBindName(nodeInfo.getStorageRootBinding());
                    propContainer.setStorageRootDir(nodeInfo.getStorageRootLocation());

                    Propstat propstat = new Propstat();
                    propstat.setPropContainer(propContainer);
                    propstat.setStatus("HTTP/1.1 200 OK");

                    PropfindResponse propfindResponse = new PropfindResponse();
                    propfindResponse.setHref(nodeInfoToUriMapper.apply(nodeInfo));
                    propfindResponse.setPropstat(propstat);
                    return propfindResponse;
                })
                .collect(Collectors.toList()));
        return ms;
    }

}
