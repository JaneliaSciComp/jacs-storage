package org.janelia.jacsstorage.webdav.utils;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.rest.Constants;
import org.janelia.jacsstorage.webdav.propfind.Multistatus;
import org.janelia.jacsstorage.webdav.propfind.Prop;
import org.janelia.jacsstorage.webdav.propfind.PropfindResponse;
import org.janelia.jacsstorage.webdav.propfind.Propstat;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class WebdavUtils {
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
        Multistatus ms = new Multistatus();
        ms.getResponse().addAll(storageVolumes.stream()
                .map(storageVolume -> {
                    String storageServiceURL = StringUtils.appendIfMissing(storageVolume.getStorageServiceURL(), "/");

                    Prop prop = new Prop();
                    prop.setDisplayname(storageVolume.getName());
                    prop.setEtag(storageVolume.getStoragePathPrefix());
                    prop.setCreationDate(storageVolume.getCreated());
                    prop.setLastmodified(storageVolume.getModified());
                    prop.setResourceType("collection");

                    Propstat propstat = new Propstat();
                    propstat.setProp(prop);

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

    public static Multistatus convertNodeList(List<DataNodeInfo> nodeInfoList, Function<DataNodeInfo, String> nodeInfoToUriMapper) {
        Multistatus ms = new Multistatus();
        ms.getResponse().addAll(nodeInfoList.stream()
                .map(nodeInfo -> {
                    Prop prop = new Prop();
                    prop.setEtag(nodeInfo.getNodeRelativePath());
                    prop.setContentType(nodeInfo.getMimeType());
                    prop.setContentLength(String.valueOf(nodeInfo.getSize()));
                    prop.setCreationDate(nodeInfo.getCreationTime());
                    prop.setLastmodified(nodeInfo.getLastModified());
                    if (nodeInfo.isCollectionFlag()) {
                        prop.setResourceType("collection");
                    }

                    Propstat propstat = new Propstat();
                    propstat.setProp(prop);
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
