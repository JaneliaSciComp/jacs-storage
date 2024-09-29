package org.janelia.jacsstorage.service.impl;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.activation.MimetypesFileTypeMap;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.coreutils.PathUtils;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStorageService;
import org.janelia.jacsstorage.service.DataContentService;
import org.janelia.jacsstorage.service.NoContentFoundException;
import org.janelia.jacsstorage.service.StorageCapacity;

public class DataContentServiceImpl implements DataContentService {

    private final ContentStorageServiceProvider contentStorageServiceProvider;
    private final ContentHandlersProvider contentHandlersProvider;

    @Inject
    DataContentServiceImpl(ContentStorageServiceProvider contentStorageServiceProvider,
                           ContentHandlersProvider contentHandlersProvider) {
        this.contentStorageServiceProvider = contentStorageServiceProvider;
        this.contentHandlersProvider = contentHandlersProvider;
    }

    @Override
    public boolean exists(JADEStorageURI storageURI) {
        ContentStorageService contentStorageService = contentStorageServiceProvider.getStorageService(storageURI);
        // artificially set the filter to only select the first item
        ContentFilterParams filterParams = new ContentFilterParams().setEntriesCount(1);
        List<ContentNode> contentNodes = contentStorageService.listContentNodes(storageURI.getStorageKey(), filterParams);
        return !contentNodes.isEmpty();
    }

    @Override
    public StorageCapacity storageCapacity(JADEStorageURI storageURI) {
        ContentStorageService contentStorageService = contentStorageServiceProvider.getStorageService(storageURI);
        return contentStorageService.getStorageCapacity(storageURI.getStorageKey());
    }

    @Override
    public Map<String, Object> readNodeMetadata(JADEStorageURI storageURI) {
        ContentStorageService contentStorageService = contentStorageServiceProvider.getStorageService(storageURI);
        // artificially set the filter to only select the first item
        ContentFilterParams filterParams = new ContentFilterParams().setEntriesCount(2);
        List<ContentNode> contentNodes = contentStorageService.listContentNodes(storageURI.getStorageKey(), filterParams);
        // if there's more than 1 node (a directory or prefix was provided)
        // then retrieve no metadata
        if (contentNodes.isEmpty()) {
            throw new NoContentFoundException("No content found for " + storageURI);
        } else if (contentNodes.size() != 1) {
            return Collections.emptyMap();
        } else {
            // if there's exactly 1 match lookup if there's a supported metada reader
            ContentMetadataReader contentMetadataReader = contentHandlersProvider.getContentMetadataReader(contentNodes.get(0).getMimeType());
            return contentMetadataReader.getMetadata(contentNodes.get(0));
        }
    }

    @Override
    public List<ContentNode> listDataNodes(JADEStorageURI storageURI, ContentFilterParams filterParams) {
        ContentStorageService contentStorageService = contentStorageServiceProvider.getStorageService(storageURI);
        return contentStorageService.listContentNodes(storageURI.getStorageKey(), filterParams);
    }

    @Override
    public long readDataStream(JADEStorageURI storageURI, ContentFilterParams filterParams, OutputStream dataStream) {
        ContentStorageService contentStorageService = contentStorageServiceProvider.getStorageService(storageURI);
        List<ContentNode> contentNodes = contentStorageService.listContentNodes(storageURI.getStorageKey(), filterParams);
        if (contentNodes.isEmpty()) {
            throw new NoContentFoundException("No content found for " + storageURI);
        } else {
            ContentFilter contentFilter = contentHandlersProvider.getContentFilter(filterParams);
            return contentFilter.applyContentFilter(filterParams, contentNodes, dataStream);
        }
    }

    @Override
    public long writeDataStream(JADEStorageURI storageURI, InputStream dataStream) {
        ContentStorageService contentStorageService = contentStorageServiceProvider.getStorageService(storageURI);
        return contentStorageService.writeContent(storageURI.getStorageKey(), dataStream);
    }

    @Override
    public void removeData(JADEStorageURI storageURI) {
        ContentStorageService contentStorageService = contentStorageServiceProvider.getStorageService(storageURI);
        contentStorageService.deleteContent(storageURI.getStorageKey());
    }
}
