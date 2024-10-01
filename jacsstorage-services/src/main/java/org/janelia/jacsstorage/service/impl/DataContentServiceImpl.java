package org.janelia.jacsstorage.service.impl;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

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
        ContentAccess<? extends ContentStorageService> contentAccess = contentStorageServiceProvider.getStorageService(storageURI);
        if (contentAccess == null) {
            return false;
        }
        ContentStorageService contentStorageService = contentAccess.storageService;
        String contentKey = contentAccess.contentKey;
        // artificially set the filter to only select the first item
        ContentFilterParams filterParams = new ContentFilterParams().setEntriesCount(1);
        List<ContentNode> contentNodes = contentStorageService.listContentNodes(contentKey, filterParams);
        return !contentNodes.isEmpty();
    }

    @Override
    public StorageCapacity storageCapacity(JADEStorageURI storageURI) {
        ContentAccess<? extends ContentStorageService> contentAccess = contentStorageServiceProvider.getStorageService(storageURI);
        if (contentAccess == null) {
            return new StorageCapacity(-1, -1);
        }
        ContentStorageService contentStorageService = contentAccess.storageService;
        String contentKey = contentAccess.contentKey;
        return contentStorageService.getStorageCapacity(contentKey);
    }

    @Override
    public Map<String, Object> readNodeMetadata(JADEStorageURI storageURI) {
        ContentAccess<? extends ContentStorageService> contentAccess = contentStorageServiceProvider.getStorageService(storageURI);
        if (contentAccess == null) {
            return Collections.emptyMap();
        }
        ContentStorageService contentStorageService = contentAccess.storageService;
        String contentKey = contentAccess.contentKey;
        // artificially set the filter to only select the first item
        ContentFilterParams filterParams = new ContentFilterParams().setEntriesCount(2);
        List<ContentNode> contentNodes = contentStorageService.listContentNodes(contentKey, filterParams);
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
        ContentAccess<? extends ContentStorageService> contentAccess = contentStorageServiceProvider.getStorageService(storageURI);
        if (contentAccess == null) {
            return Collections.emptyList();
        }
        ContentStorageService contentStorageService = contentAccess.storageService;
        String contentKey = contentAccess.contentKey;
        return contentStorageService.listContentNodes(contentKey, filterParams);
    }

    @Override
    public long readDataStream(JADEStorageURI storageURI, ContentFilterParams filterParams, OutputStream dataStream) {
        ContentAccess<? extends ContentStorageService> contentAccess = contentStorageServiceProvider.getStorageService(storageURI);
        if (contentAccess == null) {
            throw new IllegalArgumentException("Invalid storage URI");
        }
        ContentStorageService contentStorageService = contentAccess.storageService;
        String contentKey = contentAccess.contentKey;
        List<ContentNode> contentNodes = contentStorageService.listContentNodes(contentKey, filterParams);
        if (contentNodes.isEmpty()) {
            throw new NoContentFoundException("No content found for " + storageURI);
        } else {
            ContentFilter contentFilter = contentHandlersProvider.getContentFilter(filterParams);
            return contentFilter.applyContentFilter(filterParams, contentNodes, dataStream);
        }
    }

    @Override
    public long writeDataStream(JADEStorageURI storageURI, InputStream dataStream) {
        ContentAccess<? extends ContentStorageService> contentAccess = contentStorageServiceProvider.getStorageService(storageURI);
        if (contentAccess == null) {
            throw new IllegalArgumentException("Invalid storage URI");
        }
        ContentStorageService contentStorageService = contentAccess.storageService;
        String contentKey = contentAccess.contentKey;
        return contentStorageService.writeContent(contentKey, dataStream);
    }

    @Override
    public void removeData(JADEStorageURI storageURI) {
        ContentAccess<? extends ContentStorageService> contentAccess = contentStorageServiceProvider.getStorageService(storageURI);
        ContentStorageService contentStorageService = contentAccess.storageService;
        String contentKey = contentAccess.contentKey;
        contentStorageService.deleteContent(contentKey);
    }
}
