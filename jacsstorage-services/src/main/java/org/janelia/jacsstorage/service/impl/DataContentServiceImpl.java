package org.janelia.jacsstorage.service.impl;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.janelia.jacsstorage.io.ContentAccessParams;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStorageService;
import org.janelia.jacsstorage.service.DataContentService;
import org.janelia.jacsstorage.service.NoContentFoundException;
import org.janelia.jacsstorage.service.StorageCapacity;

public class DataContentServiceImpl implements DataContentService {

    private final ContentStorageServiceProvider contentStorageServiceProvider;
    private final ContentAccessProvider contentAccessProvider;

    @Inject
    DataContentServiceImpl(ContentStorageServiceProvider contentStorageServiceProvider,
                           ContentAccessProvider contentAccessProvider) {
        this.contentStorageServiceProvider = contentStorageServiceProvider;
        this.contentAccessProvider = contentAccessProvider;
    }

    @Override
    public boolean exists(JADEStorageURI storageURI) {
        ContentStorageService contentStorageService = contentStorageServiceProvider.getStorageService(storageURI);
        if (contentStorageService == null) {
            return false;
        }
        return contentStorageService.canAccess(storageURI.getContentKey());
    }

    @Override
    public StorageCapacity storageCapacity(JADEStorageURI storageURI) {
        ContentStorageService contentStorageService = contentStorageServiceProvider.getStorageService(storageURI);
        if (contentStorageService == null) {
            return new StorageCapacity(-1, -1);
        }
        return contentStorageService.getStorageCapacity(storageURI.getContentKey());
    }

    @Override
    public Map<String, Object> readNodeMetadata(JADEStorageURI storageURI) {
        ContentStorageService contentStorageService = contentStorageServiceProvider.getStorageService(storageURI);
        if (contentStorageService == null) {
            return Collections.emptyMap();
        }
        // artificially set the filter to only select the first item
        ContentAccessParams filterParams = new ContentAccessParams().setEntriesCount(2);
        List<ContentNode> contentNodes = contentStorageService.listContentNodes(storageURI.getContentKey(), filterParams);
        // if there's more than 1 node (a directory or prefix was provided)
        // then retrieve no metadata
        if (contentNodes.isEmpty()) {
            throw new NoContentFoundException("No content found for " + storageURI);
        } else if (contentNodes.size() != 1) {
            return Collections.emptyMap();
        } else {
            // if there's exactly 1 match lookup if there's a supported metada reader
            ContentMetadataReader contentMetadataReader = contentAccessProvider.getContentMetadataReader(contentNodes.get(0).getMimeType());
            return contentMetadataReader.getMetadata(contentNodes.get(0));
        }
    }

    @Override
    public List<ContentNode> listDataNodes(JADEStorageURI storageURI, ContentAccessParams filterParams) {
        ContentStorageService contentStorageService = contentStorageServiceProvider.getStorageService(storageURI);
        if (contentStorageService == null) {
            return Collections.emptyList();
        }
        return contentStorageService.listContentNodes(storageURI.getContentKey(), filterParams);
    }

    @Override
    public long readDataStream(JADEStorageURI storageURI, ContentAccessParams filterParams, OutputStream dataStream) {
        ContentStorageService contentStorageService = contentStorageServiceProvider.getStorageService(storageURI);
        if (contentStorageService == null) {
            throw new IllegalArgumentException("Invalid storage URI");
        }
        List<ContentNode> contentNodes = contentStorageService.listContentNodes(storageURI.getContentKey(), filterParams);
        if (contentNodes.isEmpty()) {
            throw new NoContentFoundException("No content found for " + storageURI);
        } else {
            ContentAccess contentAccess = contentAccessProvider.getContentFilter(filterParams);
            return contentAccess.retrieveContent(contentNodes, filterParams, dataStream);
        }
    }

    @Override
    public long writeDataStream(JADEStorageURI storageURI, InputStream dataStream) {
        ContentStorageService contentStorageService = contentStorageServiceProvider.getStorageService(storageURI);
        if (contentStorageService == null) {
            throw new IllegalArgumentException("Invalid storage URI");
        }
        return contentStorageService.writeContent(storageURI.getContentKey(), dataStream);
    }

    @Override
    public void removeData(JADEStorageURI storageURI) {
        ContentStorageService contentStorageService = contentStorageServiceProvider.getStorageService(storageURI);
        if (contentStorageService == null) {
            throw new IllegalArgumentException("Invalid storage URI");
        }
        contentStorageService.deleteContent(storageURI.getContentKey());
    }
}
