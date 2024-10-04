package org.janelia.jacsstorage.service.impl;

import java.io.InputStream;
import java.util.List;

import javax.inject.Inject;

import org.janelia.jacsstorage.io.ContentAccessParams;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.ContentGetter;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStorageService;
import org.janelia.jacsstorage.service.DataContentService;
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
    public ContentGetter getDataContent(JADEStorageURI storageURI, ContentAccessParams contentAccessParams) {
        ContentStorageService contentStorageService = contentStorageServiceProvider.getStorageService(storageURI);
        if (contentStorageService == null) {
            throw new IllegalArgumentException("Invalid storage URI");
        }
        List<ContentNode> contentNodes = contentStorageService.listContentNodes(storageURI.getContentKey(), contentAccessParams);
        return new ContentGetterImpl(
                contentStorageService,
                contentAccessProvider,
                contentNodes,
                contentAccessParams
        );
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
