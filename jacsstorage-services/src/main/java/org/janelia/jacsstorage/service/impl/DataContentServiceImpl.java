package org.janelia.jacsstorage.service.impl;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

import org.janelia.jacsstorage.coreutils.ComparatorUtils;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.service.ContentAccessProvider;
import org.janelia.jacsstorage.service.ContentGetter;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStorageService;
import org.janelia.jacsstorage.service.ContentStorageServiceProvider;
import org.janelia.jacsstorage.service.DataContentService;
import org.janelia.jacsstorage.service.StorageCapacity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Dependent
public class DataContentServiceImpl implements DataContentService {

    private static final Logger LOG = LoggerFactory.getLogger(DataContentServiceImpl.class);

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
    public ContentGetter getObjectContent(JADEStorageURI contentURI) {
        ContentStorageService contentStorageService = contentStorageServiceProvider.getStorageService(contentURI);
        if (contentStorageService == null) {
            throw new IllegalArgumentException("Invalid storage URI: " + contentURI);
        }
        LOG.debug("Use {} for {}", contentStorageService, contentURI);
        ContentNode contentNode = contentStorageService.getObjectNode(contentURI.getContentKey());
        return new ContentGetterImpl(
                contentStorageService,
                contentAccessProvider,
                Collections.singletonList(contentNode),
                new ContentAccessParams()
        );
    }

    @Override
    public ContentGetter getDataContent(JADEStorageURI storageURI, ContentAccessParams contentAccessParams) {
        ContentStorageService contentStorageService = contentStorageServiceProvider.getStorageService(storageURI);
        if (contentStorageService == null) {
            throw new IllegalArgumentException("Invalid storage URI: " + storageURI);
        }
        LOG.debug("Use {} for {}", contentStorageService, storageURI);
        List<ContentNode> contentNodes = contentStorageService.listContentNodes(storageURI.getContentKey(), contentAccessParams);
        contentNodes.sort((n1, n2) -> ComparatorUtils.naturalCompare(n1.getObjectKey(), n2.getObjectKey(), true)); // sort by key
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
