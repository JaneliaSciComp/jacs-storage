package org.janelia.jacsstorage.service.impl;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import javax.inject.Inject;

import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStorageService;
import org.janelia.jacsstorage.service.DataContentService;

public class DataContentServiceImpl implements DataContentService {

    private final ContentStorageServiceProvider contentStorageServiceProvider;
    private final ContentFilterProvider contentFilterProvider;

    @Inject
    DataContentServiceImpl(ContentStorageServiceProvider contentStorageServiceProvider,
                           ContentFilterProvider contentFilterProvider) {
        this.contentStorageServiceProvider = contentStorageServiceProvider;
        this.contentFilterProvider = contentFilterProvider;
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
        ContentFilter contentFilter = contentFilterProvider.getContentFilter(filterParams);
        return contentFilter.applyContentFilter(filterParams, contentNodes, dataStream);
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
