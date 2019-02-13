package org.janelia.jacsstorage.io;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.janelia.jacsstorage.coreutils.IOStreamUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ArchiveEntryListDataContent extends AbstractDataContent {

    private final static Logger LOG = LoggerFactory.getLogger(ArchiveEntryListDataContent.class);

    private final Map<DataNodeInfo, byte[]> dataNodeList = new LinkedHashMap<>();
    private final String rootEntry;

    ArchiveEntryListDataContent(ContentFilterParams contentFilterParams, String rootEntry) {
        super(contentFilterParams);
        this.rootEntry = rootEntry;
    }

    @Override
    public List<DataNodeInfo> listDataNodes() {
        return ImmutableList.copyOf(dataNodeList.keySet());
    }

    @Override
    public InputStream streamDataNode(DataNodeInfo dn) {
        if (dn.isCollectionFlag()) {
            throw new IllegalArgumentException("Streaming is not supported for directories - " + dn.getNodeAccessURL());
        } else {
            return new ByteArrayInputStream(dataNodeList.get(dn));
        }
    }

    void addArchiveEntry(String entryName, boolean isDir, long size, InputStream inputStream) {
        ByteArrayOutputStream entryContent = new ByteArrayOutputStream();
        IOStreamUtils.copyFrom(ByteStreams.limit(inputStream, size), entryContent);
        dataNodeList.put(DataContentUtils.createDataNodeInfo(Paths.get(rootEntry), Paths.get(entryName), isDir, size), entryContent.toByteArray());
    }

    int getEntriesCount() {
        return dataNodeList.size();
    }
}
