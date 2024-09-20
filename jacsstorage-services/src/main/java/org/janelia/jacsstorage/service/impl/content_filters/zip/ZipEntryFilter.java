package org.janelia.jacsstorage.service.impl.content_filters.zip;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.coreutils.IOStreamUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.io.ContentConverter;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.io.DataContent;
import org.janelia.jacsstorage.service.ContentException;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.impl.ContentFilter;
import org.janelia.jacsstorage.service.impl.content_filters.ContentNodeHelper;
import org.janelia.rendering.utils.ImageUtils;

public class ZipEntryFilter implements ContentFilter {

    private static final String FILTER_TYPE = "ZIP_ENTRY";

    @Override
    public boolean support(String filterType) {
        return FILTER_TYPE.equalsIgnoreCase(filterType);
    }

    @Override
    public long applyContentFilter(ContentFilterParams filterParams, List<ContentNode> contentNodes, OutputStream outputStream) {
        String entryName = filterParams.getAsString("zipEntryName", "");
        try {
            if (StringUtils.isBlank(entryName)) {
                return 0L;
            } else {
                if (CollectionUtils.isEmpty(contentNodes)) {
                    return 0L;
                } else if (contentNodes.size() == 1) {
                    try (InputStream nodeContent = contentNodes.get(0).getContent()) {
                        return IOStreamUtils.copyFrom(getZipEntryStream(nodeContent, entryName), outputStream);
                    }
                } else {
                    TarArchiveOutputStream archiveOutputStream = new TarArchiveOutputStream(outputStream, TarConstants.DEFAULT_RCDSIZE);
                    String commonPrefix = ContentNodeHelper.commonPrefix(contentNodes);
                    for (ContentNode contentNode : contentNodes) {
                        String tarEntryPrefix = commonPrefix.isEmpty() ? contentNode.getPrefix() : commonPrefix;
                        String tarEntryName = tarEntryPrefix.isEmpty() ? contentNode.getName() : tarEntryPrefix + "/" + contentNode.getName();
                        TarArchiveEntry entry = new TarArchiveEntry(tarEntryName);
                        entry.setSize(contentNode.getSize());
                        archiveOutputStream.putArchiveEntry(entry);
                        try (InputStream nodeContent = contentNode.getContent()) {
                            IOStreamUtils.copyFrom(getZipEntryStream(nodeContent, entryName), archiveOutputStream);
                        }
                        archiveOutputStream.closeArchiveEntry();
                    }
                    archiveOutputStream.finish();
                    return archiveOutputStream.getBytesWritten();
                }
            }
        } catch (Exception e) {
            throw new ContentException(e);
        }
    }

    private byte[] getZipEntryStream(InputStream nodeContent, String entryName) throws IOException {
        ZipInputStream zipInputStream = new ZipInputStream(nodeContent);
        ByteArrayOutputStream entryOutputStream = new ByteArrayOutputStream();
        for (ZipEntry ze = zipInputStream.getNextEntry(); ze != null; ze = zipInputStream.getNextEntry()) {
            if (ze.getName().equals(entryName)) {
                IOStreamUtils.copyFrom(zipInputStream, entryOutputStream);
                zipInputStream.closeEntry();
                break;
            }
        }
        return entryOutputStream.toByteArray();
    }

}
