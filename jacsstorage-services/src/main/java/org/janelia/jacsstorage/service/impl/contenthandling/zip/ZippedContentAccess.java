package org.janelia.jacsstorage.service.impl.contenthandling.zip;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.coreutils.IOStreamUtils;
import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.service.ContentException;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStreamReader;
import org.janelia.jacsstorage.service.impl.ContentAccess;
import org.janelia.jacsstorage.service.impl.contenthandling.ContentNodeHelper;

public class ZippedContentAccess implements ContentAccess {

    @Override
    public boolean isAccessTypeSupported(String contentAccessType) {
        return "ZIP_ENTRY".equalsIgnoreCase(contentAccessType);
    }

    @Override
    public long estimateContentSize(List<ContentNode> contentNodes, ContentAccessParams contentAccessParams, ContentStreamReader contentObjectReader) {
        String entryName = contentAccessParams.getAsString("zipEntryName", "");
        try {
            if (StringUtils.isBlank(entryName)) {
                return 0L;
            } else {
                if (CollectionUtils.isEmpty(contentNodes)) {
                    return 0L;
                } else if (contentNodes.size() == 1) {
                    try (InputStream nodeContentStream = contentObjectReader.readContent(contentNodes.get(0).getObjectKey())) {
                        return getZipEntrySize(nodeContentStream, entryName);
                    }
                } else {
                    long totalSize = 0;
                    for (ContentNode contentNode : contentNodes) {
                        try (InputStream nodeContentStream = contentObjectReader.readContent(contentNode.getObjectKey())) {
                            long entrySize = getZipEntrySize(nodeContentStream, entryName);
                            totalSize += entrySize;
                        }
                    }
                    return totalSize;
                }
            }
        } catch (Exception e) {
            throw new ContentException(e);
        }
    }

    @Override
    public long retrieveContent(List<ContentNode> contentNodes,
                                ContentAccessParams filterParams,
                                ContentStreamReader contentObjectReader,
                                OutputStream outputStream) {
        String entryName = filterParams.getAsString("zipEntryName", "");
        try {
            if (StringUtils.isBlank(entryName)) {
                return 0L;
            } else {
                if (CollectionUtils.isEmpty(contentNodes)) {
                    return 0L;
                } else if (contentNodes.size() == 1) {
                    try (InputStream nodeContentStream = contentObjectReader.readContent(contentNodes.get(0).getObjectKey())) {
                        return IOStreamUtils.copyFrom(getZipEntryStream(nodeContentStream, entryName), outputStream);
                    }
                } else {
                    TarArchiveOutputStream archiveOutputStream = new TarArchiveOutputStream(outputStream, TarConstants.DEFAULT_RCDSIZE);
                    String commonPrefix = ContentNodeHelper.commonPrefix(contentNodes);
                    for (ContentNode contentNode : contentNodes) {
                        String tarEntryPrefix = commonPrefix.isEmpty() ? contentNode.getPrefix() : commonPrefix;
                        String tarEntryName = tarEntryPrefix.isEmpty() ? contentNode.getName() : tarEntryPrefix + "/" + contentNode.getName();
                        TarArchiveEntry entry = new TarArchiveEntry(tarEntryName);
                        archiveOutputStream.putArchiveEntry(entry);
                        try (InputStream nodeContentStream = contentObjectReader.readContent(contentNode.getObjectKey())) {
                            long entrySize = IOStreamUtils.copyFrom(getZipEntryStream(nodeContentStream, entryName), archiveOutputStream);
                            entry.setSize(entrySize);
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

    private long getZipEntrySize(InputStream nodeContent, String entryName) throws IOException {
        ZipInputStream zipInputStream = new ZipInputStream(nodeContent);
        for (ZipEntry ze = zipInputStream.getNextEntry(); ze != null; ze = zipInputStream.getNextEntry()) {
            if (ze.getName().equals(entryName)) {
                return ze.getSize();
            }
        }
        return 0L;
    }

}
