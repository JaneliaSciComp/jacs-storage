package org.janelia.jacsstorage.io.contenthandlers.zip;

import java.io.ByteArrayInputStream;
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
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.coreutils.IOStreamUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.io.ContentConverter;
import org.janelia.jacsstorage.io.DataContent;
import org.janelia.jacsstorage.io.DataContentUtils;
import org.janelia.rendering.NamedSupplier;
import org.janelia.rendering.utils.ImageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipEntryContentConverter implements ContentConverter {

    private static final String FILTER_TYPE = "ZIP_ENTRY";

    @Override
    public boolean support(String filterType) {
        return FILTER_TYPE.equalsIgnoreCase(filterType);
    }

    @TimedMethod(
            argList = {0},
            logResult = true
    )
    @Override
    public long convertContent(DataContent dataContent, OutputStream outputStream) {
        String entryName = dataContent.getContentFilterParams().getAsString("zipEntryName", "");
        if (StringUtils.isBlank(entryName)) {
            return 0L;
        } else {
            // only handle the first node for now
            List<DataNodeInfo> peekDataNodes = dataContent.streamDataNodes().filter(dn -> !dn.isCollectionFlag()).limit(1).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(peekDataNodes)) {
                return 0L;
            } else {
                InputStream dataStream = getZipEntryStream(peekDataNodes.get(0), entryName);
                try {
                    return IOStreamUtils.copyFrom(dataStream, outputStream);
                } finally {
                    try {
                        dataStream.close();
                    } catch (Exception ignore) {
                    }
                }
            }
        }
    }

    private InputStream getZipEntryStream(DataNodeInfo dn, String zeName) {
        try {
            ZipFile zf = openZipFile(dn);
            ZipEntry ze = zf.getEntry(zeName);
            if (ze == null) {
                return new ByteArrayInputStream(new byte[0]);
            } else {
                return zf.getInputStream(ze);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private ZipFile openZipFile(DataNodeInfo dn) {
        try {
            return new ZipFile(Paths.get(URI.create(dn.getNodeAccessURL())).toFile());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public long estimateContentSize(DataContent dataContent) {
        String entryName = dataContent.getContentFilterParams().getAsString("zipEntryName", "");
        if (StringUtils.isBlank(entryName)) {
            return 0L;
        } else {
            // only handle the first node for now
            List<DataNodeInfo> peekDataNodes = dataContent.streamDataNodes().filter(dn -> !dn.isCollectionFlag()).limit(1).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(peekDataNodes)) {
                return 0L;
            } else {
                ZipFile zf = openZipFile(peekDataNodes.get(0));
                try {
                    ZipEntry ze = zf.getEntry(entryName);
                    if (ze == null) {
                        return 0L;
                    } else {
                        return ze.getSize();
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                } finally {
                    try {
                        zf.close();
                    } catch (IOException ignore) {
                    }
                }
            }
        }
    }
}
