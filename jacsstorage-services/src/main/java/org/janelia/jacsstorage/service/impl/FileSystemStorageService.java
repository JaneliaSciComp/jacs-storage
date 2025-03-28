package org.janelia.jacsstorage.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.janelia.jacsstorage.coreutils.IOStreamUtils;
import org.janelia.jacsstorage.coreutils.PathUtils;
import org.janelia.jacsstorage.model.jacsstorage.JADEOptions;
import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;
import org.janelia.jacsstorage.service.ContentException;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStorageService;
import org.janelia.jacsstorage.service.NoContentFoundException;
import org.janelia.jacsstorage.service.StorageCapacity;
import org.janelia.rendering.utils.ImageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemStorageService implements ContentStorageService {

    private final static Logger LOG = LoggerFactory.getLogger(FileSystemStorageService.class);

    FileSystemStorageService() {
    }

    @Override
    public boolean canAccess(String contentLocation) {
        Path contentPath = Paths.get(contentLocation);
        if (Files.exists(contentPath)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public ContentNode getObjectNode(String contentLocation) {
        Path contentPath = Paths.get(contentLocation);
        if (Files.exists(contentPath) && Files.isRegularFile(contentPath)) {
            return createContentNode(contentPath);
        } else {
            throw new ContentException("Content not found for " + contentLocation);
        }
    }

    @Override
    public List<ContentNode> listContentNodes(String contentLocation, ContentAccessParams filterParams) {
        return listContentFromPath(getContentPath(contentLocation), filterParams);
    }

    private Path getContentPath(String contentLocation) {
        Path contentPath = Paths.get(contentLocation);
        if (Files.notExists(contentPath)) {
            throw new NoContentFoundException("No content found at " + contentLocation);
        }
        if (Files.isSymbolicLink(contentPath)) {
            try {
                return contentPath.toRealPath();
            } catch (IOException e) {
                throw new ContentException("Could not retrieve content from: " + contentPath, e);
            }
        } else {
            return contentPath;
        }
    }

    private List<ContentNode> listContentFromPath(Path contentPath, ContentAccessParams contentAccessParams) {
        if (Files.isDirectory(contentPath, LinkOption.NOFOLLOW_LINKS)) {
            long startTime = System.currentTimeMillis();
            int traverseDepth = contentAccessParams.getMaxDepth() >= 0 ? contentAccessParams.getMaxDepth() : Integer.MAX_VALUE;
            try (Stream<Path> files = Files.walk(contentPath, traverseDepth)) {
                Stream<Path> matchingFiles = files.filter(p -> Files.isDirectory(p) || contentAccessParams.matchEntry(p.toString()));
                Stream<Path> selectedFiles;
                if (contentAccessParams.getEntriesCount() > 0) {
                    selectedFiles = matchingFiles.skip(Math.max(contentAccessParams.getStartEntryIndex(), 0))
                            .limit(contentAccessParams.getEntriesCount());
                } else {
                    selectedFiles = matchingFiles.skip(Math.max(contentAccessParams.getStartEntryIndex(), 0));
                }
                // if directories only returned nodes only have directories,
                // otherwise they will have both files and directories
                return selectedFiles
                        .filter(p -> !contentAccessParams.isDirectoriesOnly() || Files.isDirectory(p))
                        .map(this::createContentNode)
                        .collect(Collectors.toList())
                        ;
            } catch (Exception e) {
                throw new ContentException("Error reading directory content from: " + contentPath, e);
            } finally {
                LOG.info("List content {} with {} - {} secs", contentPath, contentAccessParams, (System.currentTimeMillis() - startTime) / 1000.);
            }
        } else if (Files.isRegularFile(contentPath, LinkOption.NOFOLLOW_LINKS)) {
            return Collections.singletonList(createContentNode(contentPath));
        } else {
            throw new ContentException("Cannot handle reading content from " + contentPath);
        }
    }

    private ContentNode createContentNode(Path p) {
        try {
            BasicFileAttributes fa = Files.readAttributes(p, BasicFileAttributes.class);
            Path parent = p.getParent();
            return new ContentNode(JacsStorageType.FILE_SYSTEM, JADEStorageURI.createStoragePathURI("", JADEOptions.create()))
                    .setName(p.getFileName().toString() + (Files.isDirectory(p) ? "/" : ""))
                    .setPrefix(parent != null ? parent.toString() : "")
                    .setSize(Files.isDirectory(p) ? 0 : fa.size())
                    .setLastModified(new Date(fa.lastModifiedTime().toMillis()))
                    .setCollection(Files.isDirectory(p))
                    ;
        } catch (Exception e) {
            throw new ContentException(e);
        }
    }

    @Override
    public InputStream getContentInputStream(String contentLocation) {
        Path contentPath = Paths.get(contentLocation);

        if (Files.exists(contentPath)) {
            if (Files.isRegularFile(contentPath)) {
                return ImageUtils.openSeekableStream(contentPath);
            } else {
                throw new ContentException("Content found at " + contentLocation + " is not a regular file");
            }
        } else {
            throw new NoContentFoundException("No object found at " + contentLocation);
        }
    }

    @Override
    public long streamContentToOutput(String contentLocation, OutputStream outputStream) {
        try (InputStream is = getContentInputStream(contentLocation)) {
            return IOStreamUtils.copyFrom(is, outputStream);
        } catch (Exception e) {
            throw new ContentException("Error streaming " + contentLocation, e);
        }
    }

    @Override
    public long writeContent(String contentLocation, InputStream inputStream) {
        Path contentPath = Paths.get(contentLocation);

        if (Files.exists(contentPath)) {
            if (Files.isDirectory(contentPath)) {
                throw new ContentException("Folder cannot be overwritten at: " + contentLocation);
            } else if (Files.isRegularFile(contentPath)) {
                LOG.info("File {} will be overwritten", contentLocation);
            }
        }
        try {
            Files.createDirectories(contentPath.getParent());
            return Files.copy(inputStream, contentPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new ContentException(e);
        }
    }

    @Override
    public void deleteContent(String contentLocation) {
        Path contentPath = getContentPath(contentLocation);
        try {
            PathUtils.deletePath(contentPath);
        } catch (IOException e) {
            throw new ContentException("Error deleting " + contentLocation, e);
        }
    }

    @Override
    public StorageCapacity getStorageCapacity(String contentLocation) {
        Path contentPath = getContentPath(contentLocation);
        try {
            FileStore fs = Files.getFileStore(contentPath);
            return new StorageCapacity(
                    fs.getTotalSpace(),
                    fs.getUsableSpace()
            );
        } catch (IOException e) {
            throw new ContentException(e);
        }
    }


}
