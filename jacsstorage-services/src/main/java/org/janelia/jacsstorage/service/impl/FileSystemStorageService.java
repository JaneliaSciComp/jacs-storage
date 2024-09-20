package org.janelia.jacsstorage.service.impl;

import java.io.IOException;
import java.io.InputStream;
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

import org.janelia.jacsstorage.coreutils.ComparatorUtils;
import org.janelia.jacsstorage.coreutils.PathUtils;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.service.ContentException;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemStorageService implements ContentStorageService {

    private final static Logger LOG = LoggerFactory.getLogger(FileSystemStorageService.class);

    FileSystemStorageService() {
    }

    @Override
    public List<ContentNode> listContentNodes(String contentLocation, ContentFilterParams filterParams) {
        return listContentFromPath(getContentPath(contentLocation), filterParams);
    }

    private Path getContentPath(String contentLocation) {
        Path contentPath = Paths.get(contentLocation);
        if (Files.notExists(contentPath)) {
            return null;
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

    private List<ContentNode> listContentFromPath(Path contentPath, ContentFilterParams filterParams) {
        if (contentPath == null) {
            return Collections.emptyList();
        }
        if (Files.isDirectory(contentPath, LinkOption.NOFOLLOW_LINKS)) {
            int traverseDepth = filterParams.getMaxDepth() >= 0 ? filterParams.getMaxDepth() : Integer.MAX_VALUE;
            try (Stream<Path> files = Files.walk(contentPath, traverseDepth)) {
                Stream<Path> matchingFiles = files.filter(p -> Files.isDirectory(p) || filterParams.matchEntry(p.toString()));
                Stream<Path> orderedFiles;
                if (filterParams.isNaturalSort()) {
                    orderedFiles = matchingFiles.sorted((p1, p2) -> ComparatorUtils.naturalCompare(p1.toString(), p2.toString(), true));
                } else {
                    orderedFiles = matchingFiles; // no ordering
                }
                Stream<Path> selectedFiles;
                if (filterParams.getEntriesCount() > 0) {
                    selectedFiles = orderedFiles.skip(Math.max(filterParams.getStartEntryIndex(), 0))
                            .limit(filterParams.getEntriesCount());
                } else {
                    selectedFiles = orderedFiles.skip(Math.max(filterParams.getStartEntryIndex(), 0));
                }
                return selectedFiles.map(this::createContentNode)
                        .collect(Collectors.toList())
                        ;
            } catch (Exception e) {
                throw new ContentException("Error reading directory content from: " + contentPath, e);
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
            return new ContentNode(new FileContentReader(p))
                    .setName(p.getFileName().toString())
                    .setPrefix(parent != null ? parent.toString() : "")
                    .setSize(fa.size())
                    .setLastModified(new Date(fa.lastModifiedTime().toMillis()))
                    ;
        } catch (Exception e) {
            throw new ContentException(e);
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
        if (contentPath != null) {
            try {
                PathUtils.deletePath(contentPath);
            } catch (IOException e) {
                throw new ContentException("Error deleting " + contentLocation, e);
            }
        }
    }
}
