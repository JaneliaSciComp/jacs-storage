package org.janelia.jacsstorage.coreutils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.CopyOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;
import java.util.function.BiPredicate;


public class PathUtils {
    private static class FileSizeVisitor extends SimpleFileVisitor<Path> {
        private long totalSize = 0L;

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            totalSize += Files.size(dir); // add the size for the directory (inode) entry
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            totalSize += Files.size(file);
            return FileVisitResult.CONTINUE;
        }
    }

    private static class DirTreeCopyVisitor extends SimpleFileVisitor<Path> {
        private final Path source;
        private final Path target;

        DirTreeCopyVisitor(Path source, Path target) {
            this.source = source;
            this.target = target;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            CopyOption[] options = new CopyOption[] {
                    StandardCopyOption.COPY_ATTRIBUTES
            };
            Path newdir = target.resolve(source.relativize(dir));
            Files.copy(dir, newdir, options);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            CopyOption[] options = new CopyOption[] {
                    StandardCopyOption.COPY_ATTRIBUTES,
                    StandardCopyOption.REPLACE_EXISTING
            };
            Path newfile = target.resolve(source.relativize(file));
            Files.copy(file, newfile, options);
            return FileVisitResult.CONTINUE;
        }
    }

    public static List<String> getTreePathComponentsForId(Number id) {
        return id == null ? Collections.emptyList() : getTreePathComponentsForId(id.toString());
    }

    private static List<String> getTreePathComponentsForId(String id) {
        if (StringUtils.isBlank(id)) {
            return Collections.emptyList();
        }
        String trimmedId = id.trim();
        int idLength = trimmedId.length();
        if (idLength < 7) {
            return ImmutableList.of(trimmedId);
        } else {
            return ImmutableList.of(
                    trimmedId.substring(idLength - 6, idLength - 3),
                    trimmedId.substring(idLength - 3),
                    trimmedId);
        }
    }

    public static long getSize(String fn) {
        return getSize(Paths.get(fn));
    }

    public static long getSize(Path fp) {
        Preconditions.checkArgument(Files.exists(fp), "No path found for " + fp);
        FileSizeVisitor pathVisitor = new FileSizeVisitor();
        try {
            Files.walkFileTree(fp, pathVisitor);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return pathVisitor.totalSize;
    }

    public static long getSize(Path fp, BiPredicate<Path, BasicFileAttributes> matcher) {
        Preconditions.checkArgument(Files.exists(fp), "No path found for " + fp);
        try {
            return Files.find(fp, Integer.MAX_VALUE, matcher)
                    .map(p -> {
                        try {
                            return Files.size(p);
                        } catch (IOException e) {
                            return 0L;
                        }
                    })
                    .reduce(0L, (s1, s2) -> s1 + s2);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void deletePath(Path fp) throws IOException {
        Files.walkFileTree(fp, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public static void deletePathIfEmpty(Path fp) throws IOException {
        Files.walkFileTree(fp, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                return FileVisitResult.TERMINATE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public static void copyFiles(Path source, Path target) throws IOException {
        Preconditions.checkArgument(Files.exists(source), "No path found for " + source);
        DirTreeCopyVisitor pathVisitor = new DirTreeCopyVisitor(source, target);
        Files.walkFileTree(source, pathVisitor);
    }
}
