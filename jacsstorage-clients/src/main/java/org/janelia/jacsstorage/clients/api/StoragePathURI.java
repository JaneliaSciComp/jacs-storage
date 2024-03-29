package org.janelia.jacsstorage.clients.api;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@JsonDeserialize(using = StoragePathURIDeserializer.class)
public class StoragePathURI {

    private static final Pattern STORAGE_PATH_URI_PATTERN = Pattern.compile("(?<schema>.+://)?(?<storagePath>.*)");

    private final String schema;
    private final String storagePath;

    public StoragePathURI(String storagePath) {
        if (StringUtils.isBlank(storagePath)) {
            this.schema = "";
            this.storagePath = "";
        } else {
            Matcher m = STORAGE_PATH_URI_PATTERN.matcher(storagePath);
            if (m.matches()) {
                this.schema = StringUtils.defaultIfBlank(m.group("schema"), "");
                this.storagePath = normalizePath(m.group("storagePath"));
            } else {
                this.schema = "";
                this.storagePath = normalizePath(storagePath);
            }
        }
    }

    private StoragePathURI(String schema, String storagePath) {
        this.schema = schema;
        this.storagePath = normalizePath(storagePath);
    }

    /**
     * Normalize the paths by using the UNIX style separator.
     * @param p
     * @return
     */
    private String normalizePath(String p) {
        return p.replace(File.separatorChar, '/');
    }

    public String getStoragePath() {
        return storagePath;
    }

    public Optional<StoragePathURI> getParent() {
        if (isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(new StoragePathURI(schema, getParent(this.storagePath)));
        }
    }

    private String getParent(String fp) {
        if (StringUtils.isBlank(fp)) {
            return "";
        } else {
            Path p = Paths.get(fp).getParent();
            return p != null ? p.toString() : "";
        }
    }

    public Optional<StoragePathURI> resolve(String childPath) {
        if (isEmpty()) {
            return Optional.empty();
        } else {
            StringBuilder childPathURIBuilder = new StringBuilder();
            childPathURIBuilder.append(storagePath)
                    .append('/')
                    .append(childPath);
            return Optional.of(new StoragePathURI(schema, childPathURIBuilder.toString()));
        }
    }

    public boolean isEmpty() {
        return StringUtils.isEmpty(storagePath);
    }

    private String stringify(String schemaValue, String pathValue) {
        return StringUtils.defaultIfBlank(schemaValue, "") + pathValue;
    }

    @JsonValue
    @Override
    public String toString() {
        if (isEmpty()) {
            return "";
        } else {
            return stringify(schema, storagePath);
        }
    }
}
