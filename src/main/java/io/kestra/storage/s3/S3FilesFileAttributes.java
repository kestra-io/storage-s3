package io.kestra.storage.s3;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.kestra.core.storages.FileAttributes;
import lombok.Value;

@Value
public class S3FilesFileAttributes implements FileAttributes {
    Path filePath;
    BasicFileAttributes basicFileAttributes;

    @Override
    public String getFileName() {
        return filePath.getFileName() != null ? filePath.getFileName().toString() : "/";
    }

    @Override
    public long getLastModifiedTime() {
        return basicFileAttributes.lastModifiedTime().toMillis();
    }

    @Override
    public long getCreationTime() {
        return basicFileAttributes.creationTime().toMillis();
    }

    @Override
    public FileType getType() {
        return basicFileAttributes.isDirectory() ? FileType.Directory : FileType.File;
    }

    @Override
    public long getSize() {
        return basicFileAttributes.size();
    }

    @Override
    public Map<String, String> getMetadata() {
        Path metaPath = Path.of(filePath + ".meta");
        if (!Files.exists(metaPath)) {
            return Map.of();
        }
        try {
            var props = new Properties();
            try (InputStream in = Files.newInputStream(metaPath)) {
                props.load(in);
            }
            var stored = new HashMap<String, String>();
            for (var key : props.stringPropertyNames()) {
                stored.put(key, props.getProperty(key));
            }
            return MetadataUtils.toRetrievedMetadata(stored);
        } catch (IOException e) {
            return Map.of();
        }
    }
}
