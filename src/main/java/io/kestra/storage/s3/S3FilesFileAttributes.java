package io.kestra.storage.s3;

import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;

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
        return Map.of();
    }
}
