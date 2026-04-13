package io.kestra.storage.s3;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.storages.FileAttributes;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.storages.StorageObject;

import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotEmpty;
import lombok.*;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Jacksonized
@Getter
@Plugin
@Plugin.Id("s3files")
@Slf4j
public class S3FilesStorage implements StorageInterface {
    @NotEmpty
    private String mountPath;

    @Getter(AccessLevel.PRIVATE)
    private transient Path basePath;

    @Override
    public void init() {
        this.basePath = Paths.get(mountPath).toAbsolutePath().normalize();
        if (!Files.exists(this.basePath) || !Files.isDirectory(this.basePath)) {
            throw new RuntimeException("Mount path must exist and be a directory: " + mountPath);
        }
    }

    private Path resolveLocalPath(String kestraPath) {
        String relative = kestraPath.startsWith("/") ? kestraPath.substring(1) : kestraPath;
        return basePath.resolve(relative).normalize();
    }

    private void guardTraversal(Path p) throws IOException {
        if (!p.startsWith(basePath)) throw new IOException("Path traversal attempt: " + p);
    }

    @Override
    public String getPath(String tenantId, URI uri) {
        return StorageInterface.super.getPath(tenantId, uri);
    }

    @Override
    public boolean exists(String tenantId, @Nullable String namespace, URI uri) {
        String path = getPath(tenantId, uri);
        try {
            Path p = resolveLocalPath(path);
            guardTraversal(p);
            return Files.exists(p);
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public boolean existsInstanceResource(@Nullable String namespace, URI uri) {
        return exists(getPath(uri));
    }

    private boolean exists(String path) {
        try {
            Path p = resolveLocalPath(path);
            guardTraversal(p);
            return Files.exists(p);
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public InputStream get(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        return this.getWithMetadata(tenantId, namespace, uri).inputStream();
    }

    @Override
    public InputStream getInstanceResource(@Nullable String namespace, URI uri) throws IOException {
        return this.getWithMetadata(getPath(uri)).inputStream();
    }

    @Override
    public StorageObject getWithMetadata(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        Path p = resolveLocalPath(path);
        guardTraversal(p);
        if (!Files.exists(p)) {
            throw new FileNotFoundException();
        }
        return new StorageObject(Map.of(), Files.newInputStream(p));
    }

    private StorageObject getWithMetadata(String path) throws IOException {
        Path p = resolveLocalPath(path);
        guardTraversal(p);
        if (!Files.exists(p)) {
            throw new FileNotFoundException();
        }
        return new StorageObject(Map.of(), Files.newInputStream(p));
    }

    @Override
    public List<URI> allByPrefix(String tenantId, @Nullable String namespace, URI prefix, boolean includeDirectories) {
        String path = getPath(tenantId, prefix);
        Path start = resolveLocalPath(path);
        try {
            if (!Files.exists(start)) {
                return List.of();
            }

            return Files.walk(start)
                .filter(p -> includeDirectories || !Files.isDirectory(p))
                .map(p -> {
                    String relative = start.relativize(p).toString().replace("\\", "/");
                    String prefixPath = prefix.getPath();
                    String combined = prefixPath + (prefixPath.endsWith("/") || relative.isEmpty() ? "" : "/") + relative;
                    return URI.create("kestra://" + combined);
                })
                .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<FileAttributes> list(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        Path p = resolveLocalPath(path);
        guardTraversal(p);
        if (!Files.exists(p) || !Files.isDirectory(p)) {
            throw new FileNotFoundException();
        }
        try (var stream = Files.list(p)) {
            return stream
                .map(child -> {
                    try {
                        BasicFileAttributes attrs = Files.readAttributes(child, BasicFileAttributes.class);
                        return new S3FilesFileAttributes(child, attrs);
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                })
                .collect(Collectors.toList());
        }
    }

    @Override
    public List<FileAttributes> listInstanceResource(@Nullable String namespace, URI uri) throws IOException {
        return list(getPath(uri));
    }

    private List<FileAttributes> list(String path) throws IOException {
        Path p = resolveLocalPath(path);
        guardTraversal(p);
        if (!Files.exists(p) || !Files.isDirectory(p)) {
            throw new FileNotFoundException();
        }
        try (var stream = Files.list(p)) {
            return stream
                .map(child -> {
                    try {
                        BasicFileAttributes attrs = Files.readAttributes(child, BasicFileAttributes.class);
                        return new S3FilesFileAttributes(child, attrs);
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                })
                .collect(Collectors.toList());
        }
    }

    @Override
    public FileAttributes getAttributes(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        return getAttributes(path);
    }

    @Override
    public FileAttributes getInstanceAttributes(@Nullable String namespace, URI uri) throws IOException {
        return getAttributes(getPath(uri));
    }

    private FileAttributes getAttributes(String path) throws IOException {
        Path p = resolveLocalPath(path);
        guardTraversal(p);
        if (!Files.exists(p)) {
            throw new FileNotFoundException();
        }
        BasicFileAttributes attrs = Files.readAttributes(p, BasicFileAttributes.class);
        return new S3FilesFileAttributes(p, attrs);
    }

    @Override
    public URI put(String tenantId, @Nullable String namespace, URI uri, StorageObject storageObject) throws IOException {
        String path = getPath(tenantId, uri);
        Path dest = resolveLocalPath(path);
        guardTraversal(dest);
        Files.createDirectories(dest.getParent());
        try (InputStream in = storageObject.inputStream()) {
            Files.copy(in, dest, StandardCopyOption.REPLACE_EXISTING);
        }
        return createUri(uri.getPath());
    }

    @Override
    public URI putInstanceResource(@Nullable String namespace, URI uri, StorageObject storageObject) throws IOException {
        String path = getPath(uri);
        Path dest = resolveLocalPath(path);
        guardTraversal(dest);
        Files.createDirectories(dest.getParent());
        try (InputStream in = storageObject.inputStream()) {
            Files.copy(in, dest, StandardCopyOption.REPLACE_EXISTING);
        }
        return createUri(uri.getPath());
    }

    @Override
    public URI createDirectory(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        Path p = resolveLocalPath(path);
        guardTraversal(p);
        Files.createDirectories(p);
        return createUri(uri.getPath());
    }

    @Override
    public URI createInstanceDirectory(String namespace, URI uri) throws IOException {
        String path = getPath(uri);
        Path p = resolveLocalPath(path);
        guardTraversal(p);
        Files.createDirectories(p);
        return createUri(uri.getPath());
    }

    @Override
    public boolean delete(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        FileAttributes fileAttributes;
        try {
            fileAttributes = getAttributes(tenantId, namespace, uri);
        } catch (FileNotFoundException e) {
            return false;
        }
        if (fileAttributes.getType() == FileAttributes.FileType.Directory) {
            deleteByPrefix(tenantId, namespace, uri.getPath().endsWith("/") ? uri : URI.create(uri + "/"));
        }

        return deleteSingleObject(getPath(tenantId, uri));
    }

    @Override
    public boolean deleteInstanceResource(@Nullable String namespace, URI uri) throws IOException {
        FileAttributes fileAttributes;
        try {
            fileAttributes = getInstanceAttributes(namespace, uri);
        } catch (FileNotFoundException e) {
            return false;
        }
        String path = getPath(uri);
        if (fileAttributes.getType() == FileAttributes.FileType.Directory) {
            deleteByPrefix(null, uri.getPath().endsWith("/") ? path : path + "/");
        }

        return deleteSingleObject(path);
    }

    private boolean deleteSingleObject(String path) throws IOException {
        Path p = resolveLocalPath(path);
        guardTraversal(p);
        if (!Files.exists(p)) {
            return false;
        }
        Files.delete(p);
        return true;
    }

    @Override
    public URI move(String tenantId, @Nullable String namespace, URI from, URI to) throws IOException {
        String source = getPath(tenantId, from);
        String dest = getPath(tenantId, to);
        FileAttributes attributes = getAttributes(tenantId, namespace, from);
        if (attributes.getType() == FileAttributes.FileType.Directory) {
            Path srcStart = resolveLocalPath(source);
            Path dstStart = resolveLocalPath(dest);
            try {
                Files.walk(srcStart).forEach(p -> {
                    try {
                        Path relative = srcStart.relativize(p);
                        Path target = dstStart.resolve(relative);
                        Files.createDirectories(target.getParent());
                        Files.move(p, target, StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            } catch (RuntimeException e) {
                if (e.getCause() instanceof IOException io) {
                    throw io;
                }
                throw e;
            }
        } else {
            Path src = resolveLocalPath(source);
            Path dst = resolveLocalPath(dest);
            guardTraversal(src);
            guardTraversal(dst);
            Files.createDirectories(dst.getParent());
            Files.move(src, dst, StandardCopyOption.REPLACE_EXISTING);
        }

        return createUri(to.getPath());
    }

    @Override
    public List<URI> deleteByPrefix(String tenantId, @Nullable String namespace, URI storagePrefix) throws IOException {
        String path = getPath(tenantId, storagePrefix);
        return deleteByPrefix(tenantId, path);
    }

    private List<URI> deleteByPrefix(String tenantId, String path) throws IOException {
        Path start = resolveLocalPath(path);
        guardTraversal(start);
        if (!Files.exists(start)) {
            return new ArrayList<>();
        }

        List<Path> paths = Files.walk(start).sorted(Comparator.reverseOrder()).collect(Collectors.toList());
        List<URI> deleted = new ArrayList<>();
        for (Path p : paths) {
            Files.deleteIfExists(p);
            String relative = start.relativize(p).toString().replace("\\", "/");
            String combined = path + (path.endsWith("/") || relative.isEmpty() ? "" : "/") + relative;
            deleted.add(URI.create("kestra://" + combined));
        }
        return deleted;
    }

    @Override
    public void close() {
        // no-op
    }

    private static URI createUri(String key) {
        return URI.create("kestra://%s".formatted(key));
    }
}
