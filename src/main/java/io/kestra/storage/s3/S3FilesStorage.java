package io.kestra.storage.s3;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        return new StorageObject(readMetaSidecar(p), Files.newInputStream(p));
    }

    private StorageObject getWithMetadata(String path) throws IOException {
        Path p = resolveLocalPath(path);
        guardTraversal(p);
        if (!Files.exists(p)) {
            throw new FileNotFoundException();
        }
        return new StorageObject(readMetaSidecar(p), Files.newInputStream(p));
    }

    private Map<String, String> readMetaSidecar(Path p) throws IOException {
        Path metaPath = Path.of(p + ".meta");
        if (!Files.exists(metaPath)) {
            return Map.of();
        }
        var props = new Properties();
        try (InputStream metaIn = Files.newInputStream(metaPath)) {
            props.load(metaIn);
        }
        var stored = new HashMap<String, String>();
        for (var key : props.stringPropertyNames()) {
            stored.put(key, props.getProperty(key));
        }
        return MetadataUtils.toRetrievedMetadata(stored);
    }

    private void writeMetaSidecar(Path dest, Map<String, String> metadata) throws IOException {
        Path metaPath = Path.of(dest + ".meta");
        if (metadata != null && !metadata.isEmpty()) {
            var props = new Properties();
            props.putAll(MetadataUtils.toStoredMetadata(metadata));
            try (OutputStream os = Files.newOutputStream(metaPath)) {
                props.store(os, null);
            }
        } else {
            Files.deleteIfExists(metaPath);
        }
    }

    @Override
    public List<URI> allByPrefix(String tenantId, @Nullable String namespace, URI prefix, boolean includeDirectories) {
        String path = getPath(tenantId, prefix);
        Path start = resolveLocalPath(path);
        try {
            guardTraversal(start);
            if (!Files.exists(start)) {
                return List.of();
            }

            try (Stream<Path> walk = Files.walk(start)) {
                return walk
                    .filter(p -> !p.equals(start))
                    .filter(p -> !p.getFileName().toString().endsWith(".meta"))
                    .filter(p -> includeDirectories || !Files.isDirectory(p))
                    .map(p -> {
                        String relative = start.relativize(p).toString().replace("\\", "/");
                        String prefixPath = prefix.getPath();
                        String combined = prefixPath + (prefixPath.endsWith("/") || relative.isEmpty() ? "" : "/") + relative;
                        if (Files.isDirectory(p) && !combined.endsWith("/")) {
                            combined = combined + "/";
                        }
                        return URI.create("kestra://" + combined);
                    })
                    .collect(Collectors.toList());
            }
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
                .filter(child -> !child.getFileName().toString().endsWith(".meta"))
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
                .filter(child -> !child.getFileName().toString().endsWith(".meta"))
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
        writeMetaSidecar(dest, storageObject.metadata());
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
        writeMetaSidecar(dest, storageObject.metadata());
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
            return true;
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
            return true;
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
        Files.deleteIfExists(Path.of(p + ".meta"));
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
            guardTraversal(srcStart);
            guardTraversal(dstStart);
            Files.createDirectories(dstStart);
            try (Stream<Path> walk = Files.walk(srcStart)) {
                List<Path> entries = walk.collect(Collectors.toList());
                for (var p : entries) {
                    if (p.equals(srcStart) || Files.isDirectory(p)) continue;
                    var relative = srcStart.relativize(p);
                    var target = dstStart.resolve(relative);
                    Files.createDirectories(target.getParent());
                    Files.move(p, target, StandardCopyOption.REPLACE_EXISTING);
                }
            }
            // Remove now-empty source subtree
            try (Stream<Path> cleanup = Files.walk(srcStart)) {
                cleanup.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        } else {
            Path src = resolveLocalPath(source);
            Path dst = resolveLocalPath(dest);
            guardTraversal(src);
            guardTraversal(dst);
            Files.createDirectories(dst.getParent());
            Files.move(src, dst, StandardCopyOption.REPLACE_EXISTING);
            Path srcMeta = Path.of(src + ".meta");
            if (Files.exists(srcMeta)) {
                Files.move(srcMeta, Path.of(dst + ".meta"), StandardCopyOption.REPLACE_EXISTING);
            }
        }

        return createUri(to.getPath());
    }

    @Override
    public List<URI> deleteByPrefix(String tenantId, @Nullable String namespace, URI storagePrefix) throws IOException {
        String path = getPath(tenantId, storagePrefix);
        return deleteByPrefix(tenantId, path);
    }

    private static String removeTenant(String tenantId, String k) {
        return tenantId == null ? "/" + k : k.replaceFirst(tenantId, "");
    }

    private List<URI> deleteByPrefix(String tenantId, String path) throws IOException {
        Path start = resolveLocalPath(path);
        guardTraversal(start);
        if (!Files.exists(start)) {
            return new ArrayList<>();
        }

        List<Path> paths;
        try (Stream<Path> walk = Files.walk(start)) {
            paths = walk.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
        }
        List<URI> deleted = new ArrayList<>();
        for (Path p : paths) {
            Files.deleteIfExists(p);
            if (p.getFileName().toString().endsWith(".meta")) {
                continue;
            }
            String relative = start.relativize(p).toString().replace("\\", "/");
            String combined = path + (path.endsWith("/") || relative.isEmpty() ? "" : "/") + relative;
            String tenantStripped = removeTenant(tenantId, combined);
            if (tenantStripped.endsWith("/")) {
                tenantStripped = tenantStripped.substring(0, tenantStripped.length() - 1);
            }
            deleted.add(createUri(tenantStripped));
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
