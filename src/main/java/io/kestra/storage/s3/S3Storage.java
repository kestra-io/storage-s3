package io.kestra.storage.s3;

import io.kestra.core.storages.FileAttributes;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.core.annotation.Introspected;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.Upload;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;
import software.amazon.awssdk.utils.builder.SdkBuilder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@Singleton
@Introspected
@S3StorageEnabled
public class S3Storage implements StorageInterface {
    S3Client s3Client;

    S3AsyncClient s3AsyncClient;

    private final S3Config s3Config;

    public S3Storage(S3Config s3Config) {
        this.s3Config = s3Config;
        this.s3Client = S3ClientFactory.getS3Client(s3Config);
        this.s3AsyncClient = S3ClientFactory.getAsyncS3Client(s3Config);
    }

    public String createBucket() throws IOException {
        try {
            CreateBucketRequest request = CreateBucketRequest.builder().bucket(s3Config.getBucket()).build();
            s3Client.createBucket(request);
            return s3Config.getBucket();
        } catch (AwsServiceException exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public InputStream get(String tenantId, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        return get(path);
    }

    private InputStream get(String path) throws IOException {
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                .bucket(s3Config.getBucket())
                .key(path)
                .build();
            ResponseInputStream<GetObjectResponse> inputStream = s3Client.getObject(request);
            return inputStream.response().contentLength() == 0 ? InputStream.nullInputStream() : inputStream;
        } catch (NoSuchKeyException exception) {
            throw new FileNotFoundException();
        } catch (AwsServiceException exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public List<FileAttributes> list(String tenantId, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        String prefix = path.endsWith("/") ? path : path + "/";
        try {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(s3Config.getBucket())
                .prefix(prefix)
                .build();
            List<S3Object> contents = s3Client.listObjectsV2(request).contents();
            List<FileAttributes> list = contents.stream()
                .map(S3Object::key)
                .filter(key -> {
                    key = key.substring(prefix.length());
                    // Remove recursive result and requested dir
                    return !key.isEmpty() && !Objects.equals(key, prefix) && Path.of(key).getParent() == null;
                })
                .map(throwFunction(this::getFileAttributes))
                .toList();
            if (list.isEmpty()) {
                // s3 does not handle directory deleting with a prefix that does not exist will just delete nothing
                // Deleting an "empty directory" will at least return the directory name
                throw new FileNotFoundException(uri + " (Not Found)");
            }
            return list;
        } catch (NoSuchKeyException exception) {
            throw new FileNotFoundException();
        } catch (AwsServiceException exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public Long size(String tenantId, URI uri) throws IOException {
        try {
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(s3Config.getBucket())
                .key(getPath(tenantId, uri))
                .build();
            return s3Client.headObject(headObjectRequest).contentLength();
        } catch (NoSuchKeyException exception) {
            throw new FileNotFoundException();
        } catch (AwsServiceException exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public Long lastModifiedTime(String tenantId, URI uri) throws IOException {
        try {
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(s3Config.getBucket())
                .key(getPath(tenantId, uri))
                .build();
            return s3Client.headObject(headObjectRequest).lastModified().getEpochSecond();
        } catch (NoSuchKeyException exception) {
            throw new FileNotFoundException();
        } catch (AwsServiceException exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public FileAttributes getAttributes(String tenantId, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        try {
            return getFileAttributes(path);
        } catch (FileNotFoundException e) {
            if (path.endsWith("/")) {
                throw e;
            }
            return getFileAttributes(path + "/");
        }
    }

    private FileAttributes getFileAttributes(String path) throws IOException {
        try {
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(s3Config.getBucket())
                .key(path)
                .build();
            S3FileAttributes.S3FileAttributesBuilder builder = S3FileAttributes.builder()
                .fileName(Optional.ofNullable(Path.of(path).getFileName()).map(Path::toString)
                    .orElse("/"))
                .head(s3Client.headObject(headObjectRequest));
            if (path.endsWith("/")) {
                builder.isDirectory(true);
            }
            return builder
                .build();
        } catch (NoSuchKeyException exception) {
            throw new FileNotFoundException();
        } catch (AwsServiceException exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public URI put(String tenantId, URI uri, InputStream data) throws IOException {
        try {
            int length = data.available();

            String path = getPath(tenantId, uri);
            mkdirs(path);
            PutObjectRequest request = PutObjectRequest.builder()
                .bucket(s3Config.getBucket())
                .key(path)
                .build();

            Optional<Upload> upload;
            try (S3TransferManager transferManager = S3TransferManager.builder().s3Client(s3AsyncClient).build()) {
                UploadRequest.Builder uploadRequest = UploadRequest.builder()
                    .putObjectRequest(request)
                    .requestBody(AsyncRequestBody.fromInputStream(
                        data,
                        (long) length,
                        Executors.newSingleThreadExecutor()
                    ));

                upload = Optional.of(transferManager.upload(uploadRequest.build()));
            }

            PutObjectResponse response = upload.orElseThrow(IOException::new).completionFuture().get().response();
            return createUri(tenantId, uri.getPath());
        } catch (AwsServiceException exception) {
            throw new IOException(exception);
        } catch (ExecutionException | InterruptedException exception) {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public boolean delete(String tenantId, URI uri) throws IOException {
        return !deleteByPrefix(tenantId, uri).isEmpty();
    }

    @Override
    public URI createDirectory(String tenantId, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        if (!StringUtils.endsWith(path, "/")) {
            path += "/";
        }
        mkdirs(path);
        PutObjectRequest putRequest = PutObjectRequest.builder()
            .bucket(s3Config.getBucket())
            .key(path)
            .build();
        s3Client.putObject(putRequest, RequestBody.empty());
        return createUri(tenantId, uri.getPath());
    }

    private void mkdirs(String path) throws IOException {
        path = path.replaceAll("^/*", "");
        String[] directories = path.split("/");
        StringBuilder aggregatedPath = new StringBuilder("/");
        try {
            // perform 1 put request per parent directory in the path
            for (int i = 0; i <= directories.length - (path.endsWith("/") ? 1 : 2); i++) {
                aggregatedPath.append(directories[i]).append("/");
                PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(s3Config.getBucket())
                    .key(aggregatedPath.toString())
                    .build();
                s3Client.putObject(putRequest, RequestBody.empty());
            }
        } catch (AwsServiceException | SdkClientException exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public URI move(String tenantId, URI from, URI to) throws IOException {
        String source = getPath(tenantId, from);
        String dest = getPath(tenantId, to);
        try {
            FileAttributes attributes = getAttributes(tenantId, from);
            if (attributes.getType() == FileAttributes.FileType.Directory) {
                ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                    .bucket(s3Config.getBucket())
                    .prefix(source)
                    .build();

                ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
                List<S3Object> objects = listResponse.contents();
                if (objects.isEmpty()) {
                    throw new FileNotFoundException(to + " (Not Found)");
                }

                for (S3Object object : objects) {
                    String newKey = dest + object.key().substring(source.length());
                    move(object.key(), newKey);
                }
            } else {
                move(source, dest);
            }

            return createUri(tenantId, to.getPath());
        } catch (AwsServiceException | SdkClientException exception) {
            throw new IOException(exception);
        }
    }

    private void move(String oldKey, String newKey) {
        CopyObjectRequest copyRequest = CopyObjectRequest.builder()
            .sourceBucket(s3Config.getBucket())
            .sourceKey(oldKey)
            .destinationBucket(s3Config.getBucket())
            .destinationKey(newKey)
            .build();
        s3Client.copyObject(copyRequest);

        DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
            .bucket(s3Config.getBucket())
            .key(oldKey)
            .build();
        s3Client.deleteObject(deleteRequest);
    }

    @Override
    public List<URI> deleteByPrefix(String tenantId, URI storagePrefix) throws IOException {
        ListObjectsRequest listRequest = ListObjectsRequest.builder()
            .bucket(s3Config.getBucket())
            .prefix(getPath(tenantId, storagePrefix))
            .build();
        ListObjectsResponse objectListing = s3Client.listObjects(listRequest);

        List<S3Object> s3Objects = objectListing.getValueForField("Contents", List.class).get();

        List<ObjectIdentifier> keys = s3Objects.stream()
            .map(S3Object::key)
            .map(ObjectIdentifier.builder()::key)
            .map(SdkBuilder::build)
            .toList();

        DeleteObjectsRequest deleteRequest = DeleteObjectsRequest.builder()
            .bucket(s3Config.getBucket())
            .delete(builder -> builder.objects(keys))
            .build();

        if (keys.isEmpty()) {
            return new ArrayList<>();
        }

        try {
            DeleteObjectsResponse result = s3Client.deleteObjects(deleteRequest);

            return result.deleted().stream()
                .map(DeletedObject::key)
                .map(k -> (k.endsWith("/")) ? k.substring(0, k.length() - 1) : k)
                .map(key -> createUri(tenantId, key))
                .toList();
        } catch (AwsServiceException exception) {
            throw new IOException(exception);
        }
    }

    private String getPath(String tenantId, URI uri) {
        if (uri == null) {
            uri = URI.create("/");
        }

        parentTraversalGuard(uri);
        String path = uri.getPath();
        if (!path.startsWith("/")) {
            path = "/" + path;
        }

        if (tenantId == null) {
            return path;
        }
        return "/" + tenantId + path;
    }

    // Traversal does not work with s3 but it just return empty objects so throwing is more explicit
    private void parentTraversalGuard(URI uri) {
        if (uri.toString().contains("..")) {
            throw new IllegalArgumentException("File should be accessed with their full path and not using relative '..' path.");
        }
    }

    private static URI createUri(String tenantId, String key) {
        return URI.create("kestra://%s".formatted(key).replace(tenantId + "/", ""));
    }
}
