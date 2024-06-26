package io.kestra.storage.s3;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.storages.FileAttributes;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.storages.StorageObject;
import jakarta.validation.constraints.NotEmpty;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.*;
import software.amazon.awssdk.utils.builder.SdkBuilder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.throwFunction;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Jacksonized
@Getter
@Plugin
@Plugin.Id("s3")
public class S3Storage implements S3Config, StorageInterface {

    @NotEmpty
    private String bucket;
    private String region;
    private String endpoint;
    // Configuration for StaticCredentialsProvider
    private String accessKey;
    private String secretKey;

    // Configuration for AWS STS AssumeRole
    private String stsRoleArn;
    private String stsRoleExternalId;
    private String stsRoleSessionName;
    private String stsEndpointOverride;

    @Builder.Default
    private Duration stsRoleSessionDuration = AWS_MIN_STS_ROLE_SESSION_DURATION;

    @Getter(AccessLevel.PRIVATE)
    private S3Client s3Client;

    @Getter(AccessLevel.PRIVATE)
    private S3AsyncClient s3AsyncClient;

    /**
     * {@inheritDoc}
     **/
    @Override
    public void init() {
        this.s3Client = S3ClientFactory.getS3Client(this);
        this.s3AsyncClient = S3ClientFactory.getAsyncS3Client(this);
    }

    @Override
    public InputStream get(String tenantId, URI uri) throws IOException {
        return this.getWithMetadata(tenantId, uri).inputStream();
    }

    public String createBucket() throws IOException {
        try {
            CreateBucketRequest request = CreateBucketRequest.builder().bucket(this.getBucket()).build();
            s3Client.createBucket(request);
            return this.getBucket();
        } catch (AwsServiceException exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public StorageObject getWithMetadata(String tenantId, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        try (S3TransferManager transferManager = S3TransferManager.builder().s3Client(s3AsyncClient).build()) {
            GetObjectRequest request = GetObjectRequest.builder()
                .bucket(this.getBucket())
                .key(path)
                .build();

            Download<ResponseInputStream<GetObjectResponse>> download = transferManager.download(
                DownloadRequest.builder()
                    .getObjectRequest(request)
                    .responseTransformer(AsyncResponseTransformer.toBlockingInputStream())
                    .build()
            );
            ResponseInputStream<GetObjectResponse> result = download.completionFuture().get().result();
            InputStream resultInputStream = result;

            boolean isEmpty = result.response().contentLength() == 0;
            if (isEmpty) {
                result.close();
                resultInputStream = InputStream.nullInputStream();
            }

            return new StorageObject(result.response().metadata(), resultInputStream);
        }catch (ExecutionException e) {
            if (e.getCause() instanceof S3Exception s3Exception && s3Exception.statusCode() == 404) {
                throw new FileNotFoundException();
            }
            throw new IOException(e);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<URI> allByPrefix(String tenantId, URI prefix, boolean includeDirectories) {
        String path = getPath(tenantId, prefix);
        return keysForPrefix(path, true, includeDirectories)
            .map(key -> URI.create("kestra://" + prefix.getPath() + key.substring(path.length())))
            .toList();
    }

    @Override
    public List<FileAttributes> list(String tenantId, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        String prefix = path.endsWith("/") ? path : path + "/";
        try {
            List<FileAttributes> list = keysForPrefix(prefix, false, true)
                .map(throwFunction(this::getFileAttributes))
                .toList();
            if (list.isEmpty()) {
                // this will throw FileNotFound if there is no directory
                this.getAttributes(tenantId, uri);
            }
            return list;
        } catch (NoSuchKeyException exception) {
            throw new FileNotFoundException();
        } catch (AwsServiceException exception) {
            throw new IOException(exception);
        }
    }

    private Stream<String> keysForPrefix(String prefix, boolean recursive, boolean includeDirectories) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
            .bucket(this.getBucket())
            .prefix(prefix)
            .build();
        List<S3Object> contents = s3Client.listObjectsV2(request).contents();
        return contents.stream()
            .map(S3Object::key)
            .filter(key -> {
                key = key.substring(prefix.length());
                // Remove recursive result and requested dir
                return !key.isEmpty()
                    && !Objects.equals(key, prefix)
                    && !key.equals("/")
                    && (recursive || Path.of(key).getParent() == null)
                    && (includeDirectories || !key.endsWith("/"));
            });
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
                .bucket(this.getBucket())
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
    public URI put(String tenantId, URI uri, StorageObject storageObject) throws IOException {
        try {
            String path = getPath(tenantId, uri);
            mkdirs(path);
            PutObjectRequest request = PutObjectRequest.builder()
                .bucket(this.getBucket())
                .key(path)
                .metadata(storageObject.metadata())
                .build();

            Optional<Upload> upload;
            try (
                InputStream data = storageObject.inputStream();
                S3TransferManager transferManager = S3TransferManager.builder().s3Client(s3AsyncClient).build()
            ) {
                UploadRequest.Builder uploadRequest = UploadRequest.builder()
                    .putObjectRequest(request)
                    .requestBody(AsyncRequestBody.fromInputStream(
                        data,
                        // If available bytes are equals to Integer.MAX_VALUE, then available bytes may be more than Integer.MAX_VALUE.
                        // We set to null in this case, otherwise we would be limited to 2GB.
                        data.available() == Integer.MAX_VALUE ? null : (long) data.available(),
                        Executors.newSingleThreadExecutor()
                    ));

                upload = Optional.of(transferManager.upload(uploadRequest.build()));
            }

            upload.orElseThrow(IOException::new).completionFuture().get();
            return createUri(tenantId, uri.getPath());
        } catch (AwsServiceException exception) {
            throw new IOException(exception);
        } catch (ExecutionException | InterruptedException exception) {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public boolean delete(String tenantId, URI uri) throws IOException {
        FileAttributes fileAttributes;
        try {
            fileAttributes = getAttributes(tenantId, uri);
        } catch (FileNotFoundException e) {
            return false;
        }
        if (fileAttributes.getType() == FileAttributes.FileType.Directory) {
            deleteByPrefix(tenantId, uri.getPath().endsWith("/") ? uri : URI.create(uri + "/"));
        }

        return deleteSingleObject(getPath(tenantId, uri));
    }

    private boolean deleteSingleObject(String path) {
        DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
            .bucket(this.getBucket())
            .key(path)
            .build();

        return s3Client.deleteObject(deleteRequest).sdkHttpResponse().isSuccessful();
    }

    @Override
    public URI createDirectory(String tenantId, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        if (!StringUtils.endsWith(path, "/")) {
            path += "/";
        }
        mkdirs(path);
        PutObjectRequest putRequest = PutObjectRequest.builder()
            .bucket(this.getBucket())
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
                    .bucket(this.getBucket())
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
                    .bucket(this.getBucket())
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
            .sourceBucket(this.getBucket())
            .sourceKey(oldKey)
            .destinationBucket(this.getBucket())
            .destinationKey(newKey)
            .build();
        s3Client.copyObject(copyRequest);

        deleteSingleObject(oldKey);
    }

    @Override
    public List<URI> deleteByPrefix(String tenantId, URI storagePrefix) throws IOException {
        ListObjectsRequest listRequest = ListObjectsRequest.builder()
            .bucket(this.getBucket())
            .prefix(getPath(tenantId, storagePrefix))
            .build();
        ListObjectsResponse objectListing = s3Client.listObjects(listRequest);

        @SuppressWarnings("unchecked")
        List<S3Object> s3Objects = objectListing.getValueForField("Contents", List.class).get();

        List<ObjectIdentifier> keys = s3Objects.stream()
            .map(S3Object::key)
            .map(ObjectIdentifier.builder()::key)
            .map(SdkBuilder::build)
            .toList();

        DeleteObjectsRequest deleteRequest = DeleteObjectsRequest.builder()
            .bucket(this.getBucket())
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
