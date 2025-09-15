package io.kestra.storage.s3;

import com.google.common.annotations.VisibleForTesting;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import software.amazon.awssdk.transfer.s3.model.Download;
import software.amazon.awssdk.transfer.s3.model.DownloadRequest;
import software.amazon.awssdk.transfer.s3.model.Upload;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;
import software.amazon.awssdk.utils.builder.SdkBuilder;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
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
    private static final Logger LOG = LoggerFactory.getLogger(S3Storage.class);
    private static final Pattern METADATA_KEY_WORD_SEPARATOR = Pattern.compile("_([a-z])");
    private static final Pattern UPPERCASE = Pattern.compile("([A-Z])");

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

    private boolean forcePathStyle;

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
    public boolean exists(String tenantId, @Nullable String namespace, URI uri) {
        String path = getPath(tenantId, uri);
        return exists(path);
    }

    private boolean exists(String path) {
        try {
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(this.getBucket())
                .key(path)
                .build();
            s3Client.headObject(headObjectRequest);
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    @Override
    public InputStream get(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        return this.getWithMetadata(tenantId, namespace, uri).inputStream();
    }

    @VisibleForTesting
    void createBucket() throws IOException {
        try {
            CreateBucketRequest request = CreateBucketRequest.builder().bucket(this.getBucket()).build();
            s3Client.createBucket(request);
        } catch (AwsServiceException exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public StorageObject getWithMetadata(String tenantId, @Nullable String namespace, URI uri) throws IOException {
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

            return new StorageObject(MetadataUtils.toRetrievedMetadata(result.response().metadata()), resultInputStream);
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
    public List<URI> allByPrefix(String tenantId, @Nullable String namespace, URI prefix, boolean includeDirectories) {
        String path = getPath(tenantId, prefix);
        return keysForPrefix(path, true, includeDirectories)
            .map(key -> URI.create("kestra://" + prefix.getPath() + key.substring(path.length())))
            .toList();
    }

    @Override
    public List<FileAttributes> list(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        String prefix = path.endsWith("/") ? path : path + "/";
        try {
            List<FileAttributes> list = keysForPrefix(prefix, false, true)
                .map(throwFunction(this::getFileAttributes))
                .toList();
            if (list.isEmpty()) {
                // this will throw FileNotFound if there is no directory
                this.getAttributes(tenantId, namespace, uri);
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
    public FileAttributes getAttributes(String tenantId, @Nullable String namespace, URI uri) throws IOException {
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
    public URI put(String tenantId, @Nullable String namespace, URI uri, StorageObject storageObject) throws IOException {
        try (
            InputStream data = storageObject.inputStream();
            S3TransferManager transferManager = S3TransferManager.builder().s3Client(s3AsyncClient).build()
        ) {
            String path = getPath(tenantId, uri);
            mkdirs(path);
            PutObjectRequest request = PutObjectRequest.builder()
                .bucket(this.getBucket())
                .key(path)
                .metadata(MetadataUtils.toStoredMetadata(storageObject.metadata()))
                .build();

            Optional<Upload> upload;

            Long length = (long) data.available();
            if (data instanceof ResponseInputStream<?> responseInputStream && responseInputStream.response() instanceof GetObjectResponse getObjectResponse) {
                length = getObjectResponse.contentLength();
            }
            if (length == Integer.MAX_VALUE) {
                length = null;
            }

            UploadRequest.Builder uploadRequest = UploadRequest.builder()
                .putObjectRequest(request)
                .requestBody(AsyncRequestBody.fromInputStream(
                    data,
                    // If available bytes are equals to Integer.MAX_VALUE, then available bytes may be more than Integer.MAX_VALUE.
                    // We set to null in this case, otherwise we would be limited to 2GB.
                    length,
                    Executors.newSingleThreadExecutor()
                ));

            upload = Optional.of(transferManager.upload(uploadRequest.build()));
            upload.orElseThrow(IOException::new).completionFuture().get();

            return createUri(uri.getPath());
        } catch (AwsServiceException exception) {
            throw new IOException(exception);
        } catch (ExecutionException | InterruptedException exception) {
            throw new RuntimeException(exception);
        }
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

    private boolean deleteSingleObject(String path) {
        DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
            .bucket(this.getBucket())
            .key(path)
            .build();

        return s3Client.deleteObject(deleteRequest).sdkHttpResponse().isSuccessful();
    }

    @Override
    public URI createDirectory(String tenantId, @Nullable String namespace, URI uri) throws IOException {
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
        return createUri(uri.getPath());
    }

    private void mkdirs(String path) throws IOException {
        if (!path.endsWith("/")) {
            path = path.substring(0, path.lastIndexOf("/") + 1);
        }

        // check if it exists before creating it
        if (exists(path)) {
            return;
        }

        String[] directories = path.split("/");
        StringBuilder aggregatedPath = new StringBuilder();
        try {
            // perform 1 put request per parent directory in the path
            for (String directory : directories) {
                aggregatedPath.append(directory).append("/");
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
    public URI move(String tenantId, @Nullable String namespace, URI from, URI to) throws IOException {
        String source = getPath(tenantId, from);
        String dest = getPath(tenantId, to);
        try {
            FileAttributes attributes = getAttributes(tenantId, namespace, from);
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

            return createUri(to.getPath());
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
    public List<URI> deleteByPrefix(String tenantId, @Nullable String namespace, URI storagePrefix) throws IOException {
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
                .map(k -> createUri(removeTenant(tenantId, k)))
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

    private static String removeTenant(String tenantId, String k) {
        String result = tenantId == null ? "/" + k : k.replaceFirst(tenantId, "");
        return result.startsWith("//") ? result.substring(1) : result;
    }

    private static URI createUri(String key) {
        return URI.create("kestra://%s".formatted(key));
    }

    public void close() {
        if (this.s3Client != null) {
            try {
                this.s3Client.close();
            } catch (Exception e) {
                LOG.warn("Failed to close S3Storage", e);
            }
        }

        if (this.s3AsyncClient != null) {
            try {
                this.s3AsyncClient.close();
            } catch (Exception e) {
                LOG.warn("Failed to close S3Storage", e);
            }
        }
    }
}
