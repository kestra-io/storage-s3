package io.kestra.storage.s3;

import io.kestra.core.storages.StorageInterface;
import io.micronaut.core.annotation.Introspected;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.exception.SdkClientException;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

@Singleton
@Introspected
@S3StorageEnabled
@RequiredArgsConstructor
public class S3Storage implements StorageInterface {

	private final S3Client s3Client;
	private final S3AsyncClient s3AsyncClient;
	private final S3Config s3Config;

	public String createBucket(String bucketName) throws IOException {
		try {
			CreateBucketRequest request = CreateBucketRequest.builder().bucket(bucketName).build();
			s3Client.createBucket(request);
			return bucketName;
		} catch (BucketAlreadyExistsException exception) {
			throw new IOException(exception);
		} catch (AwsServiceException exception) {
			throw new IOException(exception);
		}
	}

	@Override
	public InputStream get(URI uri) throws IOException {
		try {
			GetObjectRequest request = GetObjectRequest.builder()
					.bucket(s3Config.getBucket())
					.key(uri.getPath())
					.build();
			return s3Client.getObject(request);
		} catch (NoSuchKeyException exception) {
			throw new FileNotFoundException();
		} catch (AwsServiceException exception) {
			throw new IOException(exception);
		}
	}

	@Override
	public Long size(URI uri) throws IOException {
		try {
			HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
					.bucket(s3Config.getBucket())
					.key(uri.getPath())
					.build();
			return s3Client.headObject(headObjectRequest).contentLength();
		} catch (NoSuchKeyException exception) {
			throw new FileNotFoundException();
		} catch (AwsServiceException exception) {
			throw new IOException(exception);
		}
	}

	@Override
	public Long lastModifiedTime(URI uri) throws IOException {
		try {
			HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
					.bucket(s3Config.getBucket())
					.key(uri.getPath())
					.build();
			return s3Client.headObject(headObjectRequest).lastModified().getEpochSecond();
		} catch (NoSuchKeyException exception) {
			throw new FileNotFoundException();
		} catch (AwsServiceException exception) {
			throw new IOException(exception);
		}
	}

	@Override
	public URI put(URI uri, InputStream data) throws IOException {
		try {
			int length = data.available();

			PutObjectRequest request = PutObjectRequest.builder()
					.bucket(s3Config.getBucket())
					.key(uri.getPath())
					.build();

			Optional<Upload> upload;
			try (S3TransferManager transferManager = S3TransferManager.builder().s3Client(s3AsyncClient).build()) {
				UploadRequest.Builder uploadRequest = UploadRequest.builder()
						.putObjectRequest(request)
						.requestBody(AsyncRequestBody.fromInputStream(data, (long) length, Executors.newSingleThreadExecutor()));

				upload = Optional.of(transferManager.upload(uploadRequest.build()));
			}

			PutObjectResponse response = upload.orElseThrow(IOException::new).completionFuture().get().response();
			return createUri(uri.getPath());
		} catch (AwsServiceException exception) {
			throw new IOException(exception);
		} catch (ExecutionException | InterruptedException exception) {
			throw new RuntimeException(exception);
		}
	}

	@Override
	public boolean delete(URI uri) throws IOException {
		try {
			try {
				lastModifiedTime(uri);
			} catch (FileNotFoundException exception) {
				return false;
			}

			DeleteObjectRequest request = DeleteObjectRequest.builder()
					.bucket(s3Config.getBucket())
					.key(uri.getPath())
					.build();

			return s3Client.deleteObject(request).sdkHttpResponse().isSuccessful();
		} catch (AwsServiceException exception) {
			throw new IOException(exception);
		} catch (SdkClientException exception) {
			throw new IOException(exception);
		}
	}

	@Override
	public List<URI> deleteByPrefix(URI storagePrefix) throws IOException {
		ListObjectsRequest listRequest = ListObjectsRequest.builder()
				.bucket(s3Config.getBucket())
				.prefix(storagePrefix.getPath())
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
					.map(S3Storage::createUri)
					.toList();
		} catch (AwsServiceException exception) {
			throw new IOException(exception);
		}
	}

	private static URI createUri(String key) {
		return URI.create("kestra://%s".formatted(key));
	}
}
