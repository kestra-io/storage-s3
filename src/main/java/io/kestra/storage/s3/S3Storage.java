package io.kestra.storage.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.core.annotation.Introspected;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

@Singleton
@Introspected
@S3StorageEnabled
@RequiredArgsConstructor
public class S3Storage implements StorageInterface {
	private final AmazonS3 amazonS3;
	private final S3Config s3Config;

	public String createBucket(String bucketName) throws IOException {
		try {
			return amazonS3.createBucket(bucketName).getName();
		} catch (AmazonServiceException exception) {
			throw new IOException(exception);
		}
	}

	@Override
	public InputStream get(URI uri) throws IOException {
		try {
			if (!amazonS3.doesObjectExist(s3Config.getBucket(), uri.getPath())) {
				throw new FileNotFoundException();
			}

			return amazonS3.getObject(s3Config.getBucket(), uri.getPath()).getObjectContent();
		} catch (AmazonServiceException exception) {
			throw new IOException(exception);
		}
	}

	@Override
	public Long size(URI uri) throws IOException {
		try {
			if (!amazonS3.doesObjectExist(s3Config.getBucket(), uri.getPath())) {
				throw new FileNotFoundException();
			}

			return amazonS3.getObjectMetadata(s3Config.getBucket(), uri.getPath()).getContentLength();
		} catch (AmazonServiceException exception) {
			throw new IOException(exception);
		}
	}

	@Override
	public Long lastModifiedTime(URI uri) throws IOException {
		try {
			return amazonS3.getObjectMetadata(s3Config.getBucket(), uri.getPath()).getLastModified().getTime();
		} catch (AmazonServiceException exception) {
			throw new IOException(exception);
		}
	}

	@Override
	public URI put(URI uri, InputStream data) throws IOException {
		try {
			PutObjectResult result = amazonS3.putObject(s3Config.getBucket(), uri.getPath(), data, new ObjectMetadata());
			return createUri(uri.getPath());
		} catch (AmazonServiceException exception) {
			throw new IOException(exception);
		}
	}

	@Override
	public boolean delete(URI uri) throws IOException {
		try {
			if (!amazonS3.doesObjectExist(s3Config.getBucket(), uri.getPath())) {
				return false;
			}
			amazonS3.deleteObject(s3Config.getBucket(), uri.getPath());
			return true;
		} catch (AmazonServiceException exception) {
			throw new IOException(exception);
		} catch (SdkClientException exception) {
			throw new IOException(exception);
		}
	}

	@Override
	public List<URI> deleteByPrefix(URI storagePrefix) throws IOException {
		ObjectListing objectListing = amazonS3.listObjects(s3Config.getBucket(), storagePrefix.getPath());
		List<String> keys = objectListing.getObjectSummaries().stream().map(S3ObjectSummary::getKey).toList();

		DeleteObjectsRequest request = new DeleteObjectsRequest(s3Config.getBucket());
		request.withKeys(keys.toArray(String[]::new));

		if (keys.isEmpty()) {
			return new ArrayList<>();
		}

		try {
			DeleteObjectsResult result = amazonS3.deleteObjects(request);

			return result.getDeletedObjects().stream()
					.map(DeleteObjectsResult.DeletedObject::getKey)
					.map(S3Storage::createUri)
					.toList();
		} catch (AmazonServiceException exception) {
			throw new IOException(exception);
		}
	}

	private static URI createUri(String key) {
		return URI.create("kestra://%s".formatted(key));
	}
}
