package io.kestra.storage.s3.operation;

import com.amazonaws.AmazonClientException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import io.kestra.storage.s3.config.S3Config;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

@Singleton

@RequiredArgsConstructor
public class S3Storage implements IS3Storage {

	private final AmazonS3 amazonS3;
	private final S3Config s3Config;

	@Override
	public String createBucket(String bucketName) throws IOException {
		boolean bucketExistV2 = amazonS3.doesBucketExistV2(bucketName);

		if (bucketExistV2) {
			throw new IOException("Bucket already exists");
		}
		CreateBucketRequest request = new CreateBucketRequest(bucketName);

		return amazonS3.createBucket(request).getName();
	}

	@Override
	public InputStream download(String key) throws IOException {
		try {
			S3Object object = amazonS3.getObject(s3Config.getBucket(), key);
			return object.getObjectContent();
		} catch (AmazonClientException exception) {
			throw new IOException(exception);
		}
	}

	@Override
	public boolean exists(String key) throws IOException {
		try {
			return amazonS3.doesObjectExist(s3Config.getBucket(), key);
		} catch (AmazonClientException exception) {
			throw new IOException(exception);
		}
	}

	@Override
	public URI upload(String key, InputStream data) throws IOException {
		try {
			PutObjectResult result = amazonS3.putObject(s3Config.getBucket(), key, data, new ObjectMetadata());

			return amazonS3.getUrl(s3Config.getBucket(), key).toURI();
		} catch (AmazonClientException | URISyntaxException exception) {
			throw new IOException(exception);
		}
	}

	@Override
	public void delete(String key) throws IOException {
		try {
			amazonS3.deleteObject(s3Config.getBucket(), key);
		} catch (AmazonClientException exception) {
			throw new IOException(exception);
		}
	}

	@Override
	public boolean delete(List<String> keys) throws IOException {
		try {
			DeleteObjectsRequest request = new DeleteObjectsRequest(s3Config.getBucket())
					.withKeys(keys.toArray(String[]::new));

			DeleteObjectsResult result = amazonS3.deleteObjects(request);
			return result.isRequesterCharged();
		} catch (AmazonClientException exception) {
			throw new IOException(exception);
		}
	}

}
