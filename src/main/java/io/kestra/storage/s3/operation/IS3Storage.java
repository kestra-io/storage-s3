package io.kestra.storage.s3.operation;

import io.micronaut.core.annotation.Introspected;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

@Introspected
public interface IS3Storage {

	String createBucket(String bucketName) throws IOException;

	InputStream download(String key) throws IOException;

	boolean exists(String key) throws IOException;

	URI upload(String key, InputStream data) throws IOException;

	void delete(String key) throws IOException;

	boolean delete(List<String> keys) throws IOException;
}
