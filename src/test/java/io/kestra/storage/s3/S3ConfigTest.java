package io.kestra.storage.s3;

import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Singleton;

@Singleton
@Replaces(S3Config.class)
public class S3ConfigTest {
	String bucket;

	String region;

	String endpoint;

	String accessKey;

	String secretKey;
}
