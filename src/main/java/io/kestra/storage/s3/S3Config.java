package io.kestra.storage.s3;

import io.micronaut.context.annotation.ConfigurationProperties;
import jakarta.inject.Singleton;
import lombok.Data;
import lombok.Getter;

@Singleton
@Data
@ConfigurationProperties("kestra.storage.s3")
public class S3Config {
	String bucket;

	String region;

	String endpoint;

	String accessKey;

	String secretKey;
}
