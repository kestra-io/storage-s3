package io.kestra.storage.s3;

import io.micronaut.context.annotation.ConfigurationProperties;
import jakarta.inject.Singleton;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Singleton
@Getter
@Builder
@ConfigurationProperties("kestra.storage.s3")
public class S3Config {
	private String bucket;

	private String region;

	private String endpoint;

	private String accessKey;

	private String secretKey;

}
