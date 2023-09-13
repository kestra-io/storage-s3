package io.kestra.storage.s3.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.Setter;

@Singleton
@Getter
@ConfigurationProperties("kestra.storage.s3")
public class S3Properties {

	@Setter
	private String bucket;

	private String region;

	private String endpoint;

	private String accessKey;

	private String secretKey;

}
