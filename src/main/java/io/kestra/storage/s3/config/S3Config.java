package io.kestra.storage.s3.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import jakarta.inject.Singleton;
import lombok.Getter;

@Singleton
@Getter
@ConfigurationProperties("kestra.storage.s3")
public class S3Config {

	private String bucket;

	private String region;

	private String endpoint;

}
