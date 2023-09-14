package io.kestra.storage.s3;

import com.amazonaws.auth.*;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.annotation.Bean;
import org.testcontainers.containers.localstack.LocalStackContainer;

public class LocalS3ClientConfig {
	private final LocalStackContainer localstack;

	private final S3Config properties;

	public LocalS3ClientConfig(LocalStackContainer localstack) {
		this.localstack = localstack;
		this.properties = S3Config
            .builder()
            .bucket(IdUtils.create().toLowerCase())
            .build();
	}

	@Bean
	public AmazonS3 getAmazonS3() {
		AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard();

		if (localstack.getEndpoint() != null && localstack.getRegion() != null) {
			AwsClientBuilder.EndpointConfiguration endpointConfiguration =
					getAWSEndpointConfiguration(localstack.getEndpoint().toString(), localstack.getRegion());

			clientBuilder.withEndpointConfiguration(endpointConfiguration);
		}

		return clientBuilder.withCredentials(getCredentials()).build();
	}

	public S3Config getConfig() {
		return properties;
	}

	private AwsClientBuilder.EndpointConfiguration getAWSEndpointConfiguration(String awsEndpoint, String awsRegion) {
		return new AwsClientBuilder.EndpointConfiguration(awsEndpoint, awsRegion);
	}

	private AWSCredentialsProvider getCredentials() {
		if (localstack.getAccessKey() != null && localstack.getSecretKey() != null) {
			AWSCredentials credentials = new BasicAWSCredentials(localstack.getAccessKey(), localstack.getSecretKey());
			return new AWSStaticCredentialsProvider(credentials);
		}

		return new DefaultAWSCredentialsProviderChain();
	}
}
