package io.kestra.storage.s3;

import com.amazonaws.auth.*;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;

@Factory
@Singleton
@S3StorageEnabled
@RequiredArgsConstructor
public class S3ClientFactory {
	private final S3Config s3Config;

	@Bean
	public AmazonS3 getAmazonS3() {
		AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard();

		if (s3Config.getEndpoint() != null && s3Config.getRegion() != null) {
			AwsClientBuilder.EndpointConfiguration endpointConfiguration =
					getAWSEndpointConfiguration(s3Config.getEndpoint(), s3Config.getRegion());

			clientBuilder.withEndpointConfiguration(endpointConfiguration);
		}

		return clientBuilder.withCredentials(getCredentials()).build();
	}

	private AwsClientBuilder.EndpointConfiguration getAWSEndpointConfiguration(String awsEndpoint, String awsRegion) {
		return new AwsClientBuilder.EndpointConfiguration(awsEndpoint, awsRegion);
	}

	private AWSCredentialsProvider getCredentials() {
		if (s3Config.getAccessKey() != null && s3Config.getSecretKey() != null) {
			AWSCredentials credentials = new BasicAWSCredentials(s3Config.getAccessKey(), s3Config.getSecretKey());
			return new AWSStaticCredentialsProvider(credentials);
		}

		return new DefaultAWSCredentialsProviderChain();
	}
}
