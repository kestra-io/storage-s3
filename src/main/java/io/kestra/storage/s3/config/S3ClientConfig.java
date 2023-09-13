package io.kestra.storage.s3.config;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.micronaut.context.annotation.Bean;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;

@Singleton
@RequiredArgsConstructor
public class S3ClientConfig {

	private final S3Config s3Config;

	@Bean
	public AmazonS3 getAmazonS3() {
		AwsClientBuilder.EndpointConfiguration endpointConfiguration =
				getAWSEndpointConfiguration(s3Config.getEndpoint(), s3Config.getRegion());

		return AmazonS3ClientBuilder.standard()
				.withEndpointConfiguration(endpointConfiguration)
				.withCredentials(getCredentials())
				.build();
	}

	private AwsClientBuilder.EndpointConfiguration getAWSEndpointConfiguration(String awsEndpoint, String awsRegion) {
		return new AwsClientBuilder.EndpointConfiguration(awsEndpoint, awsRegion);
	}

	private AWSCredentialsProvider getCredentials() {
		return new DefaultAWSCredentialsProviderChain();
	}

}
