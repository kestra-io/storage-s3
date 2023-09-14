package io.kestra.storage.s3;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import org.testcontainers.containers.localstack.LocalStackContainer;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;

@Factory
public class S3ClientFactory {
	private final LocalStackContainer localStackContainer;

	public S3ClientFactory(LocalStackContainer localStackContainer) {
		this.localStackContainer = localStackContainer;
	}

	@Bean
	public S3Client getS3Client() {
		S3ClientBuilder clientBuilder = S3Client.builder().httpClient(ApacheHttpClient.create());

		if (localStackContainer.getEndpoint() != null) {
			clientBuilder.endpointOverride(localStackContainer.getEndpoint());
		}

		if (localStackContainer.getRegion() != null) {
			clientBuilder.region(Region.of(localStackContainer.getRegion()));
		}

		return clientBuilder.credentialsProvider(getCredentials()).build();
	}

	@Bean
	public S3AsyncClient getAsyncS3Client() {
		S3AsyncClientBuilder clientBuilder = S3AsyncClient.builder();

		if (localStackContainer.getEndpoint() != null && localStackContainer.getRegion() != null) {
			clientBuilder.endpointOverride(localStackContainer.getEndpoint());
			clientBuilder.region(Region.of(localStackContainer.getRegion()));
		}

		return clientBuilder.credentialsProvider(getCredentials()).build();
	}

	private AwsCredentialsProvider getCredentials() {
		if (localStackContainer.getAccessKey() != null && localStackContainer.getSecretKey() != null) {
			AwsCredentials credentials = AwsBasicCredentials.create(localStackContainer.getAccessKey(), localStackContainer.getSecretKey());
			return StaticCredentialsProvider.create(credentials);
		}

		return DefaultCredentialsProvider.create();
	}
}
