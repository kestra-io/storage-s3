package io.kestra.storage.s3;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.core.internal.http.AmazonSyncHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.internal.crt.S3CrtAsyncClient;

import java.net.URI;

@Factory
@Singleton
@S3StorageEnabled
@RequiredArgsConstructor
public class S3ClientFactory {
	private final S3Config s3Config;

	@Bean
	public S3Client getS3Client() {
		S3ClientBuilder clientBuilder = S3Client.builder().httpClient(ApacheHttpClient.create());

		if (s3Config.getEndpoint() != null) {
			clientBuilder.endpointOverride(URI.create(s3Config.getEndpoint()));
		}

		if (s3Config.getRegion() != null) {
			clientBuilder.region(Region.of(s3Config.getRegion()));
		}

		return clientBuilder.credentialsProvider(getCredentials()).build();
	}

	@Bean
	public S3AsyncClient getAsyncS3Client() {
		S3CrtAsyncClientBuilder clientBuilder = S3CrtAsyncClient.builder();

		if (s3Config.getEndpoint() != null && s3Config.getRegion() != null) {
			clientBuilder.endpointOverride(URI.create(s3Config.getEndpoint()));
			clientBuilder.region(Region.of(s3Config.getRegion()));
		}

		return clientBuilder.credentialsProvider(getCredentials()).build();
	}

	private AwsCredentialsProvider getCredentials() {
		if (s3Config.getAccessKey() != null && s3Config.getSecretKey() != null) {
			AwsCredentials credentials = AwsBasicCredentials.create(s3Config.getAccessKey(), s3Config.getSecretKey());
			return StaticCredentialsProvider.create(credentials);
		}

		return DefaultCredentialsProvider.create();
	}
}
