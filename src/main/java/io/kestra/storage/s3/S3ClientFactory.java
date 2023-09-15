package io.kestra.storage.s3;

import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;

import java.net.URI;

public class S3ClientFactory {
	public static S3Client getS3Client(S3Config s3Config) {
		S3ClientBuilder clientBuilder = S3Client.builder().httpClient(ApacheHttpClient.create());

		if (s3Config.getEndpoint() != null) {
			clientBuilder.endpointOverride(URI.create(s3Config.getEndpoint()));
		}

		if (s3Config.getRegion() != null) {
			clientBuilder.region(Region.of(s3Config.getRegion()));
		}

		return clientBuilder
            .credentialsProvider(getCredentials(s3Config))
            .build();
	}

	public static S3AsyncClient getAsyncS3Client(S3Config s3Config) {
        S3CrtAsyncClientBuilder clientBuilder = S3AsyncClient.crtBuilder();

        if (s3Config.getEndpoint() != null) {
            clientBuilder.endpointOverride(URI.create(s3Config.getEndpoint()));
        }

        if (s3Config.getRegion() != null) {
            clientBuilder.region(Region.of(s3Config.getRegion()));
        }

		return clientBuilder
            .credentialsProvider(getCredentials(s3Config))
            .build();
	}

	private static AwsCredentialsProvider getCredentials(S3Config s3Config) {
		if (s3Config.getAccessKey() != null && s3Config.getSecretKey() != null) {
			AwsCredentials credentials = AwsBasicCredentials.create(s3Config.getAccessKey(), s3Config.getSecretKey());
			return StaticCredentialsProvider.create(credentials);
		}

		return DefaultCredentialsProvider.create();
	}
}
