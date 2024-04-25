package io.kestra.storage.s3;

import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.net.URI;

public final class S3ClientFactory {

    public static S3Client getS3Client(final S3Config s3Config) {
        S3ClientBuilder clientBuilder = S3Client
            .builder()
            // Use the httpClientBuilder to delegate the lifecycle management of the HTTP client to the AWS SDK
            .httpClientBuilder(serviceDefaults -> ApacheHttpClient.builder().build());

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

    public static S3AsyncClient getAsyncS3Client(final S3Config s3Config) {
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

    /**
     * Factory method for constructing a new {@link AwsCredentialsProvider} for the given config. If no specific
     * credential provider can be resolved from the given config, then a new {@link DefaultCredentialsProvider} is returned.
     *
     * @param config The S3Config.
     * @return a new {@link AwsCredentialsProvider}.
     */
    private static AwsCredentialsProvider getCredentials(final S3Config config) {
        // StsAssumeRoleCredentialsProvider
        if (StringUtils.isNotEmpty(config.getStsRoleArn())) {
            return stsAssumeRoleCredentialsProvider(config);
        }

        // StaticCredentialsProvider
        if (StringUtils.isNotEmpty(config.getAccessKey()) &&
            StringUtils.isNotEmpty(config.getSecretKey())) {
            return staticCredentialsProvider(config);
        }

        // Otherwise, use DefaultCredentialsProvider
        return DefaultCredentialsProvider.builder().build();
    }

    /**
     * Factory method for constructing a new {@link StaticCredentialsProvider} for the given config.
     *
     * @param config The S3Config.
     * @return a new {@link StaticCredentialsProvider}.
     */
    private static StaticCredentialsProvider staticCredentialsProvider(final S3Config config) {
        final AwsCredentials credentials = AwsBasicCredentials.create(
            config.getAccessKey(),
            config.getSecretKey()
        );
        return StaticCredentialsProvider.create(credentials);
    }

    /**
     * Factory method for constructing a new {@link StsAssumeRoleCredentialsProvider} for the given config.
     *
     * @param config The S3Config.
     * @return a new {@link StsAssumeRoleCredentialsProvider}.
     */
    private static StsAssumeRoleCredentialsProvider stsAssumeRoleCredentialsProvider(final S3Config config) {
        String roleSessionName = config.getStsRoleSessionName();
        roleSessionName = roleSessionName != null ? roleSessionName : "kestra-storage-s3-" + System.currentTimeMillis();

        final AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
            .roleArn(config.getStsRoleArn())
            .roleSessionName(roleSessionName)
            .durationSeconds((int) config.getStsRoleSessionDuration().toSeconds())
            .externalId(config.getStsRoleExternalId())
            .build();

        return StsAssumeRoleCredentialsProvider.builder()
            .stsClient(stsClient(config))
            .refreshRequest(assumeRoleRequest)
            .build();
    }

    /**
     * Factory method for constructing a new {@link StsClient} for the given config.
     *
     * @param config The S3Config.
     * @return a new {@link StsClient}.
     */
    private static StsClient stsClient(final S3Config config) {
        StsClientBuilder builder = StsClient.builder();

        final String stsEndpointOverride = config.getStsEndpointOverride();
        if (stsEndpointOverride != null) {
            builder.applyMutation(stsClientBuilder ->
                stsClientBuilder.endpointOverride(URI.create(stsEndpointOverride))
            );
        }

        final String regionString = config.getRegion();
        if (regionString != null) {
            builder.applyMutation(stsClientBuilder ->
                stsClientBuilder.region(Region.of(regionString))
            );
        }
        return builder.build();
    }
}
