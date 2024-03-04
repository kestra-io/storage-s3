package io.kestra.storage.s3;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.bind.annotation.Bindable;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;

import java.time.Duration;

/**
 * The AWS S3 configuration.
 *
 * @param bucket                 The S3 bucket name.
 * @param region                 The S3 region.
 * @param endpoint               The S3 endpoint.
 * @param accessKey              The AWS Access Key ID
 * @param secretKey              The AWS Secret Key.
 * @param stsRoleArn             The AWS STS Role.
 * @param stsRoleExternalId      The AWS STS External ID.
 * @param stsRoleSessionName     The AWS STS Session name.
 * @param stsRoleSessionDuration The AWS STS Session duration.
 * @param stsEndpointOverride    The AWS STS Endpoint.
 */
@Singleton
@S3StorageEnabled
@ConfigurationProperties("kestra.storage.s3")
public record S3Config(
    String bucket,

    String region,

    @Nullable String endpoint,

    @Nullable String accessKey,

    @Nullable String secretKey,
    @Nullable String stsRoleArn,
    @Nullable String stsRoleExternalId,
    @Nullable String stsRoleSessionName,
    @Nullable String stsEndpointOverride,

    @Bindable(defaultValue = "15m")
    Duration stsRoleSessionDuration
) {

}
