package io.kestra.storage.s3;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;

public interface S3Config {

    Duration AWS_MIN_STS_ROLE_SESSION_DURATION = Duration.ofSeconds(900);

    @Schema(
        title = "The S3 bucket where to store internal objects."
    )
    @PluginProperty
    String getBucket();

    @Schema(
        title = "AWS region with which the SDK should communicate."
    )
    @PluginProperty
    String getRegion();

    @PluginProperty
    String getEndpoint();

    @Schema(
        title = "Access Key Id in order to connect to AWS.",
        description = "If no connection is defined, we will use the `DefaultCredentialsProvider` to fetch the value."
    )
    @PluginProperty
    String getAccessKey();

    @Schema(
        title = "Secret Key Id in order to connect to AWS.",
        description = "If no connection is defined, we will use the `DefaultCredentialsProvider` to fetch the value."
    )
    @PluginProperty
    String getSecretKey();

    @Schema(
        title = "AWS STS Role.",
        description = "The Amazon Resource Name (ARN) of the role to assume. If set the task will use the `StsAssumeRoleCredentialsProvider`. Otherwise, the `StaticCredentialsProvider` will be used with the provided Access Key Id and Secret Key."
    )
    @PluginProperty
    String getStsRoleArn();

    @Schema(
        title = "AWS STS External Id.",
        description = " A unique identifier that might be required when you assume a role in another account. This property is only used when an `stsRoleArn` is defined."
    )
    @PluginProperty
    String getStsRoleExternalId();

    @Schema(
        title = "AWS STS Session name. This property is only used when an `stsRoleArn` is defined."
    )
    @PluginProperty
    String getStsRoleSessionName();

    @Schema(
        title = "The AWS STS endpoint with which the SDKClient should communicate."
    )
    @PluginProperty
    String getStsEndpointOverride();

    @Schema(
        title = "AWS STS Session duration.",
        description = "The duration of the role session (default: 15 minutes, i.e., PT15M). This property is only used when an `stsRoleArn` is defined."
    )
    @PluginProperty
    java.time.Duration getStsRoleSessionDuration();

    boolean isForcePathStyle();
}
