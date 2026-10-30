package io.kestra.storage.s3;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URI;

import org.junit.jupiter.api.Test;

import io.kestra.core.tenant.TenantService;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Object;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * DeleteObjects reports per-key failures with a 200 status, which a real server can't be asked to produce on demand,
 * hence a stubbed client.
 */
class S3StorageDeleteErrorsTest {
    @Test
    void deleteByPrefixFailsOnPerKeyDeleteErrors() {
        var client = (S3Client) Proxy.newProxyInstance(
            getClass().getClassLoader(), new Class<?>[] { S3Client.class }, (proxy, method, args) ->
            {
                return switch (method.getName()) {
                    case "listObjectsV2" -> ListObjectsV2Response.builder()
                        .contents(S3Object.builder().key("tenant/a.txt").build(), S3Object.builder().key("tenant/b.txt").build())
                        .isTruncated(false)
                        .build();
                    case "deleteObjects" -> DeleteObjectsResponse.builder()
                        .errors(S3Error.builder().key("tenant/b.txt").code("AccessDenied").build())
                        .build();
                    default -> throw new UnsupportedOperationException(method.getName());
                };
            }
        );
        var storage = S3Storage.builder().bucket("bucket").s3Client(client).build();

        var exception = assertThrows(
            IOException.class,
            () -> storage.deleteByPrefix(TenantService.MAIN_TENANT, null, URI.create("/tenant/"))
        );

        assertThat(exception.getMessage(), containsString("tenant/b.txt (AccessDenied)"));
    }
}
