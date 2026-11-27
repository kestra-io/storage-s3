package io.kestra.storage.s3;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import io.kestra.core.tenant.TenantService;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Some S3-compatible servers answer a ranged GET on an empty object with a 416, as the CRT client sends when it reads
 * the first part without a preceding HEAD. A test server can't produce it on demand, hence stubbed clients.
 */
class S3StorageEmptyObjectRangeTest {
    private static final String MAIN = TenantService.MAIN_TENANT;

    private static S3Storage storage(int getStatus, Long headLength) {
        var asyncClient = (S3AsyncClient) Proxy.newProxyInstance(
            S3StorageEmptyObjectRangeTest.class.getClassLoader(), new Class<?>[] { S3AsyncClient.class }, (proxy, method, args) ->
            {
                return switch (method.getName()) {
                    case "getObject" -> CompletableFuture.failedFuture(
                        S3Exception.builder().statusCode(getStatus).message("The requested range is not satisfiable").build()
                    );
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                };
            }
        );
        var client = (S3Client) Proxy.newProxyInstance(
            S3StorageEmptyObjectRangeTest.class.getClassLoader(), new Class<?>[] { S3Client.class }, (proxy, method, args) ->
            {
                return switch (method.getName()) {
                    case "headObject" -> {
                        if (headLength == null) {
                            throw NoSuchKeyException.builder().statusCode(404).build();
                        }
                        yield HeadObjectResponse.builder().contentLength(headLength).metadata(Map.of("my_key", "value")).build();
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                };
            }
        );
        return S3Storage.builder().bucket("bucket").s3Client(client).s3AsyncClient(asyncClient).build();
    }

    @Test
    void rangeNotSatisfiableOnEmptyObjectReturnsEmptyFile() throws IOException {
        var object = storage(416, 0L).getWithMetadata(MAIN, null, URI.create("/a/empty.txt"));

        try (InputStream in = object.inputStream()) {
            assertThat(in.readAllBytes().length, is(0));
        }
        assertThat(object.metadata(), hasEntry("myKey", "value"));
    }

    @Test
    void rangeNotSatisfiableOnNonEmptyObjectIsThrown() {
        assertThrows(IOException.class, () -> storage(416, 5L).get(MAIN, null, URI.create("/a/file.txt")));
    }

    @Test
    void rangeNotSatisfiableOnMissingObjectIsNotFound() {
        assertThrows(FileNotFoundException.class, () -> storage(416, null).get(MAIN, null, URI.create("/a/missing.txt")));
    }
}
