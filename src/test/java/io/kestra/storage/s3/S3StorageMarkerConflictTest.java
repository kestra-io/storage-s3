package io.kestra.storage.s3;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import io.kestra.core.tenant.TenantService;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Concurrent writes of the same folder marker are rejected by some S3-compatible servers with a 409, which can't be
 * produced on demand by a test server, hence a stubbed client.
 */
class S3StorageMarkerConflictTest {
    private static final String MAIN = TenantService.MAIN_TENANT;

    private static S3Storage storage(Set<String> rejectedMarkers, int status, List<String> written) {
        var client = (S3Client) Proxy.newProxyInstance(
            S3StorageMarkerConflictTest.class.getClassLoader(), new Class<?>[] { S3Client.class }, (proxy, method, args) ->
            {
                return switch (method.getName()) {
                    case "headObject" -> throw NoSuchKeyException.builder().statusCode(404).build();
                    case "putObject" -> {
                        var key = ((PutObjectRequest) args[0]).key();
                        if (rejectedMarkers.contains(key)) {
                            throw S3Exception.builder().statusCode(status).message("rejected").build();
                        }
                        written.add(key);
                        yield PutObjectResponse.builder().build();
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                };
            }
        );
        return S3Storage.builder().bucket("bucket").s3Client(client).build();
    }

    @Test
    void conflictOnParentMarkerIsIgnored() throws IOException {
        var written = new CopyOnWriteArrayList<String>();
        var storage = storage(Set.of(MAIN + "/", MAIN + "/a/"), 409, written);

        storage.createDirectory(MAIN, null, URI.create("/a/b"));

        assertThat(written, hasItem(MAIN + "/a/b/"));
    }

    @Test
    void otherErrorOnParentMarkerIsThrown() {
        var storage = storage(Set.of(MAIN + "/"), 403, new CopyOnWriteArrayList<>());

        assertThrows(IOException.class, () -> storage.createDirectory(MAIN, null, URI.create("/a/b")));
    }
}
