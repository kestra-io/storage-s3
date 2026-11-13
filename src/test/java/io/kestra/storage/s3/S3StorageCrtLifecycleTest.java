package io.kestra.storage.s3;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;

import software.amazon.awssdk.crt.CRT;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

class S3StorageCrtLifecycleTest {
    private static final String BUCKET = "kestra-crt-lifecycle";

    private static LocalStackContainer localstack;

    @BeforeAll
    static void startLocalstack() {
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8.1")).withServices(S3);
        localstack.start();
    }

    @AfterAll
    static void stopLocalstack() {
        if (localstack != null) {
            localstack.stop();
        }
    }

    @Test
    void putStillWorksAfterTheShutdownHookReleasesCrt() throws IOException {
        var storage = storage();
        try {
            storage.init();
            AbstractS3StorageTest.createBucketIfMissing(storage);

            // On SIGTERM the JVM shutdown hook releases its CRT reference, which used to hang every later put.
            CRT.releaseShutdownRef();
            try {
                assertTimeoutPreemptively(Duration.ofSeconds(30), () -> assertPutAndGetWorks(storage));
            } finally {
                CRT.acquireShutdownRef();
            }
        } finally {
            storage.close();
        }
    }

    @Test
    void initIsIdempotentAndCloseCanBeRepeated() throws IOException {
        var storage = storage();
        try {
            storage.init();
            var firstClient = storage.getS3ClientForTest();

            storage.init();

            assertThat(storage.getS3ClientForTest(), sameInstance(firstClient));
        } finally {
            storage.close();
            storage.close();
        }
    }

    @Test
    void failedInitCanBeClosedRepeatedlyWithoutAffectingOtherStorage() throws IOException {
        var failedStorage = S3Storage.builder()
            .bucket(BUCKET)
            .endpoint("invalid endpoint")
            .build();

        assertThrows(IllegalArgumentException.class, failedStorage::init);
        failedStorage.close();
        failedStorage.close();

        var storage = storage();
        try {
            storage.init();
            AbstractS3StorageTest.createBucketIfMissing(storage);
            assertPutAndGetWorks(storage);
        } finally {
            storage.close();
        }
    }

    private static S3Storage storage() {
        return S3Storage.builder()
            .accessKey(localstack.getAccessKey())
            .secretKey(localstack.getSecretKey())
            .bucket(BUCKET)
            .region(localstack.getRegion())
            .endpoint(localstack.getEndpoint().toString())
            .path("kestra")
            .build();
    }

    private static void assertPutAndGetWorks(S3Storage storage) throws IOException {
        var uri = URI.create("/%s/crt-lifecycle.txt".formatted(IdUtils.create()));
        var expected = "storage lifecycle round trip".getBytes(StandardCharsets.UTF_8);
        storage.put(TenantService.MAIN_TENANT, null, uri, new ByteArrayInputStream(expected));

        try (var input = storage.get(TenantService.MAIN_TENANT, null, uri)) {
            assertThat(input.readAllBytes(), equalTo(expected));
        }
    }
}
