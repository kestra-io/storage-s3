package io.kestra.storage.s3;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;

import software.amazon.awssdk.crt.CRT;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class S3StorageCrtLifecycleTest extends AbstractS3StorageTest {
    private static final String BUCKET = "kestra-unit-test";
    private static final String PATH = "kestra";

    private static LocalStackContainer localstack;

    @BeforeAll
    static void startLocalstack() {
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8.1"))
            .withServices(S3);
        localstack.setPortBindings(java.util.List.of("4566:4566"));
        localstack.start();
    }

    @AfterAll
    static void stopLocalstack() {
        if (localstack != null) {
            localstack.stop();
        }
    }

    @Override
    S3Storage.S3StorageBuilder storageBuilder() {
        return S3Storage.builder()
            .accessKey(localstack.getAccessKey())
            .secretKey(localstack.getSecretKey())
            .bucket(BUCKET)
            .region(localstack.getRegion())
            .endpoint(localstack.getEndpoint().toString());
    }

    @Test
    void initIsIdempotentAndCloseCanBeRepeated() throws IOException {
        var storage = S3Storage.builder()
            .accessKey(localstack.getAccessKey())
            .secretKey(localstack.getSecretKey())
            .bucket(BUCKET)
            .region(localstack.getRegion())
            .endpoint(localstack.getEndpoint().toString())
            .path(PATH)
            .build();

        try {
            storage.init();
            AbstractS3StorageTest.createBucketIfMissing(storage);
            var firstClient = storage.getS3ClientForTest();

            storage.init();
            assertThat(storage.getS3ClientForTest(), sameInstance(firstClient));

            // Simulate JVM shutdown hook releasing its reference
            CRT.releaseShutdownRef();

            URI uri = URI.create("/" + IdUtils.create() + "/crt-lifecycle.txt");
            byte[] expected = "crt shutdown ref released".getBytes(StandardCharsets.UTF_8);
            storage.put(TenantService.MAIN_TENANT, null, uri, new ByteArrayInputStream(expected));

            try (InputStream input = storage.get(TenantService.MAIN_TENANT, null, uri)) {
                assertThat(input.readAllBytes(), equalTo(expected));
            }
        } finally {
            CRT.acquireShutdownRef();
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

        var storage = storageBuilder()
            .path(PATH)
            .build();
        try {
            storage.init();
            assertPutAndGetWorks(storage);
        } finally {
            storage.close();
        }
    }

    private void assertPutAndGetWorks(S3Storage storage) throws IOException {
        URI uri = URI.create("/" + IdUtils.create() + "/lifecycle.txt");
        byte[] expected = "storage lifecycle round trip".getBytes(StandardCharsets.UTF_8);
        storage.put(TenantService.MAIN_TENANT, null, uri, new ByteArrayInputStream(expected));

        try (InputStream input = storage.get(TenantService.MAIN_TENANT, null, uri)) {
            assertThat(input.readAllBytes(), equalTo(expected));
        }
    }
}
