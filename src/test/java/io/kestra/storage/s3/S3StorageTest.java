package io.kestra.storage.s3;

import io.kestra.core.storage.StorageTestSuite;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.ResponseInputStream;

import java.io.*;
import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class S3StorageTest extends StorageTestSuite {
    StorageInterface storageInterface;

    private static LocalStackContainer localstack;

    @BeforeAll
    void startLocalstack() throws IOException {
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8.1"))
            .withServices(S3);
        // some tests use a real flow with hardcoded configuration, so we have to fix the binding port
        localstack.setPortBindings(java.util.List.of("4566:4566"));
        localstack.start();

        storageInterface = S3Storage.builder()
            .accessKey(localstack.getAccessKey())
            .secretKey(localstack.getSecretKey())
            .bucket("kestra-unit-test")
            .region(localstack.getRegion())
            .endpoint(localstack.getEndpoint().toString())
            .build();
        storageInterface.init();
    }

    @AfterAll
    void stopLocalstack() {
        if (localstack != null) {
            localstack.stop();
        }
    }

    @BeforeEach
    void createBucket() {
        try {
            ((S3Storage) storageInterface).createBucket();
        } catch (IOException ignored) {
        }
    }

    /**
     * This test ensures that in case of an empty file, we return a raw empty input stream and not a ResponseInputStream.
     * This is needed as the ResponseInputStream in case of an empty file still returns some data which prevents the checksum to work and leads to a stream reading failure.
     */
    @Test
    void getEmptyFile() throws IOException {
        String prefix = IdUtils.create();
        URI uri = URI.create("/" + prefix + "/empty.txt");
        storageInterface.put(TenantService.MAIN_TENANT, null, uri, new ByteArrayInputStream(new byte[0]));

        InputStream inputStream = storageInterface.get(TenantService.MAIN_TENANT, null, uri);
        assertThat(inputStream, not(instanceOf(ResponseInputStream.class)));
        assertThat(new BufferedReader(new InputStreamReader(inputStream)).lines().count(), is(0L));
    }
}
