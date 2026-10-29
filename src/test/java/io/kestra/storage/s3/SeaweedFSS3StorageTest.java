package io.kestra.storage.s3;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.kestra.core.storage.StorageTestSuite;
import io.kestra.core.storages.FileAttributes;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Runs the storage test suite against the S3 gateway of SeaweedFS, which differs from AWS S3 on folder markers.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SeaweedFSS3StorageTest extends StorageTestSuite {
    private static final int S3_PORT = 8333;
    private static final String BUCKET = "kestra-unit-test";

    private static GenericContainer<?> seaweedfs;
    private static S3Storage s3Storage;

    @BeforeAll
    static void startSeaweedFS() throws IOException {
        seaweedfs = new GenericContainer<>(DockerImageName.parse("chrislusf/seaweedfs:4.50"))
            .withCommand(
                "server", "-ip=localhost", "-ip.bind=0.0.0.0", "-dir=/data", "-volume.max=100",
                "-filer", "-s3", "-s3.port=" + S3_PORT
            )
            .withExposedPorts(S3_PORT)
            .waitingFor(Wait.forHttp("/").forPort(S3_PORT).forStatusCode(200).withStartupTimeout(Duration.ofSeconds(120)));
        seaweedfs.start();

        s3Storage = S3Storage.builder()
            .accessKey("accesskey")
            .secretKey("secretkey")
            .bucket(BUCKET)
            .region("us-east-1")
            .endpoint("http://" + seaweedfs.getHost() + ":" + seaweedfs.getMappedPort(S3_PORT))
            .forcePathStyle(true)
            .build();
        s3Storage.init();
        s3Storage.createBucket();
    }

    @AfterAll
    static void stopSeaweedFS() {
        if (s3Storage != null) {
            s3Storage.close();
        }
        if (seaweedfs != null) {
            seaweedfs.stop();
        }
    }

    // application-test.yml points the injected storage to LocalStack, so it is replaced to exercise SeaweedFS
    @BeforeEach
    void useSeaweedFS() {
        storageInterface = s3Storage;
    }

    @Test
    void getFolderIsNotFound() throws IOException {
        URI folder = URI.create("/" + IdUtils.create() + "/");
        storageInterface.createDirectory(TenantService.MAIN_TENANT, null, folder);

        assertTimeoutPreemptively(
            Duration.ofSeconds(30),
            () -> assertThrows(FileNotFoundException.class, () -> storageInterface.get(TenantService.MAIN_TENANT, null, folder))
        );
    }

    @Test
    void moveFolderWithEmptySubFolder() throws IOException {
        String sourcePrefix = IdUtils.create();
        String targetPrefix = IdUtils.create();
        URI sourceUri = URI.create("/" + sourcePrefix + "/");
        URI targetUri = URI.create("/" + targetPrefix + "/");

        storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + sourcePrefix + "/file.txt"), new ByteArrayInputStream("data".getBytes()));
        storageInterface.createDirectory(TenantService.MAIN_TENANT, null, URI.create("/" + sourcePrefix + "/empty/"));

        storageInterface.move(TenantService.MAIN_TENANT, null, sourceUri, targetUri);

        var moved = storageInterface.allByPrefix(TenantService.MAIN_TENANT, null, targetUri, true);
        assertThat(moved.stream().map(URI::getPath).toList(), containsInAnyOrder(
            "/" + targetPrefix + "/file.txt",
            "/" + targetPrefix + "/empty/"
        ));
        assertThat(
            storageInterface.getAttributes(TenantService.MAIN_TENANT, null, URI.create("/" + targetPrefix + "/empty/")).getType(),
            is(FileAttributes.FileType.Directory)
        );
        assertThat(storageInterface.allByPrefix(TenantService.MAIN_TENANT, null, sourceUri, true), is(empty()));
        assertThrows(FileNotFoundException.class, () -> storageInterface.getAttributes(TenantService.MAIN_TENANT, null, sourceUri));
    }
}
