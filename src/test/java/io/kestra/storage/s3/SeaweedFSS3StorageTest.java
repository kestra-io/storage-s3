package io.kestra.storage.s3;

import java.io.IOException;
import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Runs the storage test suite against the S3 gateway of SeaweedFS, which differs from AWS S3 on folder markers.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SeaweedFSS3StorageTest extends AbstractS3StorageTest {
    private static final int S3_PORT = 8333;

    private static GenericContainer<?> seaweedfs;

    @BeforeAll
    static void startSeaweedFS() {
        seaweedfs = new GenericContainer<>(DockerImageName.parse("chrislusf/seaweedfs:4.50"))
            .withCommand(
                "server", "-ip=localhost", "-ip.bind=0.0.0.0", "-dir=/data", "-volume.max=100",
                "-filer", "-s3", "-s3.port=" + S3_PORT
            )
            .withExposedPorts(S3_PORT)
            .waitingFor(Wait.forHttp("/").forPort(S3_PORT).forStatusCode(200).withStartupTimeout(Duration.ofSeconds(120)));
        seaweedfs.start();
    }

    @Override
    S3Storage.S3StorageBuilder storageBuilder() {
        return S3Storage.builder()
            .accessKey("accesskey")
            .secretKey("secretkey")
            .bucket(BUCKET)
            .region("us-east-1")
            .endpoint("http://" + seaweedfs.getHost() + ":" + seaweedfs.getMappedPort(S3_PORT))
            .forcePathStyle(true);
    }

    @AfterAll
    static void stopSeaweedFS() {
        if (seaweedfs != null) {
            seaweedfs.stop();
        }
    }

    // application-test.yml points the injected storage to LocalStack, so it is replaced to exercise SeaweedFS
    @BeforeEach
    void useSeaweedFS() throws IOException {
        storageInterface = storage("");
    }
}
