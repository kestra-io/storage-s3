package io.kestra.storage.s3;

import java.io.*;
import java.net.URI;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class S3StorageTest extends AbstractS3StorageTest {
    private static LocalStackContainer localstack;

    @BeforeAll
    void startLocalstack() {
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8.1"))
            .withServices(S3);
        // some tests use a real flow with hardcoded configuration, so we have to fix the binding port
        localstack.setPortBindings(java.util.List.of("4566:4566"));
        localstack.start();
    }

    @AfterAll
    void stopLocalstack() {
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

    // the injected storage used by the inherited suite is configured in application-test.yml and expects the bucket to exist
    @BeforeEach
    void createBucket() throws IOException {
        createBucketIfMissing(storage(PATH));
    }

    /**
     * This test ensures that in case of an empty file, we return a raw empty input stream and not a ResponseInputStream.
     * This is needed as the ResponseInputStream in case of an empty file still returns some data which prevents the checksum to work and leads to a stream reading failure.
     */
    @Test
    void getEmptyFile() throws IOException {
        var storage = storage(PATH);
        String prefix = IdUtils.create();
        URI uri = URI.create("/" + prefix + "/empty.txt");
        storage.put(TenantService.MAIN_TENANT, null, uri, new ByteArrayInputStream(new byte[0]));

        InputStream inputStream = storage.get(TenantService.MAIN_TENANT, null, uri);
        assertThat(inputStream, not(instanceOf(ResponseInputStream.class)));
        assertThat(new BufferedReader(new InputStreamReader(inputStream)).lines().count(), is(0L));
    }

    @Test
    void shouldListMoreThan1000ObjectsWithPagination() throws IOException {
        var storage = storage(PATH);
        String prefix = IdUtils.create();
        int fileCount = 1500;

        for (int i = 0; i < fileCount; i++) {
            URI uri = URI.create("/" + prefix + "/file-" + i + ".txt");
            storage.put(TenantService.MAIN_TENANT, null, uri, new ByteArrayInputStream(("file " + i).getBytes()));
        }

        URI folderUri = URI.create("/" + prefix + "/");
        var uris = storage.allByPrefix(TenantService.MAIN_TENANT, null, folderUri, false);

        assertThat("Should list all files across paginated S3 responses", uris.size(), is(fileCount));
        assertThat("All listed URIs should point to the right folder", uris, everyItem(hasToString(startsWith("kestra:///" + prefix + "/"))));
    }

    @Test
    void shouldMoveMoreThan1000FilesWithPagination() throws IOException {
        var storage = storage(PATH);
        String sourcePrefix = IdUtils.create();
        String targetPrefix = IdUtils.create();
        int fileCount = 1200;

        // Upload files under the source directory
        for (int i = 0; i < fileCount; i++) {
            URI uri = URI.create("/" + sourcePrefix + "/file-" + i + ".txt");
            storage.put(TenantService.MAIN_TENANT, null, uri, new ByteArrayInputStream(("data-" + i).getBytes()));
        }

        URI sourceUri = URI.create("/" + sourcePrefix + "/");
        URI targetUri = URI.create("/" + targetPrefix + "/");

        // Move the directory
        storage.move(TenantService.MAIN_TENANT, null, sourceUri, targetUri);

        // Check that all files exist at the destination
        var destinationUris = storage.allByPrefix(TenantService.MAIN_TENANT, null, targetUri, false);
        assertThat("All files should have been moved", destinationUris.size(), is(fileCount));

        // Check that the original files no longer exist
        var sourceFiles = storage.allByPrefix(TenantService.MAIN_TENANT, null, sourceUri, false);
        assertThat("Source directory should be empty after move", sourceFiles, is(empty()));
    }

    // only AWS-like servers support bucket versioning, which is why this one is not in the shared tests
    @Test
    void s3FilesCompatibleModeEnablesBucketVersioning() throws IOException {
        var plain = storage(PATH);

        var compat = storageBuilder()
            .path(PATH)
            .s3FilesCompatible(true)
            .build();
        compat.init();

        GetBucketVersioningResponse resp = plain.getS3ClientForTest()
            .getBucketVersioning(GetBucketVersioningRequest.builder().bucket(BUCKET).build());
        assertThat(resp.status(), is(BucketVersioningStatus.ENABLED));
        compat.close();
    }
}
