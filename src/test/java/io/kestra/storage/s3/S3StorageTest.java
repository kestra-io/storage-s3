package io.kestra.storage.s3;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.Instant;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.GetBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.GetBucketVersioningResponse;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

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

    @Test
    void purgeByLastModifiedHonorsWindow() throws IOException, InterruptedException {
        var storage = storage(PATH);
        String root = IdUtils.create();
        String executionId = IdUtils.create();
        URI afterFolder = URI.create("/" + root + "/executions/" + executionId + "/");
        URI siblingFolder = URI.create("/" + root + "/executions/" + executionId + "-sibling/");

        storage.put(
            TenantService.MAIN_TENANT, null, URI.create(afterFolder + "before.txt"),
            new ByteArrayInputStream(new byte[0])
        );
        storage.put(
            TenantService.MAIN_TENANT, null, URI.create(siblingFolder + "keep.txt"),
            new ByteArrayInputStream(new byte[0])
        );

        Thread.sleep(1_100);
        Instant boundary = Instant.now();
        Thread.sleep(1_100);

        storage.put(
            TenantService.MAIN_TENANT, null, URI.create(afterFolder + "after.txt"),
            new ByteArrayInputStream(new byte[0])
        );

        var deletedAfter = storage.purgeByLastModified(
            TenantService.MAIN_TENANT, null, afterFolder, boundary, null, false
        );
        assertThat(deletedAfter, hasSize(1));
        assertThat(
            deletedAfter.stream().map(URI::getPath).toList(),
            contains("/" + root + "/executions/" + executionId + "/after.txt")
        );
        assertThat(
            storage.allByPrefix(TenantService.MAIN_TENANT, null, afterFolder, false)
                .stream()
                .map(URI::getPath)
                .toList(),
            contains("/" + root + "/executions/" + executionId + "/before.txt")
        );

        var deletedBefore = storage.purgeByLastModified(
            TenantService.MAIN_TENANT, null, afterFolder, null, boundary, false
        );
        assertThat(deletedBefore, hasSize(1));
        assertThat(
            deletedBefore.stream().map(URI::getPath).toList(),
            contains("/" + root + "/executions/" + executionId + "/before.txt")
        );
        assertThat(
            storage.allByPrefix(TenantService.MAIN_TENANT, null, afterFolder, false)
                .stream()
                .map(URI::getPath)
                .toList(),
            is(empty())
        );
        assertThat(storage.exists(TenantService.MAIN_TENANT, null, URI.create(siblingFolder + "keep.txt")), is(true));
    }

    @Test
    void purgeByLastModifiedDeletesEveryVersionInCompatibleMode() throws IOException {
        var storage = storageBuilder()
            .path(PATH)
            .s3FilesCompatible(true)
            .build();
        storage.init();

        try {
            String root = IdUtils.create();
            String executionId = IdUtils.create();
            URI folder = URI.create("/" + root + "/executions/" + executionId + "/");
            URI file = URI.create(folder + "file.txt");

            storage.put(
                TenantService.MAIN_TENANT, null, file,
                new ByteArrayInputStream("first".getBytes())
            );
            storage.put(
                TenantService.MAIN_TENANT, null, file,
                new ByteArrayInputStream("second".getBytes())
            );

            var deleted = storage.purgeByLastModified(
                TenantService.MAIN_TENANT, null, folder, null, null, false
            );

            assertThat(deleted, hasSize(1));
            assertThat(
                deleted.stream().map(URI::getPath).toList(),
                contains("/" + root + "/executions/" + executionId + "/file.txt")
            );
            assertThat(storage.allByPrefix(TenantService.MAIN_TENANT, null, folder, false), is(empty()));

            String key = storage.getPath(TenantService.MAIN_TENANT, file);
            var versions = storage.getS3ClientForTest().listObjectVersions(
                ListObjectVersionsRequest.builder()
                    .bucket(BUCKET)
                    .prefix(key)
                    .build()
            );
            assertThat(versions.versions(), is(empty()));
            assertThat(versions.deleteMarkers(), is(empty()));
        } finally {
            storage.close();
        }
    }

    @Test
    void purgeByLastModifiedDryRunDoesNotDeleteObjects() throws IOException {
        var storage = storage(PATH);
        URI folder = URI.create("/" + IdUtils.create() + "/");
        storage.put(
            TenantService.MAIN_TENANT, null, URI.create(folder + "file.txt"),
            new ByteArrayInputStream(new byte[0])
        );

        var wouldDelete = storage.purgeByLastModified(
            TenantService.MAIN_TENANT, null, folder, null, null, true
        );

        assertThat(wouldDelete, hasSize(1));
        assertThat(storage.allByPrefix(TenantService.MAIN_TENANT, null, folder, false), hasSize(1));
    }

    @Test
    void purgeByLastModifiedReturnsEmptyForMissingPrefix() throws IOException {
        var storage = storage(PATH);
        URI missingFolder = URI.create("/" + IdUtils.create() + "/");

        var deleted = storage.purgeByLastModified(
            TenantService.MAIN_TENANT, null, missingFolder, null, null, false
        );

        assertThat(deleted, is(empty()));
    }

    @Test
    void purgeByLastModifiedDoesNotMatchSiblingPrefix() throws IOException {
        var storage = storage(PATH);
        String folder = IdUtils.create();
        URI target = URI.create("/" + folder + "/");
        URI sibling = URI.create("/" + folder + "sibling/keep.txt");

        storage.put(
            TenantService.MAIN_TENANT, null, URI.create(target + "delete.txt"),
            new ByteArrayInputStream(new byte[0])
        );
        storage.put(
            TenantService.MAIN_TENANT, null, sibling,
            new ByteArrayInputStream(new byte[0])
        );

        var deleted = storage.purgeByLastModified(
            TenantService.MAIN_TENANT, null, target, null, null, false
        );

        assertThat(deleted, hasSize(1));
        assertThat(storage.exists(TenantService.MAIN_TENANT, null, sibling), is(true));
    }

    @Test
    void purgeByLastModifiedDoesNotReturnDirectoryMarkers() throws IOException {
        var storage = storage(PATH);
        String folder = IdUtils.create();
        URI target = URI.create("/" + folder + "/");
        String directoryMarker = storage.getPath(TenantService.MAIN_TENANT, URI.create(target + "exec1/"));

        storage.getS3ClientForTest().putObject(
            PutObjectRequest.builder()
                .bucket(BUCKET)
                .key(directoryMarker)
                .build(),
            RequestBody.empty()
        );

        var deleted = storage.purgeByLastModified(
            TenantService.MAIN_TENANT, null, target, null, null, false
        );

        assertThat(deleted, is(empty()));
        assertThat(storage.exists(TenantService.MAIN_TENANT, null, URI.create(target + "exec1/")), is(true));
    }

    @Test
    void purgeByLastModifiedHandlesMoreThan1000Objects() throws IOException {
        var storage = storage(PATH);
        String prefix = IdUtils.create();
        URI folder = URI.create("/" + prefix + "/");
        int fileCount = 1200;

        for (int i = 0; i < fileCount; i++) {
            URI uri = URI.create(folder + "file-" + i + ".txt");
            storage.getS3ClientForTest().putObject(
                PutObjectRequest.builder()
                    .bucket(BUCKET)
                    .key(storage.getPath(TenantService.MAIN_TENANT, uri))
                    .build(),
                RequestBody.empty()
            );
        }

        var deleted = storage.purgeByLastModified(
            TenantService.MAIN_TENANT, null, folder, null, null, false
        );

        assertThat(deleted, hasSize(fileCount));
        assertThat(storage.allByPrefix(TenantService.MAIN_TENANT, null, folder, false), is(empty()));
    }
}
