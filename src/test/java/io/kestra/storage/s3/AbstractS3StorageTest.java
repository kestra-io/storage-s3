package io.kestra.storage.s3;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.kestra.core.storage.StorageTestSuite;
import io.kestra.core.storages.FileAttributes;
import io.kestra.core.storages.StorageObject;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * S3-specific tests shared by every S3 backend under test. Each of them runs against a storage without a configured
 * {@code path} and against one with {@code path("kestra")}, whereas the inherited {@link StorageTestSuite} tests run
 * against the injected storage, which has no path (its {@code getPath} assertions expect so).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractS3StorageTest extends StorageTestSuite {
    static final String BUCKET = "kestra-unit-test";
    static final String PATH = "kestra";

    private final Map<String, S3Storage> storages = new HashMap<>();

    /**
     * Builder pre-configured with the backend under test (endpoint, credentials, bucket), to which the path is added.
     */
    abstract S3Storage.S3StorageBuilder storageBuilder();

    /**
     * @param path the configured path, an empty string meaning none
     */
    S3Storage storage(String path) throws IOException {
        var storage = storages.get(path);
        if (storage == null) {
            var builder = storageBuilder();
            if (!path.isEmpty()) {
                builder.path(path);
            }
            storage = builder.build();
            storage.init();
            createBucketIfMissing(storage);
            storages.put(path, storage);
        }
        return storage;
    }

    static void createBucketIfMissing(S3Storage storage) throws IOException {
        try {
            storage.createBucket();
        } catch (IOException e) {
            if (!(e.getCause() instanceof BucketAlreadyOwnedByYouException || e.getCause() instanceof BucketAlreadyExistsException)) {
                throw e;
            }
        }
    }

    @AfterAll
    void closeStorages() {
        storages.values().forEach(S3Storage::close);
        storages.clear();
    }

    /**
     * Runs the test body against a storage without a configured path, then against one with a path.
     */
    private void forEachStorage(StorageTestBody body) throws Exception {
        for (var path : new String[] { "", PATH }) {
            try {
                body.run(storage(path));
            } catch (AssertionError e) {
                throw new AssertionError("With path '" + path + "': " + e.getMessage(), e);
            }
        }
    }

    @FunctionalInterface
    private interface StorageTestBody {
        void run(S3Storage storage) throws Exception;
    }

    private static URI uri(String path) {
        return URI.create(path);
    }

    private static void putFile(S3Storage storage, String path) throws IOException {
        storage.put(TenantService.MAIN_TENANT, null, uri(path), new ByteArrayInputStream("data".getBytes()));
    }

    @Test
    void getFolderIsNotFound() throws Exception {
        forEachStorage(storage ->
        {
            var folder = uri("/" + IdUtils.create() + "/");
            storage.createDirectory(TenantService.MAIN_TENANT, null, folder);

            assertTimeoutPreemptively(
                Duration.ofSeconds(30),
                () -> assertThrows(FileNotFoundException.class, () -> storage.get(TenantService.MAIN_TENANT, null, folder))
            );
        });
    }

    @Test
    void moveFolderWithEmptySubFolder() throws Exception {
        forEachStorage(storage ->
        {
            String sourcePrefix = IdUtils.create();
            String targetPrefix = IdUtils.create();
            var sourceUri = uri("/" + sourcePrefix + "/");
            var targetUri = uri("/" + targetPrefix + "/");

            putFile(storage, "/" + sourcePrefix + "/file.txt");
            storage.createDirectory(TenantService.MAIN_TENANT, null, uri("/" + sourcePrefix + "/empty/"));

            storage.move(TenantService.MAIN_TENANT, null, sourceUri, targetUri);

            var moved = storage.allByPrefix(TenantService.MAIN_TENANT, null, targetUri, true);
            assertThat(
                moved.stream().map(URI::getPath).toList(), containsInAnyOrder(
                    "/" + targetPrefix + "/file.txt",
                    "/" + targetPrefix + "/empty/"
                )
            );
            assertThat(
                storage.getAttributes(TenantService.MAIN_TENANT, null, uri("/" + targetPrefix + "/empty/")).getType(),
                is(FileAttributes.FileType.Directory)
            );
            assertThat(storage.allByPrefix(TenantService.MAIN_TENANT, null, sourceUri, true), is(empty()));
            assertThrows(FileNotFoundException.class, () -> storage.getAttributes(TenantService.MAIN_TENANT, null, sourceUri));
        });
    }

    @Test
    void deletedFolderDoesNotExistAndIsRecreatedByPut() throws Exception {
        forEachStorage(storage ->
        {
            String parent = IdUtils.create();
            var parentUri = uri("/" + parent + "/");
            var folder = uri("/" + parent + "/a/");

            putFile(storage, "/" + parent + "/a/x.txt");
            assertTrue(storage.exists(TenantService.MAIN_TENANT, null, folder));

            // SeaweedFS keeps answering a HEAD with 200 on a folder marker removed by a bulk DeleteObjects
            storage.deleteByPrefix(TenantService.MAIN_TENANT, null, folder);
            assertFalse(storage.exists(TenantService.MAIN_TENANT, null, folder));

            putFile(storage, "/" + parent + "/a/y.txt");

            assertTrue(storage.exists(TenantService.MAIN_TENANT, null, folder));
            assertThat(
                storage.list(TenantService.MAIN_TENANT, null, parentUri).stream().map(FileAttributes::getFileName).toList(),
                contains("a")
            );
            assertThat(storage.getAttributes(TenantService.MAIN_TENANT, null, folder).getType(), is(FileAttributes.FileType.Directory));
        });
    }

    @Test
    void deletedInstanceFolderDoesNotExistAndIsRecreatedByPut() throws Exception {
        forEachStorage(storage ->
        {
            String parent = IdUtils.create();
            var folder = uri("/" + parent + "/a/");

            storage.putInstanceResource(null, uri("/" + parent + "/a/x.txt"), new StorageObject(Map.of(), new ByteArrayInputStream("data".getBytes())));
            assertTrue(storage.existsInstanceResource(null, folder));

            // same as above, the marker being listed before the file it contains as in a prefix listing
            storage.getS3ClientForTest().deleteObjects(
                DeleteObjectsRequest.builder()
                    .bucket(BUCKET)
                    .delete(
                        d -> d.objects(
                            ObjectIdentifier.builder().key(storage.getPath(uri("/" + parent + "/a/"))).build(),
                            ObjectIdentifier.builder().key(storage.getPath(uri("/" + parent + "/a/x.txt"))).build()
                        )
                    )
                    .build()
            );
            assertFalse(storage.existsInstanceResource(null, folder));

            storage.putInstanceResource(null, uri("/" + parent + "/a/y.txt"), new StorageObject(Map.of(), new ByteArrayInputStream("data".getBytes())));

            assertTrue(storage.existsInstanceResource(null, folder));
            assertThat(storage.getInstanceAttributes(null, folder).getType(), is(FileAttributes.FileType.Directory));
        });
    }

    @Test
    void moveFolderWithoutTrailingSlashLeavesSiblingWithSamePrefix() throws Exception {
        forEachStorage(storage ->
        {
            String parent = IdUtils.create();

            putFile(storage, "/" + parent + "/level1/file.txt");
            putFile(storage, "/" + parent + "/level12.yml");

            var moved = storage.move(TenantService.MAIN_TENANT, null, uri("/" + parent + "/level1"), uri("/" + parent + "/dest"));

            assertThat(moved, is(uri("kestra:///" + parent + "/dest")));
            assertTrue(storage.exists(TenantService.MAIN_TENANT, null, uri("/" + parent + "/level12.yml")));
            assertFalse(storage.exists(TenantService.MAIN_TENANT, null, uri("/" + parent + "/dest2.yml")));
            assertTrue(storage.exists(TenantService.MAIN_TENANT, null, uri("/" + parent + "/dest/file.txt")));
            assertFalse(storage.exists(TenantService.MAIN_TENANT, null, uri("/" + parent + "/level1/file.txt")));
            assertThat(
                storage.allByPrefix(TenantService.MAIN_TENANT, null, uri("/" + parent + "/"), false).stream().map(URI::getPath).toList(),
                containsInAnyOrder("/" + parent + "/level12.yml", "/" + parent + "/dest/file.txt")
            );
        });
    }

    @Test
    void deleteByPrefixDeletesMoreThan1000Objects() throws Exception {
        forEachStorage(storage ->
        {
            String parent = IdUtils.create();
            int count = 2100;

            // objects are created directly to keep the test fast, deleteByPrefix is what is under test
            var client = storage.getS3ClientForTest();
            for (int i = 0; i < count; i++) {
                client.putObject(
                    PutObjectRequest.builder()
                        .bucket(BUCKET)
                        .key(storage.getPath(TenantService.MAIN_TENANT, uri("/" + parent + "/file-" + i + ".txt")))
                        .build(),
                    RequestBody.empty()
                );
            }
            var folder = uri("/" + parent + "/");
            assertThat(storage.allByPrefix(TenantService.MAIN_TENANT, null, folder, false), hasSize(count));

            var deleted = storage.deleteByPrefix(TenantService.MAIN_TENANT, null, folder);

            assertThat(deleted, hasSize(count));
            assertThat(storage.allByPrefix(TenantService.MAIN_TENANT, null, folder, true), is(empty()));
        });
    }

    @Test
    void deleteByPrefixRemovesTheFolder() throws Exception {
        forEachStorage(storage ->
        {
            String prefix = IdUtils.create();
            var folder = uri("/" + prefix + "/storage/");

            putFile(storage, "/" + prefix + "/storage/a.yml");
            putFile(storage, "/" + prefix + "/storage/sub/b.yml");

            storage.deleteByPrefix(TenantService.MAIN_TENANT, null, folder);

            // get() short-circuits on a folder, so it can't tell whether the folder is gone
            assertThrows(FileNotFoundException.class, () -> storage.getAttributes(TenantService.MAIN_TENANT, null, folder));
            assertFalse(storage.exists(TenantService.MAIN_TENANT, null, folder));
        });
    }
}
