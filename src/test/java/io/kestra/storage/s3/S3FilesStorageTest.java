package io.kestra.storage.s3;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;

import io.kestra.core.storage.StorageTestSuite;
import io.kestra.core.storages.StorageInterface;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class S3FilesStorageTest extends StorageTestSuite {
    private StorageInterface nioStorage;

    @BeforeAll
    void createStorage() throws IOException {
        Path tempDir = Files.createTempDirectory("s3files-test-");
        nioStorage = S3FilesStorage.builder()
            .mountPath(tempDir.toAbsolutePath().toString())
            .build();
        nioStorage.init();
    }

    @BeforeEach
    void setup() {
        storageInterface = nioStorage;
    }
}
