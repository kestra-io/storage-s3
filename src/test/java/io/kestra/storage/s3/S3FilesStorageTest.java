package io.kestra.storage.s3;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import io.kestra.core.storage.StorageTestSuite;
import io.kestra.core.storages.StorageInterface;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class S3FilesStorageTest extends StorageTestSuite {
    @BeforeAll
    void setup() throws IOException {
        Path tempDir = Files.createTempDirectory("s3files-test-");
        storageInterface = S3FilesStorage.builder()
            .mountPath(tempDir.toAbsolutePath().toString())
            .build();
        storageInterface.init();
    }
}
