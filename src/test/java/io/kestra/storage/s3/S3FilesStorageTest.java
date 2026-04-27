package io.kestra.storage.s3;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import io.kestra.core.storage.StorageTestSuite;

class S3FilesStorageTest extends StorageTestSuite {

    @TempDir
    Path tempDir;

    @BeforeEach
    void setup() throws IOException {
        storageInterface = S3FilesStorage.builder()
            .mountPath(tempDir.toAbsolutePath().toString())
            .build();
        storageInterface.init();
    }
}
