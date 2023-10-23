package io.kestra.storage.s3;

import io.kestra.core.storage.StorageTestSuite;
import io.kestra.core.storages.StorageInterface;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

class S3StorageTest extends StorageTestSuite {
    @Inject
    StorageInterface storageInterface;

    private static LocalStackContainer localstack;

    @BeforeAll
    static void startLocalstack() {
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:1.4.0"));
        // some tests use a real flow with hardcoded configuration, so we have to fix the binding port
        localstack.setPortBindings(java.util.List.of("4566:4566"));
        localstack.start();
    }

    @AfterAll
    static void stopLocalstack() {
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
}
