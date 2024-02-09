package io.kestra.storage.s3;

import io.kestra.core.storage.StorageTestSuite;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.ResponseInputStream;

import java.io.*;
import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class S3StorageTest extends StorageTestSuite {
    @Inject
    StorageInterface storageInterface;

    private static LocalStackContainer localstack;

    @BeforeAll
    static void startLocalstack() {
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.3.2"));
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

    /**
     * This test ensures that in case of an empty file, we return a raw empty input stream and not a ResponseInputStream.
     * This is needed as the ResponseInputStream in case of an empty file still returns some data which prevents the checksum to work and leads to a stream reading failure.
     */
    @Test
    void getEmptyFile() throws IOException {
        String prefix = IdUtils.create();
        URI uri = URI.create("/" + prefix + "/empty.txt");
        storageInterface.put(null, uri, new ByteArrayInputStream(new byte[0]));

        InputStream inputStream = storageInterface.get(null, uri);
        assertThat(inputStream, not(instanceOf(ResponseInputStream.class)));
        assertThat(new BufferedReader(new InputStreamReader(inputStream)).lines().count(), is(0L));
    }
}
