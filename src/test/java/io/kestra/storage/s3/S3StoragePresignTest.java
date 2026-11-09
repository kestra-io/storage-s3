package io.kestra.storage.s3;

import java.net.URI;
import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kestra.core.storages.PresignRequest;
import io.kestra.core.storages.PresignedRequest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

class S3StoragePresignTest {

    private S3Storage storage;

    @BeforeEach
    void setUp() throws Exception {
        storage = S3Storage.builder()
            .bucket("test-bucket")
            .region("us-east-1")
            .accessKey("accesskey")
            .secretKey("secretkey")
            .build();
        storage.init();
    }

    @AfterEach
    void tearDown() {
        storage.close();
    }

    @Test
    void shouldCapTtlToSevenDaysWhenTtlIsLonger() throws Exception {
        PresignedRequest request = storage.presign(
            PresignRequest.builder().tenantId("main").uri(URI.create("/file.txt")).method(PresignedRequest.Method.GET).ttl(Duration.ofDays(30)).build()
        ).orElseThrow();

        assertThat(request.url().getQuery(), containsString("X-Amz-Expires=604800"));
    }

    @Test
    void shouldRefuseAnUploadLargerThanASinglePutCanCarry() {
        PresignRequest request = PresignRequest.builder()
            .tenantId("main")
            .uri(URI.create("/file.txt"))
            .method(PresignedRequest.Method.PUT)
            .ttl(Duration.ofMinutes(5))
            .contentLength(5L * 1024 * 1024 * 1024 + 1)
            .build();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> storage.presign(request));

        assertThat(exception.getMessage(), containsString("/file.txt"));
    }
}
