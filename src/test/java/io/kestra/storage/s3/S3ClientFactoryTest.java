package io.kestra.storage.s3;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class S3ClientFactoryTest {
    @Test
    void cachedS3Clients() {
        S3Storage mock = Mockito.mock(S3Storage.class);
        S3ClientFactory.getS3Client(mock);
        assertThat(Mockito.mockingDetails(mock).getInvocations().size(), greaterThan(1));
        Mockito.clearInvocations(mock);
        S3ClientFactory.getS3Client(mock);
        // Only hashcode which is excluded from invocations should be invoked
        assertThat(Mockito.mockingDetails(mock).getInvocations().isEmpty(), is(true));

        S3ClientFactory.getAsyncS3Client(mock);
        assertThat(Mockito.mockingDetails(mock).getInvocations().size(), greaterThan(1));
        Mockito.clearInvocations(mock);
        S3ClientFactory.getAsyncS3Client(mock);
        assertThat(Mockito.mockingDetails(mock).getInvocations().isEmpty(), is(true));
    }
}
