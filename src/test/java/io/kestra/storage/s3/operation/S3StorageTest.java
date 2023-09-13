package io.kestra.storage.s3.operation;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.kestra.core.utils.IdUtils;
import io.kestra.storage.s3.config.LocalS3ClientConfig;
import io.kestra.storage.s3.config.S3Properties;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.shaded.com.google.common.io.CharStreams;
import org.testcontainers.utility.DockerImageName;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@MicronautTest
class S3StorageTest {

	private static S3Storage storageInterface;

	private static final S3Properties properties = new S3Properties();

	private static LocalStackContainer localstack;

	private static LocalS3ClientConfig localS3ClientConfig;

	@BeforeAll
	static void setUp() {
		DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:1.4.0");

		localstack = new LocalStackContainer(localstackImage)
				.withServices(LocalStackContainer.Service.S3);
		localstack.setPortBindings(List.of("4566:4566"));
		localstack.start();

		localS3ClientConfig = new LocalS3ClientConfig(localstack);
		storageInterface = new S3Storage(localS3ClientConfig.getAmazonS3(), localS3ClientConfig.getProperties());
	}

	@AfterAll
	static void stopContainers() {
		if (localstack != null) {
			localstack.stop();
		}
	}

	private URI putFile(URL resource, String path) throws Exception {
		return storageInterface.put(
				new URI(path),
				new FileInputStream(Objects.requireNonNull(resource).getFile())
		                           );
	}

	@Test
	void get() throws Exception {
		String prefix = IdUtils.create();

		URL resource = S3StorageTest.class.getClassLoader().getResource("application.properties");
		String content = CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource).getFile())));

		this.putFile(resource, "/" + prefix + "/storage/get.yml");

		URI item = new URI("/" + prefix + "/storage/get.yml");
		InputStream get = storageInterface.get(item);
		assertThat(CharStreams.toString(new InputStreamReader(get)), is(content));
		assertTrue(storageInterface.exists(item));
		assertThat(storageInterface.size(item), is((long) content.length()));
		assertThat(storageInterface.lastModifiedTime(item), notNullValue());

		InputStream getScheme = storageInterface.get(new URI("kestra:///" + prefix + "/storage/get.yml"));
		assertThat(CharStreams.toString(new InputStreamReader(getScheme)), is(content));
	}

	@Test
	void missing() {
		String prefix = IdUtils.create();

		assertThrows(FileNotFoundException.class, () -> {
			storageInterface.get(new URI("/" + prefix + "/storage/missing.yml"));
		});
	}

	@Test
	void put() throws Exception {
		String prefix = IdUtils.create();

		URL resource = S3StorageTest.class.getClassLoader().getResource("application.properties");
		URI put = this.putFile(resource, "/" + prefix + "/storage/put.yml");
		InputStream get = storageInterface.get(new URI("/" + prefix + "/storage/put.yml"));

		assertThat(put.toString(), is(new URI("kestra:///" + prefix + "/storage/put.yml").toString()));
		assertThat(
				CharStreams.toString(new InputStreamReader(get)),
				is(CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource).getFile()))))
		          );

		assertThat(storageInterface.size(new URI("/" + prefix + "/storage/put.yml")), is(66L));

		assertThrows(FileNotFoundException.class, () -> {
			assertThat(storageInterface.size(new URI("/" + prefix + "/storage/muissing.yml")), is(66L));
		});

		boolean delete = storageInterface.delete(put);
		assertThat(delete, is(true));

		delete = storageInterface.delete(put);
		assertThat(delete, is(false));

		assertThrows(FileNotFoundException.class, () -> {
			storageInterface.get(new URI("/" + prefix + "/storage/put.yml"));
		});
	}

	@Test
	void deleteByPrefix() throws Exception {
		String prefix = IdUtils.create();

		URL resource = S3StorageTest.class.getClassLoader().getResource("application.properties");

		List<String> path = Arrays.asList(
				"/" + prefix + "/storage/root.yml",
				"/" + prefix + "/storage/level1/1.yml",
				"/" + prefix + "/storage/level1/level2/1.yml"
		                                 );

		path.forEach(throwConsumer(s -> this.putFile(resource, s)));

		List<URI> deleted = storageInterface.deleteByPrefix(new URI("/" + prefix + "/storage/"));

		assertThat(deleted, containsInAnyOrder(path.stream().map(s -> URI.create("kestra://" + s)).toArray()));

		assertThrows(FileNotFoundException.class, () -> {
			storageInterface.get(new URI("/" + prefix + "/storage/"));
		});

		path
				.forEach(s -> {
					assertThrows(FileNotFoundException.class, () -> {
						storageInterface.get(new URI(s));
					});
				});
	}

	@Test
	void deleteByPrefixNoResult() throws Exception {
		String prefix = IdUtils.create();

		List<URI> deleted = storageInterface.deleteByPrefix(new URI("/" + prefix + "/storage/"));
		assertThat(deleted.size(), is(0));
	}

	@BeforeAll
	static void createBucket() throws Exception {
		try {
			storageInterface.createBucket(localS3ClientConfig.getProperties().getBucket());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}