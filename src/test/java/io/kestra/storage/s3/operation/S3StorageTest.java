package io.kestra.storage.s3.operation;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

@MicronautTest
@Testcontainers
class S3StorageTest {

	@Inject
	private static IS3Storage s3Storage;

	private static LocalStackContainer localstack;

	@BeforeAll
	public static void setUp() {
		DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:1.4.0");

		localstack = new LocalStackContainer(localstackImage)
				.withServices(LocalStackContainer.Service.S3);
		localstack.setPortBindings(List.of("4566:4566"));
		localstack.start();
	}

	@AfterAll
	public static void stopContainers() {
		if (localstack != null) {
			localstack.stop();
		}
	}

	@Test
	void createBucket() {

	}

	@Test
	void download() {
	}

	@Test
	void exists() {
	}

	@Test
	void upload() {
	}

	@Test
	void delete() {
	}

	@Test
	void testDelete() {
	}
}