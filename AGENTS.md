# Kestra S3 Storage

## What

S3 storage plugin for Kestra

## Why

Enables Kestra to use Amazon S3 as its internal storage backend for persisting flow execution data, logs, and artifacts.

## How

### Architecture

Single-module plugin.

### Project Structure

```
storage-s3/
├── src/main/java/io/kestra/storage/s3/
├── src/test/java/io/kestra/storage/s3/
├── build.gradle
└── README.md
```

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
