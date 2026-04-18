# Kestra S3 Storage

## What

- Implements the storage backend under `io.kestra.storage.s3`.
- Includes classes such as `S3FileAttributes`, `MetadataUtils`, `S3Storage`, `S3ClientFactory`.

## Why

- This repository implements a Kestra storage backend for Storage Plugin for Amazon S3.
- It stores namespace files and internal execution artifacts outside local disk.

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

## Local rules

- Keep the scope on Kestra internal storage behavior, not workflow task semantics.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
