package io.kestra.storage.s3;

import java.time.Instant;
import java.util.Map;

import io.kestra.core.storages.FileAttributes;

import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.Value;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

@Value
public class S3FileAttributes implements FileAttributes {
    String fileName;
    // null for a directory resolved from a listing
    @Nullable
    HeadObjectResponse head;
    Instant lastModified;
    boolean isDirectory;
    Map<String, String> metadata;

    @Builder
    public S3FileAttributes(String fileName, @Nullable HeadObjectResponse head, @Nullable Instant lastModified, boolean isDirectory) {
        this.fileName = fileName;
        this.head = head;
        var resolved = head != null ? head.lastModified() : lastModified;
        // some non-standard S3-compatible servers omit LastModified
        this.lastModified = resolved != null ? resolved : Instant.EPOCH;
        this.isDirectory = isDirectory;

        this.metadata = head != null ? MetadataUtils.toRetrievedMetadata(head.metadata()) : Map.of();
    }

    @Override
    public long getLastModifiedTime() {
        return lastModified.toEpochMilli();
    }

    /**
     * https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html
     * Amazon S3 maintains only the last modified date for each object. For example, the Amazon S3 console shows
     * the Last Modified date in the object Properties pane. When you initially create a new object, this date reflects
     * the date the object is created. If you replace the object, the date changes accordingly. So when we use the term
     * creation date, it is synonymous with the term last modified date.
     * 
     * @return
     */
    @Override
    public long getCreationTime() {
        return lastModified.toEpochMilli();
    }

    @Override
    public FileType getType() {
        if (isDirectory || fileName.endsWith("/") || (head != null && "application/x-directory".equals(head.contentType()))) {
            return FileType.Directory;
        }
        return FileType.File;
    }

    @Override
    public long getSize() {
        return head != null ? head.contentLength() : 0;
    }

    @Override
    public Map<String, String> getMetadata() {
        return metadata;
    }
}
