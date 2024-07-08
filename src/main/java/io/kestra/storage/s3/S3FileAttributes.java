package io.kestra.storage.s3;

import io.kestra.core.storages.FileAttributes;
import lombok.Builder;
import lombok.Value;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.IOException;
import java.util.Map;

@Value
@Builder
public class S3FileAttributes implements FileAttributes {

    String fileName;
    HeadObjectResponse head;
    boolean isDirectory;

    @Override
    public long getLastModifiedTime() {
        return head.lastModified().getEpochSecond();
    }

    /**
     * https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html
     * Amazon S3 maintains only the last modified date for each object. For example, the Amazon S3 console shows
     * the Last Modified date in the object Properties pane. When you initially create a new object, this date reflects
     * the date the object is created. If you replace the object, the date changes accordingly. So when we use the term
     * creation date, it is synonymous with the term last modified date.
     * @return
     */
    @Override
    public long getCreationTime() {
        return head.lastModified().getEpochSecond();
    }

    @Override
    public FileType getType() {
        if (isDirectory || fileName.endsWith("/") || head.contentType().equals("application/x-directory")) {
            return FileType.Directory;
        }
        return FileType.File;
    }

    @Override
    public long getSize() {
        return head.contentLength();
    }

    @Override
    public Map<String, String> getMetadata() {
        return head.metadata();
    }
}
