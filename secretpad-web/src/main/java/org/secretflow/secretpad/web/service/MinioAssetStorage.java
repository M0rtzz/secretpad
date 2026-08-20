package org.secretflow.secretpad.web.service;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.InputStream;

/** MinIO-backed immutable object storage for uploaded and materialized assets. */
@Component
public class MinioAssetStorage {
    private final AmazonS3 client;
    private final String bucket;

    public MinioAssetStorage(
            @Value("${secretpad.data-assets.minio.endpoint:http://127.0.0.1:9000}") String endpoint,
            @Value("${secretpad.data-assets.minio.access-key:minioadmin}") String accessKey,
            @Value("${secretpad.data-assets.minio.secret-key:minioadmin}") String secretKey,
            @Value("${secretpad.data-assets.minio.bucket:data-sandbox-assets}") String bucket) {
        ClientConfiguration config = new ClientConfiguration();
        config.setProtocol(endpoint.startsWith("https") ? Protocol.HTTPS : Protocol.HTTP);
        this.client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(config)
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .build();
        this.bucket = bucket;
    }

    public String put(String key, File file, String contentType, String checksum) {
        if (!client.doesBucketExistV2(bucket)) client.createBucket(bucket);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(file.length());
        metadata.setContentType(contentType);
        metadata.addUserMetadata("sha256", checksum);
        client.putObject(new PutObjectRequest(bucket, key, file).withMetadata(metadata));
        return "s3://" + bucket + "/" + key;
    }

    public InputStream open(String uri) {
        String prefix = "s3://" + bucket + "/";
        if (uri == null || !uri.startsWith(prefix)) throw new IllegalArgumentException("不支持的资产存储地址");
        return client.getObject(bucket, uri.substring(prefix.length())).getObjectContent();
    }

    public void delete(String uri) {
        String prefix = "s3://" + bucket + "/";
        if (uri != null && uri.startsWith(prefix)) client.deleteObject(bucket, uri.substring(prefix.length()));
    }

    public String encryptedSnapshot(String sourceUri, String destinationKey, String checksum) {
        String prefix = "s3://" + bucket + "/";
        if (sourceUri == null || !sourceUri.startsWith(prefix)) throw new IllegalArgumentException("跨节点快照源不在受管 MinIO 中");
        try (S3Object source = client.getObject(bucket, sourceUri.substring(prefix.length()));
             InputStream input = source.getObjectContent()) {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(source.getObjectMetadata().getContentLength());
            metadata.setContentType(source.getObjectMetadata().getContentType());
            metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
            metadata.addUserMetadata("sha256", checksum == null ? "" : checksum);
            metadata.addUserMetadata("immutable-snapshot", "true");
            client.putObject(bucket, destinationKey, input, metadata);
            return "s3://" + bucket + "/" + destinationKey;
        } catch (java.io.IOException e) {
            throw new IllegalStateException("创建跨节点加密快照失败", e);
        }
    }
}
