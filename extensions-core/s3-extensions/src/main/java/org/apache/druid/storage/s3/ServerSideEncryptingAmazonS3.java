/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.storage.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import org.apache.druid.java.util.common.ISE;

import java.io.File;

/**
 * {@link AmazonS3} wrapper with {@link ServerSideEncryption}. Every {@link AmazonS3#putObject},
 * {@link AmazonS3#copyObject}, {@link AmazonS3#getObject}, and {@link AmazonS3#getObjectMetadata},
 * {@link AmazonS3#initiateMultipartUpload}, @{@link AmazonS3#uploadPart} methods should be
 * wrapped using ServerSideEncryption.
 * <p>
 * Additional methods can be added to this class if needed, but subclassing AmazonS3Client is discouraged to reduce
 * human mistakes like some methods are not encoded properly.
 */
public class ServerSideEncryptingAmazonS3
{
  public static Builder builder()
  {
    return new Builder();
  }

  private final AmazonS3 amazonS3;
  private final ServerSideEncryption serverSideEncryption;
  private final TransferManager transferManager;

  public ServerSideEncryptingAmazonS3(AmazonS3 amazonS3, ServerSideEncryption serverSideEncryption, S3TransferConfig transferConfig)
  {
    this.amazonS3 = amazonS3;
    this.serverSideEncryption = serverSideEncryption;
    if (transferConfig.isUseTransferManager()) {
      this.transferManager = TransferManagerBuilder.standard()
          .withS3Client(amazonS3)
          .withMinimumUploadPartSize(transferConfig.getMinimumUploadPartSize())
          .withMultipartUploadThreshold(transferConfig.getMultipartUploadThreshold())
          .build();
    } else {
      this.transferManager = null;
    }
  }

  public AmazonS3 getAmazonS3()
  {
    return amazonS3;
  }

  public boolean doesObjectExist(String bucket, String objectName)
  {
    try {
      // Ignore return value, just want to see if we can get the metadata at all.
      getObjectMetadata(bucket, objectName);
      return true;
    }
    catch (AmazonS3Exception e) {
      if (e.getStatusCode() == 404) {
        // Object not found.
        return false;
      } else {
        // Some other error: re-throw.
        throw e;
      }
    }
  }

  public ListObjectsV2Result listObjectsV2(ListObjectsV2Request request)
  {
    return amazonS3.listObjectsV2(request);
  }

  public AccessControlList getBucketAcl(String bucket)
  {
    return amazonS3.getBucketAcl(bucket);
  }

  public ObjectMetadata getObjectMetadata(String bucket, String key)
  {
    final GetObjectMetadataRequest getObjectMetadataRequest = serverSideEncryption.decorate(
        new GetObjectMetadataRequest(bucket, key)
    );
    return amazonS3.getObjectMetadata(getObjectMetadataRequest);
  }

  public S3Object getObject(String bucket, String key)
  {
    return getObject(new GetObjectRequest(bucket, key));
  }

  public S3Object getObject(GetObjectRequest request)
  {
    return amazonS3.getObject(serverSideEncryption.decorate(request));
  }

  public PutObjectResult putObject(String bucket, String key, File file)
  {
    return putObject(new PutObjectRequest(bucket, key, file));
  }

  public PutObjectResult putObject(PutObjectRequest request)
  {
    return amazonS3.putObject(serverSideEncryption.decorate(request));
  }

  public CopyObjectResult copyObject(CopyObjectRequest request)
  {
    return amazonS3.copyObject(serverSideEncryption.decorate(request));
  }

  public void deleteObject(String bucket, String key)
  {
    amazonS3.deleteObject(bucket, key);
  }

  public void deleteObjects(DeleteObjectsRequest request)
  {
    amazonS3.deleteObjects(request);
  }


  public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request)
      throws SdkClientException
  {
    return amazonS3.initiateMultipartUpload(serverSideEncryption.decorate(request));
  }

  public UploadPartResult uploadPart(UploadPartRequest request)
      throws SdkClientException
  {
    return amazonS3.uploadPart(serverSideEncryption.decorate(request));
  }

  public void cancelMultiPartUpload(AbortMultipartUploadRequest request)
      throws SdkClientException
  {
    amazonS3.abortMultipartUpload(request);
  }

  public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request)
      throws SdkClientException
  {
    return amazonS3.completeMultipartUpload(request);
  }

  public void upload(PutObjectRequest request) throws InterruptedException
  {
    if (transferManager == null) {
      putObject(request);
    } else {
      Upload transfer = transferManager.upload(serverSideEncryption.decorate(request));
      transfer.waitForCompletion();
    }
  }

  public static class Builder
  {
    private AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3Client.builder();
    private S3StorageConfig s3StorageConfig = new S3StorageConfig(new NoopServerSideEncryption(), null);

    public Builder setAmazonS3ClientBuilder(AmazonS3ClientBuilder amazonS3ClientBuilder)
    {
      this.amazonS3ClientBuilder = amazonS3ClientBuilder;
      return this;
    }

    public Builder setS3StorageConfig(S3StorageConfig s3StorageConfig)
    {
      this.s3StorageConfig = s3StorageConfig;
      return this;
    }

    public AmazonS3ClientBuilder getAmazonS3ClientBuilder()
    {
      return this.amazonS3ClientBuilder;
    }

    public S3StorageConfig getS3StorageConfig()
    {
      return this.s3StorageConfig;
    }

    public ServerSideEncryptingAmazonS3 build()
    {
      if (amazonS3ClientBuilder == null) {
        throw new ISE("AmazonS3ClientBuilder cannot be null!");
      }
      if (s3StorageConfig == null) {
        throw new ISE("S3StorageConfig cannot be null!");
      }

      AmazonS3 amazonS3Client;
      try {
        amazonS3Client = S3Utils.retryS3Operation(() -> amazonS3ClientBuilder.build());
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }

      return new ServerSideEncryptingAmazonS3(amazonS3Client, s3StorageConfig.getServerSideEncryption(), s3StorageConfig.getS3TransferConfig());
    }
  }
}
