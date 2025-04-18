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

package org.apache.druid.storage.s3.output;

import com.amazonaws.AmazonClientException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.storage.s3.NoopServerSideEncryption;
import org.apache.druid.storage.s3.S3TransferConfig;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RetryableS3OutputStreamTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final TestAmazonS3 s3 = new TestAmazonS3(0);
  private final String path = "resultId";

  private S3OutputConfig config;
  private long chunkSize;

  private S3UploadManager s3UploadManager;

  @Before
  public void setup() throws IOException
  {
    final File tempDir = temporaryFolder.newFolder();
    chunkSize = 10L;
    config = new S3OutputConfig(
        "TEST",
        "TEST",
        tempDir,
        HumanReadableBytes.valueOf(chunkSize),
        2,
        false
    )
    {
      @Override
      public File getTempDir()
      {
        return tempDir;
      }

      @Override
      public Long getChunkSize()
      {
        return chunkSize;
      }

      @Override
      public int getMaxRetry()
      {
        return 2;
      }
    };

    s3UploadManager = new S3UploadManager(
        new S3OutputConfig("bucket", "prefix", EasyMock.mock(File.class), new HumanReadableBytes("5MiB"), 1),
        new S3ExportConfig("tempDir", new HumanReadableBytes("5MiB"), 1, null),
        new DruidProcessingConfigTest.MockRuntimeInfo(10, 0, 0),
        new StubServiceEmitter());
  }

  @Test
  public void testWriteAndHappy() throws IOException
  {
    chunkSize = 10;
    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
    try (RetryableS3OutputStream out =
             new RetryableS3OutputStream(config, s3, path, s3UploadManager)) {
      for (int i = 0; i < 25; i++) {
        bb.clear();
        bb.putInt(i);
        out.write(bb.array());
      }
    }
    // each chunk is 10 bytes, so there should be 10 chunks.
    Assert.assertEquals(10, s3.partRequests.size());
    s3.assertCompleted(chunkSize, Integer.BYTES * 25);
  }

  @Test
  public void testWriteSizeLargerThanConfiguredMaxChunkSizeShouldSucceed() throws IOException
  {
    chunkSize = 10;
    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES * 3);
    try (RetryableS3OutputStream out =
             new RetryableS3OutputStream(config, s3, path, s3UploadManager)) {
      bb.clear();
      bb.putInt(1);
      bb.putInt(2);
      bb.putInt(3);
      out.write(bb.array());
    }
    // each chunk 10 bytes, so there should be 2 chunks.
    Assert.assertEquals(2, s3.partRequests.size());
    s3.assertCompleted(chunkSize, Integer.BYTES * 3);
  }

  @Test
  public void testWriteSmallBufferShouldSucceed() throws IOException
  {
    chunkSize = 128;
    try (RetryableS3OutputStream out =
             new RetryableS3OutputStream(config, s3, path, s3UploadManager)) {
      for (int i = 0; i < 600; i++) {
        out.write(i);
      }
    }
    // each chunk 128 bytes, so there should be 5 chunks.
    Assert.assertEquals(5, s3.partRequests.size());
    s3.assertCompleted(chunkSize, 600);
  }

  @Test
  public void testWriteSmallBufferExactChunkSizeShouldSucceed() throws IOException
  {
    chunkSize = 128;
    final int fileSize = 128 * 5;
    try (RetryableS3OutputStream out =
             new RetryableS3OutputStream(config, s3, path, s3UploadManager)) {
      for (int i = 0; i < fileSize; i++) {
        out.write(i);
      }
    }
    // each chunk 128 bytes, so there should be 5 chunks.
    Assert.assertEquals(5, s3.partRequests.size());
    s3.assertCompleted(chunkSize, fileSize);
  }

  @Test
  public void testSuccessToUploadAfterRetry() throws IOException
  {
    final TestAmazonS3 s3 = new TestAmazonS3(1);

    chunkSize = 10;
    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
    try (RetryableS3OutputStream out =
             new RetryableS3OutputStream(config, s3, path, s3UploadManager)) {
      for (int i = 0; i < 25; i++) {
        bb.clear();
        bb.putInt(i);
        out.write(bb.array());
      }
    }
    // each chunk is 10 bytes, so there should be 10 chunks.
    Assert.assertEquals(10, s3.partRequests.size());
    s3.assertCompleted(chunkSize, Integer.BYTES * 25);
  }

  @Test
  public void testFailToUploadAfterRetries() throws IOException
  {
    final TestAmazonS3 s3 = new TestAmazonS3(3);

    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
    try (RetryableS3OutputStream out =
             new RetryableS3OutputStream(config, s3, path, s3UploadManager)) {
      for (int i = 0; i < 2; i++) {
        bb.clear();
        bb.putInt(i);
        out.write(bb.array());
      }

      bb.clear();
      bb.putInt(3);
      out.write(bb.array());
    }

    s3.assertCancelled();
  }

  private static class TestAmazonS3 extends ServerSideEncryptingAmazonS3
  {
    private final List<UploadPartRequest> partRequests = new ArrayList<>();

    private int uploadFailuresLeft;
    private boolean cancelled = false;
    @Nullable
    private CompleteMultipartUploadRequest completeRequest;

    private TestAmazonS3(int totalUploadFailures)
    {
      super(EasyMock.createMock(AmazonS3.class), new NoopServerSideEncryption(), new S3TransferConfig());
      this.uploadFailuresLeft = totalUploadFailures;
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request)
        throws SdkClientException
    {
      InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
      result.setUploadId("uploadId");
      return result;
    }

    @Override
    public UploadPartResult uploadPart(UploadPartRequest request) throws SdkClientException
    {
      if (uploadFailuresLeft > 0) {
        throw new AmazonClientException(
            new IOE("Upload failure test. Remaining failures [%s]", --uploadFailuresLeft)
        );
      }
      synchronized (partRequests) {
        partRequests.add(request);
      }
      UploadPartResult result = new UploadPartResult();
      result.setETag(StringUtils.format("etag-%s", request.getPartNumber()));
      result.setPartNumber(request.getPartNumber());
      return result;
    }

    @Override
    public void cancelMultiPartUpload(AbortMultipartUploadRequest request) throws SdkClientException
    {
      cancelled = true;
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request)
        throws SdkClientException
    {
      completeRequest = request;
      return new CompleteMultipartUploadResult();
    }

    private void assertCompleted(long chunkSize, long expectedFileSize)
    {
      Assert.assertNotNull(completeRequest);
      Assert.assertFalse(cancelled);

      Set<Integer> partNumbersFromRequest = partRequests.stream().map(UploadPartRequest::getPartNumber).collect(Collectors.toSet());
      Assert.assertEquals(partRequests.size(), partNumbersFromRequest.size());

      // Verify sizes of uploaded chunks
      int numSmallerChunks = 0;
      for (UploadPartRequest part : partRequests) {
        Assert.assertTrue(part.getPartSize() <= chunkSize);
        if (part.getPartSize() < chunkSize) {
          ++numSmallerChunks;
        }
      }
      Assert.assertTrue(numSmallerChunks <= 1);

      final List<PartETag> eTags = completeRequest.getPartETags();
      Assert.assertEquals(partRequests.size(), eTags.size());
      Assert.assertEquals(
          partNumbersFromRequest,
          eTags.stream().map(PartETag::getPartNumber).collect(Collectors.toSet())
      );
      Assert.assertEquals(
          partNumbersFromRequest.stream().map(partNumber -> "etag-" + partNumber).collect(Collectors.toSet()),
          eTags.stream().map(PartETag::getETag).collect(Collectors.toSet())
      );
      Assert.assertEquals(
          expectedFileSize,
          partRequests.stream().mapToLong(UploadPartRequest::getPartSize).sum()
      );
    }

    private void assertCancelled()
    {
      Assert.assertTrue(cancelled);
      Assert.assertNull(completeRequest);
    }
  }
}
