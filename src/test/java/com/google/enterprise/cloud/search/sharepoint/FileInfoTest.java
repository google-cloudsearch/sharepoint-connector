package com.google.enterprise.cloud.search.sharepoint;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.enterprise.cloud.search.sharepoint.FileInfo.FileHeader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class FileInfoTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testBuilder() throws IOException {
    FileInfo fileInfo =
        new FileInfo.Builder(new ByteArrayInputStream("golden".getBytes()))
            .setHeaders(Collections.singletonList(new FileHeader("some-header", "some-value")))
            .build();
    assertEquals("some-value", fileInfo.getFirstHeaderWithName("some-header"));
    String extractedContent = new String(ByteStreams.toByteArray(fileInfo.getContents()));
    assertEquals("golden", extractedContent);
    fileInfo.getContents().close();
  }

  @Test
  public void testNullContent() throws IOException {
    thrown.expect(NullPointerException.class);
    new FileInfo.Builder(null).build();
  }

  @Test
  public void testNullHeaders() throws IOException {
    thrown.expect(NullPointerException.class);
    new FileInfo.Builder(new ByteArrayInputStream("golden".getBytes())).setHeaders(null).build();
  }

  @Test
  public void testDuplicateHeaders() throws IOException {
    ImmutableList<FileInfo.FileHeader> headers =
        new ImmutableList.Builder<FileInfo.FileHeader>()
            .add(new FileHeader("some-header", "some-value"))
            .add(new FileHeader("different-header", "different-other-value"))
            .add(new FileHeader("some-header", "some-other-value"))
            .build();
    FileInfo fileInfo =
        new FileInfo.Builder(new ByteArrayInputStream("golden".getBytes()))
            .setHeaders(headers)
            .build();
    assertEquals("some-value", fileInfo.getFirstHeaderWithName("some-header"));
    assertEquals("different-other-value", fileInfo.getFirstHeaderWithName("different-header"));
    assertEquals(null, fileInfo.getFirstHeaderWithName("unknown-header"));
    assertEquals(headers, fileInfo.getHeaders());
  }
}
