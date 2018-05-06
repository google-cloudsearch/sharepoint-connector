package com.google.enterprise.cloud.search.sharepoint;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.enterprise.cloud.search.sharepoint.SharePointIncrementalCheckpoint.ChangeObjectType;
import com.google.enterprise.cloud.search.sharepoint.SharePointIncrementalCheckpoint.DiffKind;
import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SharePointIncrementalCheckpointTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testBuildAndParseContentDB() throws IOException {
    // Act
    SharePointIncrementalCheckpoint checkpoint =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.CONTENT_DB)
            .addChangeToken("obj1", "ch1")
            .build();
    byte[] payload = checkpoint.encodePayload();
    SharePointIncrementalCheckpoint parsed = SharePointIncrementalCheckpoint.parse(payload);

    // Assert
    assertTrue(Objects.nonNull(checkpoint));
    assertTrue(checkpoint.getObjectType() == ChangeObjectType.CONTENT_DB);
    assertEquals(checkpoint.getTokens(), Collections.singletonMap("obj1", "ch1"));
    assertEquals(checkpoint, parsed);
    assertEquals(
        ImmutableMap.builder()
            .put(DiffKind.ADD, Collections.emptySet())
            .put(DiffKind.REMOVE, Collections.emptySet())
            .put(DiffKind.MODIFIED, Collections.emptySet())
            .put(DiffKind.NOT_MODIFIED, Collections.singleton("obj1"))
            .build(),
        checkpoint.diff(checkpoint));
  }

  @Test
  public void testBuildAndParseSiteCollection() throws IOException {
    // Act
    SharePointIncrementalCheckpoint checkpoint =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.SITE_COLLECTION)
            .addChangeToken("obj1", "ch1")
            .build();
    byte[] payload = checkpoint.encodePayload();
    SharePointIncrementalCheckpoint parsed = SharePointIncrementalCheckpoint.parse(payload);

    // Assert
    assertTrue(Objects.nonNull(checkpoint));
    assertTrue(checkpoint.getObjectType() == ChangeObjectType.SITE_COLLECTION);
    assertEquals(checkpoint.getTokens(), Collections.singletonMap("obj1", "ch1"));
    assertEquals(checkpoint, parsed);
    assertEquals(
        ImmutableMap.builder()
            .put(DiffKind.ADD, Collections.emptySet())
            .put(DiffKind.REMOVE, Collections.emptySet())
            .put(DiffKind.MODIFIED, Collections.emptySet())
            .put(DiffKind.NOT_MODIFIED, Collections.singleton("obj1"))
            .build(),
        checkpoint.diff(checkpoint));
  }

  @Test
  public void testParseNull() throws IOException {
    // Act
    SharePointIncrementalCheckpoint checkpoint = SharePointIncrementalCheckpoint.parse(null);

    // Assert
    assertTrue(Objects.isNull(checkpoint));
  }

  @Test
  public void testParseEmptyArray() throws IOException {
    // Act
    SharePointIncrementalCheckpoint checkpoint =
        SharePointIncrementalCheckpoint.parse(new byte[] {});

    // Assert
    assertTrue(Objects.isNull(checkpoint));
  }

  @Test
  public void testParseInvalidPayload() throws IOException {
    // Act and Assert
    thrown.expect(IOException.class);
    SharePointIncrementalCheckpoint.parse("invalid".getBytes());
  }

  @Test
  public void testParseEmptyJson() throws IOException {
    // Act
    SharePointIncrementalCheckpoint checkpoint =
        SharePointIncrementalCheckpoint.parse("{}".getBytes());

    // Assert
    assertTrue(Objects.isNull(checkpoint));
  }

  @Test
  public void testDefaultConstructor() {
    // Act
    SharePointIncrementalCheckpoint checkpoint = new SharePointIncrementalCheckpoint();

    // Assert
    assertFalse(checkpoint.isValid());
  }

  @Test
  public void tesOnlyObjectTypeSet() {
    // Act
    SharePointIncrementalCheckpoint checkpoint =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.CONTENT_DB).build();

    // Assert
    assertFalse(checkpoint.isValid());
  }

  @Test
  public void testDiff() {
    SharePointIncrementalCheckpoint checkpoint1 =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.CONTENT_DB)
            .addChangeToken("obj1", "ch1")
            .addChangeToken("obj2", "ch2")
            .addChangeToken("obj3", "ch3")
            .build();
    SharePointIncrementalCheckpoint checkpoint2 =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.CONTENT_DB)
            .addChangeToken("obj1", "ch1updated")
            .addChangeToken("obj3", "ch3")
            .addChangeToken("obj4", "ch4")
            .build();
    assertEquals(
        ImmutableMap.builder()
            .put(DiffKind.ADD, Collections.singleton("obj4"))
            .put(DiffKind.REMOVE, Collections.singleton("obj2"))
            .put(DiffKind.MODIFIED, Collections.singleton("obj1"))
            .put(DiffKind.NOT_MODIFIED, Collections.singleton("obj3"))
            .build(),
        checkpoint1.diff(checkpoint2));
  }
}
