package com.google.enterprise.cloudsearch.sharepoint;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Key;
import com.google.api.client.util.Value;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class SharePointIncrementalCheckpoint extends GenericJson {
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  @Key private ChangeObjectType objectType;
  @Key private Map<String, String> tokens;

  public SharePointIncrementalCheckpoint() {
    super();
    setFactory(JSON_FACTORY);
  }

  private SharePointIncrementalCheckpoint(Builder builder) {
    super();
    this.objectType = builder.objectType;
    this.tokens = ImmutableMap.copyOf(builder.tokens);
    setFactory(JSON_FACTORY);
  }

  public enum ChangeObjectType {
    @Value
    CONTENT_DB,
    @Value
    SITE_COLLECTION;
  }

  public enum DiffKind {
    ADD,
    REMOVE,
    MODIFIED,
    NOT_MODIFIED
  }

  public static class Builder {
    private final Map<String, String> tokens = new HashMap<>();
    private final ChangeObjectType objectType;

    Builder(ChangeObjectType changeObjectType) {
      this.objectType = changeObjectType;
    }

    Builder addChangeToken(String objectId, String changeToken) {
      tokens.put(objectId, changeToken);
      return this;
    }

    SharePointIncrementalCheckpoint build() {
      return new SharePointIncrementalCheckpoint(this);
    }
  }

  private static SharePointIncrementalCheckpoint parse(String payloadString) throws IOException {
    if (Strings.isNullOrEmpty(payloadString)) {
      return null;
    }
    return JSON_FACTORY.fromString(payloadString, SharePointIncrementalCheckpoint.class);
  }

  static SharePointIncrementalCheckpoint parse(byte[] payload) throws IOException {
    if (payload == null) {
      return null;
    }
    SharePointIncrementalCheckpoint parsed = parse(new String(payload, UTF_8));
    if (parsed == null || !parsed.isValid()) {
      return null;
    }
    return parsed;
  }

  byte[] encodePayload() throws IOException {
    return this.toPrettyString().getBytes(UTF_8);
  }

  Map<String, String> getTokens() {
    return tokens;
  }

  ChangeObjectType getObjectType() {
    return objectType;
  }

  Map<DiffKind, Set<String>> diff(SharePointIncrementalCheckpoint other) {
    Set<String> added =
        other
            .getTokens()
            .keySet()
            .stream()
            .filter(e -> !this.tokens.containsKey(e))
            .collect(Collectors.toSet());
    Set<String> removed =
        this.getTokens()
            .keySet()
            .stream()
            .filter(e -> !other.tokens.containsKey(e))
            .collect(Collectors.toSet());
    Set<String> updated =
        this.getTokens()
            .keySet()
            .stream()
            .filter(
                e ->
                    other.tokens.containsKey(e)
                        && !Objects.equals(tokens.get(e), other.tokens.get(e)))
            .collect(Collectors.toSet());
    Set<String> notModified =
        this.getTokens()
            .keySet()
            .stream()
            .filter(
                e ->
                    other.tokens.containsKey(e)
                        && Objects.equals(tokens.get(e), other.tokens.get(e)))
            .collect(Collectors.toSet());
    return ImmutableMap.<DiffKind, Set<String>>builder()
        .put(DiffKind.ADD, added)
        .put(DiffKind.REMOVE, removed)
        .put(DiffKind.MODIFIED, updated)
        .put(DiffKind.NOT_MODIFIED, notModified)
        .build();
  }

  boolean isValid() {
    return objectType != null && tokens != null && !tokens.isEmpty();
  }
}
