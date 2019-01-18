/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

/** Object for holding SharePoint change tokens. */
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

  /** SharePoint object type for associated change token */
  public enum ChangeObjectType {
    @Value
    CONTENT_DB,
    @Value
    SITE_COLLECTION;
  }

  enum DiffKind {
    ADD,
    REMOVE,
    MODIFIED,
    NOT_MODIFIED
  }

  /** Builder object for creating {@link SharePointIncrementalCheckpoint} */
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
