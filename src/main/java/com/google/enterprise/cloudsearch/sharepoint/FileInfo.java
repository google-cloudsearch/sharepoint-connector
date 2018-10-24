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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

class FileInfo {
  /** Non-null contents. */
  private final InputStream contents;
  /** Non-null headers. Pair of header name and header value. */
  private final List<FileHeader> headers;

  private FileInfo(InputStream contents, List<FileHeader> headers) {
    this.contents = contents;
    this.headers = headers;
  }

  InputStream getContents() {
    return contents;
  }

  List<FileHeader> getHeaders() {
    return headers;
  }

  /** Find the first header with {@code name}, ignoring case. */
  String getFirstHeaderWithName(String name) {
    String nameLowerCase = name.toLowerCase(Locale.ENGLISH);
    for (int i = 0; i < headers.size(); i++) {
      String headerNameLowerCase = headers.get(i).header.toLowerCase(Locale.ENGLISH);
      if (headerNameLowerCase.equals(nameLowerCase)) {
        return headers.get(i).value;
      }
    }
    return null;
  }

  static class FileHeader {
    private final String header;
    private final String value;

    FileHeader(String header, String value) {
      this.header = header;
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof FileHeader)) {
        return false;
      }
      FileHeader that = (FileHeader) o;
      return Objects.equals(header, that.header)
          && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(header, value);
    }
  }

  static class Builder {
    private InputStream contents;
    private List<FileHeader> headers = Collections.emptyList();

    Builder(InputStream contents) {
      setContents(contents);
    }

    Builder setContents(InputStream contents) {
      if (contents == null) {
        throw new NullPointerException();
      }
      this.contents = contents;
      return this;
    }

    /** Sets the headers received as a response. */
    Builder setHeaders(List<FileHeader> headers) {
      if (headers == null) {
        throw new NullPointerException();
      }
      this.headers = Collections.unmodifiableList(new ArrayList<FileHeader>(headers));
      return this;
    }

    FileInfo build() {
      return new FileInfo(contents, headers);
    }
  }
}
