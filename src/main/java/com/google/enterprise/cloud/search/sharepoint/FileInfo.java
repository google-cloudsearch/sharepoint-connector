package com.google.enterprise.cloud.search.sharepoint;

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
      return Objects.equals(header, that.header) &&
          Objects.equals(value, that.value);
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
