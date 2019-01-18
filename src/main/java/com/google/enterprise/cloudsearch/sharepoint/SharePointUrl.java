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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.util.Strings;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Objects;

class SharePointUrl {

  private final String url;
  private final boolean performBrowserLeniency;
  private final URI uri;

  private SharePointUrl(Builder builder) throws URISyntaxException {
    this.url = builder.url;
    this.performBrowserLeniency = builder.performBrowserLeniency;
    this.uri = encode();
  }

  URI getURI() {
    return uri;
  }

  String getUrl() {
    return url;
  }

  private URI encode() throws URISyntaxException {
    if (!performBrowserLeniency) {
      // If no need to perform browser leniency, just return properly escaped
      // string using toASCIIString() to handle unicode.
      return escape(url);
    }
    String[] urlParts = url.split("\\?", 2);
    URI encodedUri = escape(urlParts[0]);
    if (urlParts.length == 1) {
      return encodedUri;
    }
    // Special handling for path when path is empty. e.g. for URL
    // http://sharepoint.example.com?ID=1 generates 400 bad request
    // in Java code but in browser it works fine.
    // Following code block will generate URL as
    // http://sharepoint.example.com/?ID=1 which works fine in Java code.
    String path = "".equals(encodedUri.getPath()) ? "/" : encodedUri.getPath();
    // Create new URI with query parameters
    return new URI(
        encodedUri.getScheme(),
        encodedUri.getAuthority(),
        path,
        urlParts[1],
        encodedUri.getFragment());
  }

  static URI escape(String urlToEncode) throws URISyntaxException {
    // The path of the URI may be unencoded, but the rest of
    // the URI is correct. Thus, we split up the path from the host, and then
    // turn them into URIs separately, and then turn everything into a
    // properly-escaped string.
    String[] parts = urlToEncode.split("/", 4);
    checkArgument(parts.length >= 3, "Too few '/'s: " + urlToEncode);
    String host = parts[0] + "/" + parts[1] + "/" + parts[2];
    // Host must be properly-encoded already.
    URI hostUri = URI.create(host);
    if (parts.length == 3) {
      // There was no path.
      return hostUri;
    }
    URI pathUri = new URI(null, null, "/" + parts[3], null);
    return hostUri.resolve(pathUri);
  }

  URL toURL() throws MalformedURLException {
    return new URL(uri.toASCIIString());
  }

  String getRootUrl() throws URISyntaxException {
    return new URI(uri.getScheme(), uri.getAuthority(), null, null, null).toString();
  }

  @Override
  public String toString() {
    return String.format(
        "SharePointUrl(url=%s, performBrowserLeniency=%s)", url, performBrowserLeniency);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SharePointUrl)) {
      return false;
    }
    SharePointUrl that = (SharePointUrl) o;
    return performBrowserLeniency == that.performBrowserLeniency
        && Objects.equals(url, that.url)
        && Objects.equals(uri, that.uri);
  }

  @Override
  public int hashCode() {
    return Objects.hash(url, performBrowserLeniency, uri);
  }

  static class Builder {
    private String url;
    private boolean performBrowserLeniency = true;

    Builder(String url) {
      checkArgument(!Strings.isNullOrEmpty(url));
      this.url = url;
    }

    Builder setPerformBrowserLeniency(boolean performBrowserLeniency) {
      this.performBrowserLeniency = performBrowserLeniency;
      return this;
    }

    SharePointUrl build() throws URISyntaxException {
      url = getCanonicalUrl(url);
      return new SharePointUrl(this);
    }

    /**
     * Remove trailing slash from URLs as SharePoint doesn't like trailing slash in
     * SiteData.GetUrlSegments
     */
    static String getCanonicalUrl(String url) {
      if (!url.endsWith("/")) {
        return url;
      }
      return url.substring(0, url.length() - 1);
    }
  }
}
