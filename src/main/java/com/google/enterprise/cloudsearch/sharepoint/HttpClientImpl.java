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
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

class HttpClientImpl implements HttpClient {
  private static final Logger log = Logger.getLogger(HttpClientImpl.class.getName());
  static final String HTTP_SHAREPOINT_ERROR_HEADER = "SharePointError";
  static final String HTTP_REDIRECT_LOCATION_HEADER = "Location";
  private SharePointRequestContext requestContext;
  private final int maxRedirectsAllowed;
  private final boolean performBrowserLeniency;
  private final ConnectionFactory connectionFactory;

  private HttpClientImpl(Builder builder) {
    requestContext = checkNotNull(builder.requestContext);
    checkArgument(builder.maxRedirectsAllowed >= 0);
    maxRedirectsAllowed = builder.maxRedirectsAllowed;
    performBrowserLeniency = builder.performBrowserLeniency;
    connectionFactory = checkNotNull(builder.connectionFactory);
  }

  /**
   * Download content and response headers for {@link #url}. Initial URL request can be redirected
   * to another URL. While java handles redirects automatically, it doesn't encode redirect
   * locations properly if it contains query string parameters and auto redirect fails. Most modern
   * browsers support supports redirects with query strings. When {@link #performBrowserLeniency} is
   * true, connector follows entire redirection chain and encodes redirect locations.
   */
  @Override
  public FileInfo issueGetRequest(URL url) throws IOException {
    int redirectAttempt = 0;
    final URL initialRequest = url;
    HttpURLConnection conn;
    int responseCode;
    do {
      log.log(Level.FINER, "Handling URL {0}", url);
      conn = connectionFactory.getConnection(url);
      boolean isWhiteListed =
          initialRequest.getHost().equalsIgnoreCase(url.getHost())
              && initialRequest.getPort() == url.getPort();
      requestContext.addContext(conn, isWhiteListed);
      conn.setDoInput(true);
      conn.setDoOutput(false);
      // Set follow redirects to true here if connector need not to handle
      // encoding of redirect URLs.
      conn.setInstanceFollowRedirects(!performBrowserLeniency);
      responseCode = conn.getResponseCode();
      if (responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
        return null;
      }
      if (responseCode == HttpURLConnection.HTTP_OK || !performBrowserLeniency) {
        break;
      }
      if (responseCode != HttpURLConnection.HTTP_MOVED_TEMP
          && responseCode != HttpURLConnection.HTTP_MOVED_PERM) {
        closeConnection(conn, responseCode);
        throw new IOException(String.format("Got status code %d for URL %s", responseCode, url));
      }
      if ((maxRedirectsAllowed == 0)) {
        throw new IOException(
            String.format(
                "Got status code %d for url %s "
                    + "but connector is configured to follow 0 redirects.",
                responseCode, initialRequest));
      }
      redirectAttempt++;
      String redirectLocation = conn.getHeaderField(HTTP_REDIRECT_LOCATION_HEADER);
      // Close input stream for current connection since redirect is detected.
      tryCloseInputStream(conn.getInputStream());
      if (Strings.isNullOrEmpty(redirectLocation)) {
        throw new IOException(
            "No redirect location available for URL " + url);
      }
      log.log(
          Level.INFO, "Redirected to URL {0} from URL {1}", new Object[] {redirectLocation, url});
      try {
        url =
            new SharePointUrl.Builder(redirectLocation)
                .setPerformBrowserLeniency(performBrowserLeniency)
                .build()
                .toURL();
      } catch (IllegalArgumentException | URISyntaxException e) {
        throw new IOException("Invalid redirection url " + redirectLocation, e);
      }
    } while (redirectAttempt <= maxRedirectsAllowed);
    if (responseCode != HttpURLConnection.HTTP_OK) {
      closeConnection(conn, responseCode);
      throw new IOException(
          String.format(
              "Got status code %d for initial " + "request %s after %d redirect attempts.",
              responseCode, initialRequest, redirectAttempt));
    }
    String errorHeader = conn.getHeaderField(HTTP_SHAREPOINT_ERROR_HEADER);
    // SharePoint adds header SharePointError to response to indicate error
    // on SharePoint for requested URL.
    // errorHeader = 2 if SharePoint rejects current request because
    // of current processing load
    // errorHeader = 0 for other errors on SharePoint server

    if (errorHeader != null) {
      closeConnection(conn, responseCode);
      if ("2".equals(errorHeader)) {
        throw new IOException(
            "Got error 2 from SharePoint for URL ["
                + url
                + "]. Error Code 2 indicates SharePoint has rejected current "
                + "request because of current processing load on SharePoint.");
      } else {
        throw new IOException(
            "Got error " + errorHeader + " from SharePoint for URL [" + url + "].");
      }
    }

    List<FileInfo.FileHeader> headers = new LinkedList<FileInfo.FileHeader>();
    // Start at 1 since index 0 is special.
    for (int i = 1;; i++) {
      String key = conn.getHeaderFieldKey(i);
      if (key == null) {
        break;
      }
      String value = conn.getHeaderField(i);
      headers.add(new FileInfo.FileHeader(key, value));
    }
    log.log(Level.FINER, "Response HTTP headers: {0}", headers);
    return new FileInfo.Builder(conn.getInputStream()).setHeaders(headers).build();
  }

  /** Returns redirect location for input URL if one available (HTTP 302). Null otherwise. */
  @Override
  public String getRedirectLocation(URL url) throws IOException {
    // Handle Unicode. Java does not properly encode the GET.
    try {
      url = new URL(url.toURI().toASCIIString());
    } catch (URISyntaxException ex) {
      throw new IOException(ex);
    }
    HttpURLConnection conn = connectionFactory.getConnection(url);
    int responseCode = 0;
    try {
      requestContext.addContext(conn, true);
      conn.setDoInput(true);
      conn.setDoOutput(false);
      conn.setInstanceFollowRedirects(false);
      responseCode = conn.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_MOVED_TEMP) {
        log.log(
            Level.WARNING,
            "Received response code {0} instead of 302 for URL {1}",
            new Object[] {responseCode, url});
        return null;
      }
      return conn.getHeaderField(HTTP_REDIRECT_LOCATION_HEADER);
    } finally {
      closeConnection(conn, responseCode);
    }
  }

  private void closeConnection(HttpURLConnection conn, int responseCode) throws IOException {
    InputStream inputStream =
        responseCode >= HttpURLConnection.HTTP_BAD_REQUEST
            ? conn.getErrorStream()
            : conn.getInputStream();
    tryCloseInputStream(inputStream);
  }

  private boolean tryCloseInputStream(InputStream streamToClose) {
    if (streamToClose == null) {
      return false;
    }
    try {
      streamToClose.close();
      return true;
    } catch (IOException e) {
      log.log(Level.WARNING, "Error closing input stream", e);
      return false;
    }
  }

  static class ConnectionFactory {
    HttpURLConnection getConnection(URL url) throws IOException {
      return (HttpURLConnection) url.openConnection();
    }
  }

  static class Builder {
    private int maxRedirectsAllowed;
    private boolean performBrowserLeniency;
    private SharePointRequestContext requestContext;
    private ConnectionFactory connectionFactory;

    public Builder() {
      // http://docs.oracle.com/javase/7/docs/api/java/net/doc-files/net-properties.html
      // http.maxRedirects default 20
      maxRedirectsAllowed = 20;
      performBrowserLeniency = false;
      connectionFactory = new ConnectionFactory();
    }

    Builder setSharePointRequestContext(SharePointRequestContext requestContext) {
      this.requestContext = checkNotNull(requestContext);
      return this;
    }

    Builder setMaxRedirectsAllowed(int maxRedirectsAllowed) {
      this.maxRedirectsAllowed = maxRedirectsAllowed;
      return this;
    }

    Builder setPerformBrowserLeniency(boolean performBrowserLeniency) {
      this.performBrowserLeniency = performBrowserLeniency;
      return this;
    }

    @VisibleForTesting
    Builder setConnectionFactory(ConnectionFactory connectionFactory) {
      this.connectionFactory = checkNotNull(connectionFactory);
      return this;
    }

    HttpClientImpl build() {
      return new HttpClientImpl(this);
    }
  }
}
