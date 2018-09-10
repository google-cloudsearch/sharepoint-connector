package com.google.enterprise.cloudsearch.sharepoint;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableMap;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.xml.ws.BindingProvider;
import javax.xml.ws.handler.MessageContext;

class SharePointRequestContext {
  static final String HEADER_X_FORMS_BASED_AUTH_ACCEPTED = "X-FORMS_BASED_AUTH_ACCEPTED";
  static final String WS_REQUEST_TIMEOUT = "com.sun.xml.ws.request.timeout";
  static final String INTERNAL_WS_REQUEST_TIMEOUT = "com.sun.xml.internal.ws.request.timeout";
  static final String WS_CONNECT_TIMEOUT = "com.sun.xml.ws.connect.timeout";
  static final String INTERNAL_WS_CONNECT_TIMEOUT = "com.sun.xml.internal.ws.connect.timeout";

  private final FormsAuthenticationHandler authenticationHandler;
  private final String userAgent;
  private final Map<String, Integer> timeoutConfiguration;
  private final int socketTimeoutMillis;
  private final int readTimeoutMillis;

  private SharePointRequestContext(Builder builder) {
    authenticationHandler = builder.authenticationHandler;
    userAgent = builder.userAgent;
    socketTimeoutMillis = builder.socketTimeoutMillis;
    readTimeoutMillis = builder.readTimeoutMillis;
    timeoutConfiguration =
        new ImmutableMap.Builder<String, Integer>()
            .put(INTERNAL_WS_CONNECT_TIMEOUT, builder.socketTimeoutMillis)
            .put(WS_CONNECT_TIMEOUT, builder.socketTimeoutMillis)
            .put(INTERNAL_WS_REQUEST_TIMEOUT, builder.readTimeoutMillis)
            .put(WS_REQUEST_TIMEOUT, builder.readTimeoutMillis)
            .build();
  }

  /**
   * Set authentication cookies, User-Agent header and request timeout configuration
   *
   * @param port {@link BindingProvider} to process
   */
  void addContext(BindingProvider port) {
    checkNotNull(port);
    addRequestHeaders(port);
    port.getRequestContext().putAll(timeoutConfiguration);
  }

  void addContext(HttpURLConnection connection, boolean isWhiteListed) {
    checkNotNull(connection);
    // Add forms authentication cookies or disable forms authentication
    List<String> authenticationCookies =
        (authenticationHandler == null || !isWhiteListed)
            ? Collections.emptyList()
            : authenticationHandler.getAuthenticationCookies();
    if (authenticationCookies.isEmpty()) {
      // To access a SharePoint site that uses multiple authentication
      // providers by using a set of Windows credentials, need to add
      // "X-FORMS_BASED_AUTH_ACCEPTED" request header to web service request
      // and set its value to "f"
      // http://msdn.microsoft.com/en-us/library/hh124553(v=office.14).aspx
      connection.addRequestProperty(HEADER_X_FORMS_BASED_AUTH_ACCEPTED, "f");
    } else {
      authenticationCookies.forEach(c -> connection.addRequestProperty("Cookie", c));
    }
    // Set User-Agent value
    if (!"".equals(userAgent)) {
      connection.addRequestProperty("User-Agent", userAgent);
    }
    connection.setReadTimeout(readTimeoutMillis);
    connection.setConnectTimeout(socketTimeoutMillis);
  }

  private void addRequestHeaders(BindingProvider port) {
    Map<String, List<String>> headers = new HashMap<String, List<String>>();
    // Add forms authentication cookies or disable forms authentication
    List<String> authenticationCookies =
        authenticationHandler == null
            ? Collections.emptyList()
            : authenticationHandler.getAuthenticationCookies();
    if (authenticationCookies.isEmpty()) {
      // To access a SharePoint site that uses multiple authentication
      // providers by using a set of Windows credentials, need to add
      // "X-FORMS_BASED_AUTH_ACCEPTED" request header to web service request
      // and set its value to "f"
      // http://msdn.microsoft.com/en-us/library/hh124553(v=office.14).aspx
      headers.put(HEADER_X_FORMS_BASED_AUTH_ACCEPTED, Collections.singletonList("f"));
    } else {
      headers.put("Cookie", authenticationCookies);
    }

    // Set User-Agent value
    if (!"".equals(userAgent)) {
      headers.put("User-Agent", Collections.singletonList(userAgent));
    }

    // Set request headers
    port.getRequestContext().put(MessageContext.HTTP_REQUEST_HEADERS, headers);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SharePointRequestContext that = (SharePointRequestContext) o;
    return socketTimeoutMillis == that.socketTimeoutMillis
        && readTimeoutMillis == that.readTimeoutMillis
        && Objects.equals(authenticationHandler, that.authenticationHandler)
        && Objects.equals(userAgent, that.userAgent)
        && Objects.equals(timeoutConfiguration, that.timeoutConfiguration);
  }

  @Override
  public int hashCode() {
    return Objects.hash(authenticationHandler, userAgent, timeoutConfiguration, socketTimeoutMillis,
        readTimeoutMillis);
  }

  public static final class Builder {
    private FormsAuthenticationHandler authenticationHandler;
    private String userAgent = "";
    private int socketTimeoutMillis = 30000; // 30 seconds
    private int readTimeoutMillis = 180000; // 3 min

    public Builder() {
    }

    public Builder setAuthenticationHandler(FormsAuthenticationHandler authenticationHandler) {
      this.authenticationHandler = authenticationHandler;
      return this;
    }

    public Builder setUserAgent(String userAgent) {
      this.userAgent = userAgent;
      return this;
    }

    public Builder setSocketTimeoutMillis(int socketTimeoutMillis) {
      this.socketTimeoutMillis = socketTimeoutMillis;
      return this;
    }

    public Builder setReadTimeoutMillis(int readTimeoutMillis) {
      this.readTimeoutMillis = readTimeoutMillis;
      return this;
    }

    public SharePointRequestContext build() {
      return new SharePointRequestContext(this);
    }
  }
}
