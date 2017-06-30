package com.google.enterprise.cloud.search.sharepoint;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.ws.BindingProvider;
import javax.xml.ws.handler.MessageContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SharePointRequestContextTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Mock FormsAuthenticationHandler authenticationHandler;
  @Mock BindingProvider port;
  @Mock HttpURLConnection connection;
  
  static final Map<String, Object> DEFAULT_TIMOUT_CONTEXT =
      new ImmutableMap.Builder<String, Object>()
          .put(SharePointRequestContext.INTERNAL_WS_CONNECT_TIMEOUT, 30000)
          .put(SharePointRequestContext.WS_CONNECT_TIMEOUT, 30000)
          .put(SharePointRequestContext.INTERNAL_WS_REQUEST_TIMEOUT, 180000)
          .put(SharePointRequestContext.WS_REQUEST_TIMEOUT, 180000)
          .build();
  static Map<String, List<String>> DEFAULT_AUTH_HEADER =
      new ImmutableMap.Builder<String, List<String>>()
          .put(
              SharePointRequestContext.HEADER_X_FORMS_BASED_AUTH_ACCEPTED,
              Collections.singletonList("f"))
          .build();
  
  @Test
  public void testEmptyBuilder() {
    new SharePointRequestContext.Builder().build();
  }
  
  @Test
  public void testAddContextBindingProvider() {
    SharePointRequestContext requestContext = new SharePointRequestContext.Builder().build();
    Map<String, Object> requestContextMap = new HashMap<String, Object>();
    when(port.getRequestContext()).thenReturn(requestContextMap);
    requestContext.addContext(port);
    Map<String, Object> expected = new HashMap<String, Object>();
    expected.putAll(DEFAULT_TIMOUT_CONTEXT);
    expected.put(MessageContext.HTTP_REQUEST_HEADERS, DEFAULT_AUTH_HEADER);
    assertEquals(expected, requestContextMap);
  }
  
  @Test
  public void testAddContextBindingProviderUserAgent() {
    SharePointRequestContext requestContext =
        new SharePointRequestContext.Builder().setUserAgent("Unit-Test").build();
    Map<String, Object> requestContextMap = new HashMap<String, Object>();
    when(port.getRequestContext()).thenReturn(requestContextMap);
    requestContext.addContext(port);
    Map<String, Object> expected = new HashMap<String, Object>();
    expected.putAll(DEFAULT_TIMOUT_CONTEXT);
    Map<String, List<String>> authHeaders = new HashMap<>();
    authHeaders.putAll(DEFAULT_AUTH_HEADER);
    authHeaders.put("User-Agent", Collections.singletonList("Unit-Test"));
    expected.put(MessageContext.HTTP_REQUEST_HEADERS, authHeaders);
    assertEquals(expected, requestContextMap);
  }

  @Test
  public void testAddContextBindingProviderAuthCookies() {
    List<String> cookies = Arrays.asList("c1", "c2");
    when(authenticationHandler.getAuthenticationCookies()).thenReturn(cookies);
    SharePointRequestContext requestContext =
        new SharePointRequestContext.Builder()
            .setAuthenticationHandler(authenticationHandler)
            .setUserAgent("Unit-Test")
            .build();
    Map<String, Object> requestContextMap = new HashMap<String, Object>();
    when(port.getRequestContext()).thenReturn(requestContextMap);
    requestContext.addContext(port);
    Map<String, Object> expected = new HashMap<String, Object>();
    expected.putAll(DEFAULT_TIMOUT_CONTEXT);
    Map<String, List<String>> authHeaders = new HashMap<>();
    authHeaders.put("User-Agent", Collections.singletonList("Unit-Test"));
    authHeaders.put("Cookie", cookies);
    expected.put(MessageContext.HTTP_REQUEST_HEADERS, authHeaders);
    assertEquals(expected, requestContextMap);
  }
  
  @Test
  public void testAddContextConnection() {
    SharePointRequestContext requestContext = new SharePointRequestContext.Builder().build();
    requestContext.addContext(connection, true);
    InOrder inOrder = inOrder(connection);
    inOrder
        .verify(connection)
        .addRequestProperty(SharePointRequestContext.HEADER_X_FORMS_BASED_AUTH_ACCEPTED, "f");
    inOrder.verify(connection).setReadTimeout(180000);
    inOrder.verify(connection).setConnectTimeout(30000);
    verifyNoMoreInteractions(connection);
  }

  @Test
  public void testAddContextConnectionUserAgent() {
    SharePointRequestContext requestContext =
        new SharePointRequestContext.Builder().setUserAgent("Unit-Test").build();
    requestContext.addContext(connection, true);
    InOrder inOrder = inOrder(connection);
    inOrder
        .verify(connection)
        .addRequestProperty(SharePointRequestContext.HEADER_X_FORMS_BASED_AUTH_ACCEPTED, "f");
    inOrder.verify(connection).addRequestProperty("User-Agent", "Unit-Test");
    inOrder.verify(connection).setReadTimeout(180000);
    inOrder.verify(connection).setConnectTimeout(30000);
    verifyNoMoreInteractions(connection);
  }

  @Test
  public void testAddContextConnectionAuthCookies() {
    List<String> cookies = Arrays.asList("c1", "c2");
    when(authenticationHandler.getAuthenticationCookies()).thenReturn(cookies);
    SharePointRequestContext requestContext =
        new SharePointRequestContext.Builder()
            .setAuthenticationHandler(authenticationHandler)
            .setUserAgent("Unit-Test")
            .build();
    requestContext.addContext(connection, true);
    InOrder inOrder = inOrder(authenticationHandler, connection);
    inOrder.verify(authenticationHandler).getAuthenticationCookies();
    inOrder.verify(connection).addRequestProperty("Cookie", "c1");
    inOrder.verify(connection).addRequestProperty("Cookie", "c2");
    inOrder.verify(connection).addRequestProperty("User-Agent", "Unit-Test");
    inOrder.verify(connection).setReadTimeout(180000);
    inOrder.verify(connection).setConnectTimeout(30000);
    verifyNoMoreInteractions(authenticationHandler, connection);
  }
  
  @Test
  public void testAddContextConnectionNotWhiteListed() {
    SharePointRequestContext requestContext =
        new SharePointRequestContext.Builder()
            .setAuthenticationHandler(authenticationHandler)
            .build();
    requestContext.addContext(connection, false);
    InOrder inOrder = inOrder(connection);
    inOrder
        .verify(connection)
        .addRequestProperty(SharePointRequestContext.HEADER_X_FORMS_BASED_AUTH_ACCEPTED, "f");
    inOrder.verify(connection).setReadTimeout(180000);
    inOrder.verify(connection).setConnectTimeout(30000);
    verifyNoMoreInteractions(connection, authenticationHandler);
  }
  
  @Test
  public void testAddContextConnectionNonDefaultTimeOut() {
    SharePointRequestContext requestContext =
        new SharePointRequestContext.Builder()
            .setReadTimeoutMillis(100)
            .setSocketTimeoutMillis(20)
            .build();
    requestContext.addContext(connection, true);
    InOrder inOrder = inOrder(connection);
    inOrder
        .verify(connection)
        .addRequestProperty(SharePointRequestContext.HEADER_X_FORMS_BASED_AUTH_ACCEPTED, "f");
    inOrder.verify(connection).setReadTimeout(100);
    inOrder.verify(connection).setConnectTimeout(20);
    verifyNoMoreInteractions(connection);
  }
}
