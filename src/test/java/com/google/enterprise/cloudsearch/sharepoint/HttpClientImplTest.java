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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.enterprise.cloudsearch.sharepoint.HttpClientImpl.ConnectionFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HttpClientImplTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Mock ConnectionFactory connectionFactory;
  @Mock SharePointRequestContext requestContext;
  @Mock InputStream contentStream;
  @Mock InputStream errorStream;

  @Test
  public void testBuilderNullRequestContext() {
    thrown.expect(NullPointerException.class);
    new HttpClientImpl.Builder().setConnectionFactory(connectionFactory).build();
  }

  @Test
  public void testBuilderNullConnectionFactory() {
    thrown.expect(NullPointerException.class);
    new HttpClientImpl.Builder()
        .setSharePointRequestContext(requestContext)
        .setConnectionFactory(null)
        .build();
  }

  @Test
  public void testBuilder() {
    new HttpClientImpl.Builder()
        .setSharePointRequestContext(requestContext)
        .setConnectionFactory(connectionFactory)
        .build();
  }

  @Test
  public void testIssueGetRequest() throws IOException {
    URL url = new URL("http://sp.com");
    HttpClient client =
        new HttpClientImpl.Builder()
            .setSharePointRequestContext(requestContext)
            .setConnectionFactory(connectionFactory)
            .build();
    HttpURLConnection connection =
        setUpConnection(
            200,
            contentStream,
            Collections.singletonList(new Pair("Some-Header", "Some-Value")),
            null);
    when(connectionFactory.getConnection(url)).thenReturn(connection);
    FileInfo fileInfo = client.issueGetRequest(url);
    assertEquals(
        Collections.singletonList(new FileInfo.FileHeader("Some-Header", "Some-Value")),
        fileInfo.getHeaders());
    assertEquals(contentStream, fileInfo.getContents());
    InOrder inOrder = inOrder(connectionFactory, requestContext, connection);
    verifyConnectionSetup(url, connection, inOrder, true);
    inOrder.verify(connection).getHeaderField(HttpClientImpl.HTTP_SHAREPOINT_ERROR_HEADER);
    inOrder.verify(connection).getHeaderFieldKey(1);
    inOrder.verify(connection).getHeaderField(1);
    inOrder.verify(connection).getHeaderFieldKey(2);
    inOrder.verify(connection).getInputStream();
    verifyNoMoreInteractions(connectionFactory, requestContext, connection);
  }

  @Test
  public void testIssueGetRequestSharePointError() throws IOException {
    URL url = new URL("http://sp.com");
    HttpClient client =
        new HttpClientImpl.Builder()
            .setSharePointRequestContext(requestContext)
            .setConnectionFactory(connectionFactory)
            .build();
    HttpURLConnection connection = setUpConnection(200, null, Collections.emptyList(), null);
    when(connectionFactory.getConnection(url)).thenReturn(connection);
    when(connection.getHeaderField(HttpClientImpl.HTTP_SHAREPOINT_ERROR_HEADER)).thenReturn("0");
    try {
      client.issueGetRequest(url);
      fail("missing IO exception");
    } catch (IOException e) {
      // expected IO exception.
    }
    InOrder inOrder = inOrder(connectionFactory, requestContext, connection);
    verifyConnectionSetup(url, connection, inOrder, true);
    inOrder.verify(connection).getHeaderField(HttpClientImpl.HTTP_SHAREPOINT_ERROR_HEADER);
    inOrder.verify(connection).getInputStream();
    verifyNoMoreInteractions(connectionFactory, requestContext, connection);
  }

  @Test
  public void testIssueGetRequestSharePointError2() throws IOException {
    URL url = new URL("http://sp.com");
    HttpClient client =
        new HttpClientImpl.Builder()
            .setSharePointRequestContext(requestContext)
            .setConnectionFactory(connectionFactory)
            .build();
    HttpURLConnection connection = setUpConnection(200, null, Collections.emptyList(), null);
    when(connectionFactory.getConnection(url)).thenReturn(connection);
    when(connection.getHeaderField(HttpClientImpl.HTTP_SHAREPOINT_ERROR_HEADER)).thenReturn("2");
    try {
      client.issueGetRequest(url);
      fail("missing IO exception");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Got error 2 from SharePoint"));
    }
    InOrder inOrder = inOrder(connectionFactory, requestContext, connection);
    verifyConnectionSetup(url, connection, inOrder, true);
    inOrder.verify(connection).getHeaderField(HttpClientImpl.HTTP_SHAREPOINT_ERROR_HEADER);
    inOrder.verify(connection).getInputStream();
    verifyNoMoreInteractions(connectionFactory, requestContext, connection);
  }

  @Test
  public void testIssueGetRequestRedirect() throws IOException {
    URL url = new URL("http://sp.com");
    HttpClient client =
        new HttpClientImpl.Builder()
            .setSharePointRequestContext(requestContext)
            .setConnectionFactory(connectionFactory)
            .setPerformBrowserLeniency(true)
            .build();
    HttpURLConnection connection =
        setUpConnection(
            302,
            contentStream,
            Collections.singletonList(new Pair("Some-Header", "Some-Value")),
            null);
    when(connection.getHeaderField(HttpClientImpl.HTTP_REDIRECT_LOCATION_HEADER))
        .thenReturn("http://sp.com/home");
    when(connectionFactory.getConnection(url)).thenReturn(connection);
    URL redirectUrl = new URL("http://sp.com/home");
    HttpURLConnection redirectConnection =
        setUpConnection(
            200,
            contentStream,
            Collections.singletonList(new Pair("Some-Header", "Some-Value")),
            null);
    when(connectionFactory.getConnection(redirectUrl)).thenReturn(redirectConnection);
    FileInfo fileInfo = client.issueGetRequest(url);
    assertEquals(
        Collections.singletonList(new FileInfo.FileHeader("Some-Header", "Some-Value")),
        fileInfo.getHeaders());
    assertEquals(contentStream, fileInfo.getContents());
    InOrder inOrder =
        inOrder(connectionFactory, requestContext, connection, contentStream, redirectConnection);
    verifyConnectionSetup(url, connection, inOrder, false);
    inOrder.verify(connection).getHeaderField(HttpClientImpl.HTTP_REDIRECT_LOCATION_HEADER);
    inOrder.verify(connection).getInputStream();
    inOrder.verify(contentStream).close();
    verifyConnectionSetup(redirectUrl, redirectConnection, inOrder, false);
    inOrder.verify(redirectConnection).getHeaderField(HttpClientImpl.HTTP_SHAREPOINT_ERROR_HEADER);
    inOrder.verify(redirectConnection).getHeaderFieldKey(1);
    inOrder.verify(redirectConnection).getHeaderField(1);
    inOrder.verify(redirectConnection).getHeaderFieldKey(2);
    inOrder.verify(redirectConnection).getInputStream();
    verifyNoMoreInteractions(
        connectionFactory, requestContext, contentStream, connection, redirectConnection);
  }

  @Test
  public void testIssueGetRequestDoNotFollow() throws IOException {
    URL url = new URL("http://sp.com");
    HttpClient client =
        new HttpClientImpl.Builder()
            .setSharePointRequestContext(requestContext)
            .setConnectionFactory(connectionFactory)
            .build();
    HttpURLConnection connection = setUpConnection(302, null, Collections.emptyList(), null);
    when(connectionFactory.getConnection(url)).thenReturn(connection);
    try {
      client.issueGetRequest(url);
      fail("missing IO exception");
    } catch (IOException e) {
      // expected IO exception.
    }
    InOrder inOrder = inOrder(connectionFactory, requestContext, connection);
    verifyConnectionSetup(url, connection, inOrder, true);
    inOrder.verify(connection).getInputStream();
    verifyNoMoreInteractions(connectionFactory, requestContext, connection);
  }

  @Test
  public void testIssueGetRequestInvalidLocation() throws IOException {
    URL url = new URL("http://sp.com");
    HttpClient client =
        new HttpClientImpl.Builder()
            .setSharePointRequestContext(requestContext)
            .setConnectionFactory(connectionFactory)
            .setPerformBrowserLeniency(true)
            .build();
    HttpURLConnection connection =
        setUpConnection(
            302,
            contentStream,
            Collections.singletonList(new Pair("Some-Header", "Some-Value")),
            null);
    when(connection.getHeaderField(HttpClientImpl.HTTP_REDIRECT_LOCATION_HEADER)).thenReturn("abc");
    when(connectionFactory.getConnection(url)).thenReturn(connection);
    try {
      client.issueGetRequest(url);
      fail("missing IO exception");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Invalid redirection url"));
    }
    InOrder inOrder = inOrder(connectionFactory, requestContext, connection);
    verifyConnectionSetup(url, connection, inOrder, false);
    inOrder.verify(connection).getHeaderField(HttpClientImpl.HTTP_REDIRECT_LOCATION_HEADER);
    inOrder.verify(connection).getInputStream();
    verifyNoMoreInteractions(connectionFactory, requestContext, connection);
  }

  @Test
  public void testIssueGetRequestMissingLocation() throws IOException {
    URL url = new URL("http://sp.com");
    HttpClient client =
        new HttpClientImpl.Builder()
            .setSharePointRequestContext(requestContext)
            .setConnectionFactory(connectionFactory)
            .setPerformBrowserLeniency(true)
            .build();
    HttpURLConnection connection =
        setUpConnection(
            302,
            contentStream,
            Collections.singletonList(new Pair("Some-Header", "Some-Value")),
            null);
    when(connection.getHeaderField(HttpClientImpl.HTTP_REDIRECT_LOCATION_HEADER)).thenReturn(null);
    when(connectionFactory.getConnection(url)).thenReturn(connection);
    try {
      client.issueGetRequest(url);
      fail("missing IO exception");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("No redirect location available"));
    }
    InOrder inOrder = inOrder(connectionFactory, requestContext, connection);
    verifyConnectionSetup(url, connection, inOrder, false);
    inOrder.verify(connection).getHeaderField(HttpClientImpl.HTTP_REDIRECT_LOCATION_HEADER);
    inOrder.verify(connection).getInputStream();
    verifyNoMoreInteractions(connectionFactory, requestContext, connection);
  }

  @Test
  public void testIssueGetRequestFollowNonRedirectError() throws IOException {
    URL url = new URL("http://sp.com");
    HttpClient client =
        new HttpClientImpl.Builder()
            .setSharePointRequestContext(requestContext)
            .setConnectionFactory(connectionFactory)
            .setPerformBrowserLeniency(true)
            .build();
    HttpURLConnection connection = setUpConnection(500, null, Collections.emptyList(), errorStream);
    when(connectionFactory.getConnection(url)).thenReturn(connection);
    try {
      client.issueGetRequest(url);
      fail("missing IO exception");
    } catch (IOException e) {
      // expected IO exception.
    }
    InOrder inOrder = inOrder(connectionFactory, requestContext, connection, errorStream);
    verifyConnectionSetup(url, connection, inOrder, false);
    inOrder.verify(connection).getErrorStream();
    inOrder.verify(errorStream).close();
    verifyNoMoreInteractions(connectionFactory, requestContext, connection, errorStream);
  }

  @Test
  public void testIssueGetRequestFollowNonRedirectErrorNullErrorStream() throws IOException {
    URL url = new URL("http://sp.com");
    HttpClient client =
        new HttpClientImpl.Builder()
            .setSharePointRequestContext(requestContext)
            .setConnectionFactory(connectionFactory)
            .setPerformBrowserLeniency(true)
            .build();
    HttpURLConnection connection = setUpConnection(401, null, Collections.emptyList(), null);
    when(connectionFactory.getConnection(url)).thenReturn(connection);
    try {
      client.issueGetRequest(url);
      fail("missing IO exception");
    } catch (IOException e) {
      // expected IO exception.
    }
    InOrder inOrder = inOrder(connectionFactory, requestContext, connection);
    verifyConnectionSetup(url, connection, inOrder, false);
    inOrder.verify(connection).getErrorStream();
    verifyNoMoreInteractions(connectionFactory, requestContext, connection);
  }

  @Test
  public void testIssueGetRequestNotFound() throws IOException {
    URL url = new URL("http://sp.com");
    HttpClient client =
        new HttpClientImpl.Builder()
            .setSharePointRequestContext(requestContext)
            .setConnectionFactory(connectionFactory)
            .build();
    HttpURLConnection connection = setUpConnection(404, null, Collections.emptyList(), null);
    when(connectionFactory.getConnection(url)).thenReturn(connection);
    assertEquals(null, client.issueGetRequest(url));
    InOrder inOrder = inOrder(connectionFactory, requestContext, connection);
    verifyConnectionSetup(url, connection, inOrder, true);
    verifyNoMoreInteractions(connectionFactory, requestContext, connection);
  }

  @Test
  public void testGetRedirectLocation() throws IOException {
    URL url = new URL("http://sp.com");
    HttpClient client =
        new HttpClientImpl.Builder()
            .setSharePointRequestContext(requestContext)
            .setConnectionFactory(connectionFactory)
            .setPerformBrowserLeniency(true)
            .build();
    HttpURLConnection connection =
        setUpConnection(
            302,
            contentStream,
            Collections.singletonList(new Pair("Some-Header", "Some-Value")),
            null);
    when(connection.getHeaderField(HttpClientImpl.HTTP_REDIRECT_LOCATION_HEADER))
        .thenReturn("http://sp.com/redirect");
    when(connectionFactory.getConnection(url)).thenReturn(connection);
    assertEquals("http://sp.com/redirect", client.getRedirectLocation(url));
    InOrder inOrder = inOrder(connectionFactory, requestContext, connection, contentStream);
    verifyConnectionSetup(url, connection, inOrder, false);
    inOrder.verify(connection).getHeaderField(HttpClientImpl.HTTP_REDIRECT_LOCATION_HEADER);
    inOrder.verify(connection).getInputStream();
    inOrder.verify(contentStream).close();
    verifyNoMoreInteractions(connectionFactory, requestContext, connection, contentStream);
  }

  @Test
  public void testGetRedirectLocationNo302() throws IOException {
    URL url = new URL("http://sp.com");
    HttpClient client =
        new HttpClientImpl.Builder()
            .setSharePointRequestContext(requestContext)
            .setConnectionFactory(connectionFactory)
            .setPerformBrowserLeniency(true)
            .build();
    HttpURLConnection connection =
        setUpConnection(
            200,
            contentStream,
            Collections.singletonList(new Pair("Some-Header", "Some-Value")),
            null);
    when(connectionFactory.getConnection(url)).thenReturn(connection);
    assertEquals(null, client.getRedirectLocation(url));
    InOrder inOrder = inOrder(connectionFactory, requestContext, connection, contentStream);
    verifyConnectionSetup(url, connection, inOrder, false);
    inOrder.verify(connection).getInputStream();
    inOrder.verify(contentStream).close();
    verifyNoMoreInteractions(connectionFactory, requestContext, connection, contentStream);
  }

  @Test
  public void testGetRedirectLocationErrorStream() throws IOException {
    URL url = new URL("http://sp.com");
    HttpClient client =
        new HttpClientImpl.Builder()
            .setSharePointRequestContext(requestContext)
            .setConnectionFactory(connectionFactory)
            .setPerformBrowserLeniency(true)
            .build();
    HttpURLConnection connection =
        setUpConnection(
            500,
            null,
            Collections.singletonList(new Pair("Some-Header", "Some-Value")),
            errorStream);
    when(connectionFactory.getConnection(url)).thenReturn(connection);
    doAnswer(
            invocation -> {
              throw new IOException("Ignore error closing stream");
            })
        .when(errorStream)
        .close();
    assertEquals(null, client.getRedirectLocation(url));
    InOrder inOrder = inOrder(connectionFactory, requestContext, connection, errorStream);
    verifyConnectionSetup(url, connection, inOrder, false);
    inOrder.verify(connection).getErrorStream();
    inOrder.verify(errorStream).close();
    verifyNoMoreInteractions(connectionFactory, requestContext, connection, errorStream);
  }

  private void verifyConnectionSetup(
      URL url, HttpURLConnection connection, InOrder inOrder, boolean followRedirects)
      throws IOException {
    inOrder.verify(connectionFactory).getConnection(url);
    inOrder.verify(requestContext).addContext(connection, true);
    inOrder.verify(connection).setDoInput(true);
    inOrder.verify(connection).setDoOutput(false);
    inOrder.verify(connection).setInstanceFollowRedirects(followRedirects);
    inOrder.verify(connection).getResponseCode();
  }

  private HttpURLConnection setUpConnection(
      int responseCode, InputStream content, List<Pair> headers, InputStream error)
      throws IOException {
    HttpURLConnection connection = Mockito.mock(HttpURLConnection.class);
    when(connection.getResponseCode()).thenReturn(responseCode);
    if (content != null) {
      when(connection.getInputStream()).thenReturn(content);
    }
    if (headers != null) {
      for (int i = 0; i < headers.size(); i++) {
        when(connection.getHeaderFieldKey(i + 1)).thenReturn(headers.get(i).key);
        when(connection.getHeaderField(i + 1)).thenReturn(headers.get(i).value);
      }
    }
    if (error != null) {
      when(connection.getErrorStream()).thenReturn(error);
    }
    return connection;
  }

  private static class Pair {
    final String key;
    final String value;

    Pair(String key, String value) {
      this.key = key;
      this.value = value;
    }
  }
}
