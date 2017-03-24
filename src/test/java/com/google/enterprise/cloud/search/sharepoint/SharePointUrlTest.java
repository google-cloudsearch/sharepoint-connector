package com.google.enterprise.cloud.search.sharepoint;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SharePointUrlTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testBuilderBrowserLeniency() throws URISyntaxException, MalformedURLException {
    String docUrl = "http://sp.com/shared documents?param=value";
    SharePointUrl url = new SharePointUrl.Builder(docUrl).setPerformBrowserLeniency(true).build();
    assertEquals(new URI("http://sp.com/shared%20documents?param=value"), url.getURI());
    assertEquals(new URL("http://sp.com/shared%20documents?param=value"), url.toURL());
    assertEquals(docUrl, url.getUrl());
    assertEquals("http://sp.com", url.getRootUrl());
  }

  @Test
  public void testBuilderNonBrowserLeniency() throws URISyntaxException, MalformedURLException {
    String docUrl = "http://sp.com/shared documents";
    SharePointUrl url = new SharePointUrl.Builder(docUrl).setPerformBrowserLeniency(false).build();
    assertEquals(new URI("http://sp.com/shared%20documents"), url.getURI());
    assertEquals(new URL("http://sp.com/shared%20documents"), url.toURL());
  }

  @Test
  public void testBuilderNonBrowserLeniencyQueryParam() throws URISyntaxException, MalformedURLException {
    String docUrl = "http://sp.com/shared documents?param=value";
    SharePointUrl url = new SharePointUrl.Builder(docUrl).setPerformBrowserLeniency(false).build();
    assertEquals(new URI("http://sp.com/shared%20documents%3Fparam=value"), url.getURI());
    assertEquals(new URL("http://sp.com/shared%20documents%3Fparam=value"), url.toURL());
  }

  @Test
  public void testInvalidUrl() throws URISyntaxException {
    thrown.expect(IllegalArgumentException.class);
    SharePointUrl.escape("abc");
  }

  @Test
  public void testBuilderBrowserLeniencyNoParam() throws URISyntaxException, MalformedURLException {
    String docUrl = "https://sp.google.com/shared documents/folder";
    SharePointUrl url = new SharePointUrl.Builder(docUrl).setPerformBrowserLeniency(true).build();
    assertEquals(new URI("https://sp.google.com/shared%20documents/folder"), url.getURI());
    assertEquals(new URL("https://sp.google.com/shared%20documents/folder"), url.toURL());
    assertEquals("https://sp.google.com", url.getRootUrl());
  }

  @Test
  public void testBuilderBrowserLeniencyTrailingSlash() throws URISyntaxException, MalformedURLException {
    String docUrl = "https://sp.google.com/shared documents/folder/";
    SharePointUrl url = new SharePointUrl.Builder(docUrl).setPerformBrowserLeniency(true).build();
    assertEquals(new URI("https://sp.google.com/shared%20documents/folder"), url.getURI());
    assertEquals(new URL("https://sp.google.com/shared%20documents/folder"), url.toURL());
    assertEquals("https://sp.google.com", url.getRootUrl());
    assertEquals("https://sp.google.com/shared documents/folder", url.getUrl());
  }

  @Test
  public void testEquals() throws URISyntaxException, MalformedURLException {
    SharePointUrl url1 = new SharePointUrl.Builder("http://abc.com").build();
    SharePointUrl url2 = new SharePointUrl.Builder("http://abc.com")
        .setPerformBrowserLeniency(true).build();
    assertEquals(url1, url2);
    assertEquals(url1.hashCode(), url2.hashCode());

    SharePointUrl urlTrailingSlash = new SharePointUrl.Builder("http://abc.com/")
        .setPerformBrowserLeniency(true).build();
    assertEquals(url1, urlTrailingSlash);
    assertEquals(url1.hashCode(), urlTrailingSlash.hashCode());

    SharePointUrl urlNoLeniency = new SharePointUrl.Builder("http://abc.com")
        .setPerformBrowserLeniency(false).build();
    assertFalse(url1.equals(urlNoLeniency));
  }


}
