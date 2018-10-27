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
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.api.client.http.AbstractInputStreamContent;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for {@link HtmlContentFilter} */
public class HtmlContentFilterTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Test
  public void testConstructorNullFilterList() {
    thrown.expect(NullPointerException.class);
    new HtmlContentFilter(null);
  }

  @Test
  public void testConstructorEmptyList() {
    HtmlContentFilter filter = new HtmlContentFilter(ImmutableList.of());
    assertEquals(ImmutableList.of(), filter.getFilters());
  }

  @Test
  public void testConfigurationNotInitialized() {
    thrown.expect(IllegalStateException.class);
    HtmlContentFilter.fromConfiguration();
  }

  @Test
  public void testFromConfiguration() {
    Properties config = new Properties();
    config.put("htmlContent.filters", "f1,f2");
    Configuration.initConfig(config);
    HtmlContentFilter filter = HtmlContentFilter.fromConfiguration();
    assertEquals(ImmutableList.of("f1", "f2"), filter.getFilters());
  }

  @Test
  public void testFromConfigurationWithDefaults() throws IOException {
    Configuration.initConfig(new Properties());
    HtmlContentFilter filter = HtmlContentFilter.fromConfiguration();
    String html =
        "<html><noscript>javascript not available</noscript>"
            + "should be present<div class='bar noIndex foo'>"
            + "navigation</div>noIndex as text</html>";
    ByteArrayInputStream content = new ByteArrayInputStream(html.getBytes(UTF_8));

    AbstractInputStreamContent filteredContent =
        filter.getParsedHtmlContent(content, "http://google.com", "text/html");
    byte[] bytes = ByteStreams.toByteArray(filteredContent.getInputStream());
    String output = new String(bytes);
    // We are validating missing and expected values. Jsoup formats output HTML so exact string
    // matching for entire output HTML might not work.
    assertThat(
        output, allOf(containsString("should be present"), containsString("noIndex as text")));
    assertThat(
        output,
        allOf(
            not(containsString("<noscript>")),
            not(containsString("javascript not available")),
            not(containsString("navigation"))));
  }

  @Test
  public void testWithNoFiltering() throws IOException {
    HtmlContentFilter filter = new HtmlContentFilter(ImmutableList.of());
    String html =
        "<html><noscript>javascript not available</noscript>"
            + "should be present<div class='bar noIndex foo'>"
            + "navigation</div>noIndex as text</html>";
    ByteArrayInputStream content = new ByteArrayInputStream(html.getBytes(UTF_8));

    AbstractInputStreamContent filteredContent =
        filter.getParsedHtmlContent(content, "http://google.com", "text/html");
    byte[] bytes = ByteStreams.toByteArray(filteredContent.getInputStream());
    String output = new String(bytes);
    // We are validating expected values. Jsoup formats output HTML so exact string
    // matching for entire output HTML might not work.
    assertThat(
        output,
        allOf(
            containsString("should be present"),
            containsString("<noscript>"),
            containsString("bar noIndex foo"),
            containsString("javascript not available"),
            containsString("noIndex as text")));
  }
}
