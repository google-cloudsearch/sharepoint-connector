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

import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.repackaged.com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/** Utility object to filter out unwanted HTML tags. */
class HtmlContentFilter {
  /**
   * SharePoint includes warning message regarding javascript within "noscript" tags. As well as
   * certain sections of page such as navigation are marked with css class "noIndex".
   */
  private static final ImmutableList<String> DEFAULT_HTML_FILTERS =
      ImmutableList.of("noscript", "div.noIndex", "script");

  private static final Charset CHARSET = StandardCharsets.UTF_8;
  private static final String HTML_CONTENT_FILTER_CONFIG = "htmlContent.filters";

  private final ImmutableList<String> filters;

  HtmlContentFilter(List<String> filters) {
    this.filters = ImmutableList.copyOf(filters);
  }

  ImmutableList<String> getFilters() {
    return filters;
  }

  static HtmlContentFilter fromConfiguration() {
    checkState(Configuration.isInitialized(), "Configuration not initialized yet");
    return new HtmlContentFilter(
        Configuration.getMultiValue(
                HTML_CONTENT_FILTER_CONFIG,
                DEFAULT_HTML_FILTERS,
                Configuration.STRING_PARSER)
            .get());
  }

  AbstractInputStreamContent getParsedHtmlContent(
      InputStream content, String baseUrl, String contentType) throws IOException {
    Document html = Jsoup.parse(content, CHARSET.name(), baseUrl);
    filters.stream().forEach(filter -> html.select(filter).remove());
    return ByteArrayContent.fromString(contentType, html.outerHtml());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(filters);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof HtmlContentFilter)) {
      return false;
    }
    HtmlContentFilter other = (HtmlContentFilter) obj;
    return Objects.equal(filters, other.filters);
  }
}
