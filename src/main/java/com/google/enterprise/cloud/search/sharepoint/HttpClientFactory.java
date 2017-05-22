package com.google.enterprise.cloud.search.sharepoint;

interface HttpClientFactory {
  /**
   * Creates an instance of {@link HttpClient}
   *
   * @return {@link HttpClient}
   */
  HttpClient getInstance();
}
