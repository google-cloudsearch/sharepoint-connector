package com.google.enterprise.cloudsearch.sharepoint;

interface HttpClientFactory {
  /**
   * Creates an instance of {@link HttpClient}
   *
   * @return {@link HttpClient}
   */
  HttpClient getInstance();
}
