package com.google.enterprise.cloudsearch.sharepoint;

import java.io.IOException;
import java.net.URL;

interface HttpClient {
  /**
   * Download file content and related headers
   *
   * @param url to download
   * @return {@link: FileInfo} file info wrt url
   * @throws IOException
   */
  FileInfo issueGetRequest(URL url) throws IOException;

  /**
   * Return redirect location for input URL
   *
   * @param url
   * @return redirect location for input URL
   * @throws IOException
   */
  String getRedirectLocation(URL url) throws IOException;
}
