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
