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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class SharePointResponseHelper {

  public static String getSiteCollectionResponse() {
    return loadTestResponse("sites-SiteCollection-sc.xml");
  }

  public static String getWebResponse() {
    return loadTestResponse("sites-SiteCollection-s.xml");
  }

  public static String getListResponse() {
    return loadTestResponse("sites-SiteCollection-Lists-CustomList-l.xml");
  }

  public static String getListItemResponse() {
    return loadTestResponse("sites-SiteCollection-Lists-CustomList-2-li.xml");
  }

  public static String getListRootFolderContentResponse() {
    return loadTestResponse("sites-SiteCollection-Lists-CustomList-f.xml");
  }

  public static String getChangesForSiteCollection() {
    return loadTestResponse("testModifiedGetDocIdsClient.changes-sc.xml");
  }

  public static String getChangesSitePermissionsChange() {
    return loadTestResponse("testModifiedSitePermissions.changes-sc.xml");
  }

  public static String getChangesForcontentDB() {
    return loadTestResponse("testModifiedGetDocIdsClient.changes-cd.xml");
  }

  public static String loadTestResponse(String fileName) {
    try {
      return loadResourceAsString("spresponses/" + fileName);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static String loadResourceAsString(String resource) throws IOException {
    return readInputStreamToString(SiteConnectorTest.class.getResourceAsStream(resource));
  }

  private static String readInputStreamToString(InputStream inputStream) throws IOException {
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int length;
    while ((length = inputStream.read(buffer)) != -1) {
      result.write(buffer, 0, length);
    }
    return result.toString("UTF-8");
  }
}
