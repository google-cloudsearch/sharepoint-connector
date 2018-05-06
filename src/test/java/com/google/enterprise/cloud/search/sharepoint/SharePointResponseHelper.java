package com.google.enterprise.cloud.search.sharepoint;

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
