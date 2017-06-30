package com.google.enterprise.cloud.search.sharepoint;

import com.google.enterprise.cloudsearch.sdk.Application;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ListingConnector;

public class SharePointRepositoryConnector {
  public static void main(String[] args) throws InterruptedException {
    Application application =
        new Application.Builder(new ListingConnector(new SharePointRepository()), args).build();
    application.start();
  }
}
