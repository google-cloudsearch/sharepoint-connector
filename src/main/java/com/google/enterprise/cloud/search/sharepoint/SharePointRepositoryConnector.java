package com.google.enterprise.cloud.search.sharepoint;

import com.google.enterprise.cloudsearch.sdk.indexing.IndexingApplication;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ListingConnector;

public class SharePointRepositoryConnector {
  public static void main(String[] args) throws InterruptedException {
    IndexingApplication application =
        new IndexingApplication.Builder(new ListingConnector(new SharePointRepository()), args)
            .build();
    application.start();
  }
}
