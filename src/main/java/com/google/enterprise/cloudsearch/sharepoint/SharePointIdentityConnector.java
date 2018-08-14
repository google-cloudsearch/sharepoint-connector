package com.google.enterprise.cloudsearch.sharepoint;

import com.google.enterprise.cloudsearch.sdk.identity.FullSyncIdentityConnector;
import com.google.enterprise.cloudsearch.sdk.identity.IdentityApplication;

public class SharePointIdentityConnector {
  public static void main(String[] args) throws InterruptedException {
    IdentityApplication application =
        new IdentityApplication.Builder(
                new FullSyncIdentityConnector(new SharePointIdentityRepository()), args)
            .build();
    application.start();
  }
}
