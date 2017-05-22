package com.google.enterprise.cloud.search.sharepoint;

import com.google.enterprise.springboard.sdk.Application;
import com.google.enterprise.springboard.sdk.template.ListingConnector;
import java.io.IOException;

public class SharePointRepositoryConnector {
  public static void main(String[] args) throws InterruptedException, IOException {
    Application application =
        new Application.Builder(new ListingConnector(new SharePointRepository()), args).build();
    application.start();
  }
}
