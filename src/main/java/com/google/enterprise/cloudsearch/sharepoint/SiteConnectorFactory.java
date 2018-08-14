package com.google.enterprise.cloudsearch.sharepoint;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.schemas.sharepoint.soap.SiteDataSoap;
import com.microsoft.schemas.sharepoint.soap.directory.UserGroupSoap;
import com.microsoft.schemas.sharepoint.soap.people.PeopleSoap;
import java.io.IOException;

interface SiteConnectorFactory {
  SiteConnector getInstance(String siteUrl, String webUrl) throws IOException;
  
  @VisibleForTesting
  interface SoapFactory {
    /** The {@code endpoint} string is a SharePoint URL, meaning that spaces are not encoded. */
    SiteDataSoap newSiteData(String endpoint);

    UserGroupSoap newUserGroup(String endpoint);

    PeopleSoap newPeople(String endpoint);
  }
}
