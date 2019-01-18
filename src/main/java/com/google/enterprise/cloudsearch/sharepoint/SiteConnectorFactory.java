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
