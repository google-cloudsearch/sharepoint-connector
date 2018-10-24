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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.identity.IdentitySourceConfiguration;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

class SharePointConfiguration {
  private static final String DEFAULT_USER_NAME = isCredentialOptional() ? "" : null;
  private static final String DEFAULT_PASSWORD = isCredentialOptional() ? "" : null;

  private final SharePointUrl sharePointUrl;
  private final String virtualServerUrl;
  private final boolean siteCollectionOnly;
  private final Set<String> siteCollectionsToInclude;
  private final String userName;
  private final String password;
  private final String sharePointUserAgent;
  private final int webservicesSocketTimeoutMills;
  private final int webservicesReadTimeoutMills;
  private final boolean performXmlValidation;
  private final boolean performBrowserLeniency;
  private final ImmutableMap<String, IdentitySourceConfiguration>
      referenceIdentitySourceConfiguration;
  private final boolean stripDomainInUserPrincipals;

  private static boolean isCredentialOptional() {
    return System.getProperty("os.name", "").contains("Windows");
  }

  enum SharePointDeploymentType {
    ONLINE,
    ON_PREMISES;
  }

  private final SharePointDeploymentType sharePointDeploymentType;

  private SharePointConfiguration(Builder builder) throws URISyntaxException {
    sharePointUrl = builder.sharePointUrl;
    if (!"".equals(builder.sharePointSiteCollectionOnly)) {
      // Use config if provided
      this.siteCollectionOnly = Boolean.parseBoolean(builder.sharePointSiteCollectionOnly);
    } else {
      // If Connector is configured against Site Collection URL, we use that as a signal for
      // Site Collection Only Mode
      this.siteCollectionOnly = builder.sharePointUrl.getUrl().split("/").length > 3;
    }

    this.siteCollectionsToInclude =
        Collections.unmodifiableSet(new HashSet<>(builder.siteCollectionsToInclude));
    this.virtualServerUrl = sharePointUrl.getRootUrl();
    checkState(
        !Strings.isNullOrEmpty(builder.userName) || isCredentialOptional(),
        "user name can not be null or empty");
    this.userName = builder.userName;
    checkState(
        !Strings.isNullOrEmpty(builder.password) || isCredentialOptional(),
        "password can not be null or empty");
    this.password = builder.password;
    this.sharePointUserAgent =
        checkNotNull(builder.sharePointUserAgent, "user agent can not be null");
    checkArgument(
        builder.webservicesSocketTimeoutMills >= 0,
        "Webservices socket time out can not be less than 0");
    this.webservicesSocketTimeoutMills = builder.webservicesSocketTimeoutMills;
    checkArgument(
        builder.webservicesReadTimeoutMills >= 0,
        "Webservices read time out can not be less than 0");
    this.webservicesReadTimeoutMills = builder.webservicesReadTimeoutMills;
    this.performXmlValidation = builder.performXmlValidation;
    this.performBrowserLeniency = builder.performBrowserLeniency;
    this.referenceIdentitySourceConfiguration =
        checkNotNull(builder.referenceIdentitySourceConfiguration);
    this.stripDomainInUserPrincipals = builder.stripDomainInUserPrincipals;
    this.sharePointDeploymentType = builder.sharePointDeploymentType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SharePointConfiguration)) {
      return false;
    }
    SharePointConfiguration that = (SharePointConfiguration) o;
    return (siteCollectionOnly == that.siteCollectionOnly)
        && Objects.equals(sharePointUrl, that.sharePointUrl)
        && Objects.equals(virtualServerUrl, that.virtualServerUrl)
        && Objects.equals(siteCollectionsToInclude, that.siteCollectionsToInclude)
        && Objects.equals(userName, that.userName)
        && Objects.equals(password, that.password)
        && Objects.equals(sharePointUserAgent, that.sharePointUserAgent)
        && Objects.equals(webservicesSocketTimeoutMills, that.webservicesSocketTimeoutMills)
        && Objects.equals(webservicesReadTimeoutMills, that.webservicesReadTimeoutMills)
        && Objects.equals(performXmlValidation, that.performXmlValidation)
        && Objects.equals(performBrowserLeniency, that.performBrowserLeniency)
        && Objects.equals(
            referenceIdentitySourceConfiguration, that.referenceIdentitySourceConfiguration)
        && Objects.equals(stripDomainInUserPrincipals, that.stripDomainInUserPrincipals)
        && Objects.equals(sharePointDeploymentType, that.sharePointDeploymentType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        sharePointUrl,
        virtualServerUrl,
        siteCollectionOnly,
        siteCollectionsToInclude,
        userName,
        password,
        sharePointUserAgent,
        webservicesSocketTimeoutMills,
        webservicesReadTimeoutMills,
        performXmlValidation,
        performBrowserLeniency,
        referenceIdentitySourceConfiguration,
        stripDomainInUserPrincipals,
        sharePointDeploymentType);
  }

  boolean isSiteCollectionUrl() {
    return this.siteCollectionOnly;
  }

  String getVirtualServerUrl() {
    return this.virtualServerUrl;
  }

  SharePointUrl getSharePointUrl() {
    return this.sharePointUrl;
  }

  String getUserName() {
    return userName;
  }

  String getPassword() {
    return password;
  }

  String getSharePointUserAgent() {
    return sharePointUserAgent;
  }

  int getWebservicesSocketTimeoutMills() {
    return webservicesSocketTimeoutMills;
  }

  int getWebservicesReadTimeoutMills() {
    return webservicesReadTimeoutMills;
  }

  SharePointDeploymentType getSharePointDeploymentType() {
    return sharePointDeploymentType;
  }

  boolean isPerformXmlValidation() {
    return performXmlValidation;
  }

  boolean isPerformBrowserLeniency() {
    return performBrowserLeniency;
  }

  boolean isStripDomainInUserPrincipals() {
    return stripDomainInUserPrincipals;
  }

  ImmutableMap<String, IdentitySourceConfiguration> getReferenceIdentitySourceConfiguration() {
    return referenceIdentitySourceConfiguration;
  }

  @Override
  public String toString() {
    return "SharePointConfiguration [sharePointUrl="
        + sharePointUrl
        + ", virtualServerUrl="
        + virtualServerUrl
        + ", siteCollectionOnly="
        + siteCollectionOnly
        + ", siteCollectionsToInclude="
        + siteCollectionsToInclude
        + ", userName="
        + userName
        + ", password=xxxxx"
        + ", sharePointUserAgent="
        + sharePointUserAgent
        + ", webservicesSocketTimeoutMills="
        + webservicesSocketTimeoutMills
        + ", webservicesReadTimeoutMills="
        + webservicesReadTimeoutMills
        + ", performXmlValidation="
        + performXmlValidation
        + ", performBrowserLeniency="
        + performBrowserLeniency
        + ", referenceIdentitySourceConfiguration="
        + referenceIdentitySourceConfiguration
        + ", stripDomainInUserPrincipals="
        + stripDomainInUserPrincipals
        + ", sharePointDeploymentType="
        + sharePointDeploymentType
        + "]";
  }

  static class Builder {
    private SharePointUrl sharePointUrl;
    private String sharePointSiteCollectionOnly = "";
    private Set<String> siteCollectionsToInclude = new HashSet<String>();
    private String userName = DEFAULT_USER_NAME;
    private String password = DEFAULT_PASSWORD;
    private String sharePointUserAgent = "";
    private int webservicesSocketTimeoutMills = 30 * 1000;
    private int webservicesReadTimeoutMills = 180 * 1000;
    private boolean performXmlValidation = false;
    private boolean performBrowserLeniency = true;
    private ImmutableMap<String, IdentitySourceConfiguration> referenceIdentitySourceConfiguration;
    private boolean stripDomainInUserPrincipals;
    private SharePointDeploymentType sharePointDeploymentType =
        SharePointDeploymentType.ON_PREMISES;

    Builder(SharePointUrl sharePointUrl) {
      this.sharePointUrl = sharePointUrl;
    }

    Builder setUserName(String userName) {
      this.userName = userName;
      return this;
    }

    Builder setPassword(String password) {
      this.password = password;
      return this;
    }

    Builder setSharePointSiteCollectionOnly(String sharePointSiteCollectionOnly) {
      this.sharePointSiteCollectionOnly = sharePointSiteCollectionOnly;
      return this;
    }

    Builder setSiteCollectionsToInclude(Set<String> siteCollectionsToInclude) {
      this.siteCollectionsToInclude = siteCollectionsToInclude;
      return this;
    }

    Builder setUserAgent(String sharePointUserAgent) {
      this.sharePointUserAgent = sharePointUserAgent;
      return this;
    }

    Builder setWebservicesSocketTimeoutMills(int webservicesSocketTimeoutMills) {
      this.webservicesSocketTimeoutMills = webservicesSocketTimeoutMills;
      return this;
    }

    Builder setWebservicesReadTimeoutMills(int webservicesReadTimeoutMills) {
      this.webservicesReadTimeoutMills = webservicesReadTimeoutMills;
      return this;
    }

    Builder setPerformXmlValidation(boolean xmlValidation) {
      this.performXmlValidation = xmlValidation;
      return this;
    }

    Builder setPerformBrowserLeniency(boolean performBrowserLeniency) {
      this.performBrowserLeniency = performBrowserLeniency;
      return this;
    }

    Builder setReferenceIdentitySourceConfiguration(
        Map<String, IdentitySourceConfiguration> referenceIdentitySourceConfiguration) {
      this.referenceIdentitySourceConfiguration =
          ImmutableMap.copyOf(referenceIdentitySourceConfiguration);
      return this;
    }

    Builder setStripDomainInUserPrincipals(boolean stripDomainInUserPrincipals) {
      this.stripDomainInUserPrincipals = stripDomainInUserPrincipals;
      return this;
    }

    Builder setSharePointDeploymentType(SharePointDeploymentType sharePointDeploymentType) {
      this.sharePointDeploymentType = sharePointDeploymentType;
      return this;
    }

    SharePointConfiguration build() throws URISyntaxException {
      if ((sharePointUrl == null)
          || (sharePointSiteCollectionOnly == null)
          || (siteCollectionsToInclude == null)) {
        throw new InvalidConfigurationException();
      }
      sharePointSiteCollectionOnly = sharePointSiteCollectionOnly.trim();
      return new SharePointConfiguration(this);
    }
  }

  static SharePointConfiguration fromConfiguration() {
    checkState(Configuration.isInitialized(), "connector configuration not initialized yet");
    String sharePointServer = Configuration.getString("sharepoint.server", null).get();
    boolean performBrowserLeniency =
        Configuration.getBoolean("connector.lenientUrlRulesAndCustomRedirect", true).get();
    SharePointUrl sharepointUrl;
    try {
      sharepointUrl =
          new SharePointUrl.Builder(sharePointServer)
              .setPerformBrowserLeniency(performBrowserLeniency)
              .build();
    } catch (Exception e) {
      throw new InvalidConfigurationException("Invalid SharePoint URL " + sharePointServer, e);
    }
    String username = Configuration.getString("sharepoint.username", DEFAULT_USER_NAME).get();
    String password = Configuration.getString("sharepoint.password", DEFAULT_PASSWORD).get();
    String siteCollectionOnlyMode =
        Configuration.getString("sharepoint.siteCollectionOnly", "").get();
    String sharePointUserAgent = Configuration.getString("sharepoint.userAgent", "").get().trim();
    int socketTimeoutMillis =
        Configuration.getInteger("sharepoint.webservices.socketTimeoutSecs", 30).get() * 1000;
    int readTimeOutMillis =
        Configuration.getInteger("sharepoint.webservices.readTimeOutSecs", 180).get() * 1000;
    boolean xmlValidation = Configuration.getBoolean("sharepoint.xmlValidation", false).get();
    boolean stripDomainInUserPrincipals =
        Configuration.getBoolean("sharepoint.stripDomainInUserPrincipals", false).get();
    SharePointDeploymentType sharePointDeploymentType =
        Configuration.getValue(
                "sharepoint.deploymentType",
                SharePointDeploymentType.ON_PREMISES,
                (v) -> SharePointDeploymentType.valueOf(v.toUpperCase(Locale.ENGLISH)))
            .get();
    try {
      return new Builder(sharepointUrl)
          .setUserName(username)
          .setPassword(password)
          .setSharePointSiteCollectionOnly(siteCollectionOnlyMode)
          .setUserAgent(sharePointUserAgent)
          .setWebservicesSocketTimeoutMills(socketTimeoutMillis)
          .setWebservicesReadTimeoutMills(readTimeOutMillis)
          .setPerformXmlValidation(xmlValidation)
          .setPerformBrowserLeniency(performBrowserLeniency)
          .setStripDomainInUserPrincipals(stripDomainInUserPrincipals)
          .setReferenceIdentitySourceConfiguration(
              IdentitySourceConfiguration.getReferenceIdentitySourcesFromConfiguration())
          .setSharePointDeploymentType(sharePointDeploymentType)
          .build();
    } catch (Exception e) {
      throw new InvalidConfigurationException("Invalid SharePoint Configuration", e);
    }
  }
}
