package com.google.enterprise.cloud.search.sharepoint;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterableImpl;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.identity.IdentityGroup;
import com.google.enterprise.cloudsearch.sdk.identity.IdentityUser;
import com.google.enterprise.cloudsearch.sdk.identity.Repository;
import com.google.enterprise.cloudsearch.sdk.identity.RepositoryContext;
import com.microsoft.schemas.sharepoint.soap.ContentDatabase;
import com.microsoft.schemas.sharepoint.soap.ContentDatabases;
import com.microsoft.schemas.sharepoint.soap.Sites;
import com.microsoft.schemas.sharepoint.soap.VirtualServer;
import java.io.IOException;
import java.net.Authenticator;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

class SharePointIdentityRepository implements Repository {
  private static final Logger log = Logger.getLogger(SharePointIdentityRepository.class.getName());

  private final SiteConnectorFactoryImpl.Builder siteConnectorFactoryBuilder;
  private final ScheduledExecutorService scheduledExecutorService;

  private SharePointConfiguration sharepointConfiguration;
  private SiteConnectorFactory siteConnectorFactory;
  private NtlmAuthenticator ntlmAuthenticator;
  private RepositoryContext repositoryContext;

  SharePointIdentityRepository() {
    this(new SiteConnectorFactoryImpl.Builder());
  }

  @VisibleForTesting
  SharePointIdentityRepository(SiteConnectorFactoryImpl.Builder siteConnectorFactoryBuilder) {
    this.siteConnectorFactoryBuilder = checkNotNull(siteConnectorFactoryBuilder);
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void init(RepositoryContext context) throws IOException {
    // TODO(tvartak) : Create base SharePointRepository object to share between Identity and Content
    // Connectors.
    checkState(Configuration.isInitialized());
    this.repositoryContext = checkNotNull(context);
    sharepointConfiguration = SharePointConfiguration.fromConfiguration();
    String username = sharepointConfiguration.getUserName();
    String password = sharepointConfiguration.getPassword();
    ntlmAuthenticator = new NtlmAuthenticator(username, password);
    SharePointUrl sharePointUrl = sharepointConfiguration.getSharePointUrl();
    try {
      ntlmAuthenticator.addPermitForHost(sharePointUrl.toURL());
    } catch (MalformedURLException malformed) {
      throw new InvalidConfigurationException(
          "Unable to parse SharePoint URL " + sharePointUrl, malformed);
    }
    if (!"".equals(username) && !"".equals(password)) {
      Authenticator.setDefault(ntlmAuthenticator);
    }
    AuthenticationClientFactory authenticationClientFactory = new AuthenticationClientFactoryImpl();
    authenticationClientFactory.init(
        sharePointUrl.getUrl(), username, password, scheduledExecutorService);
    FormsAuthenticationHandler formsAuthenticationHandler =
        authenticationClientFactory.getFormsAuthenticationHandler();
    if (formsAuthenticationHandler != null) {
      try {
        formsAuthenticationHandler.start();
      } catch (IOException e) {
        throw new RepositoryException.Builder()
            .setCause(e)
            .setErrorMessage("Error authenticating to SharePoint")
            .build();
      }
    }
    SharePointRequestContext requestContext =
        new SharePointRequestContext.Builder()
            .setAuthenticationHandler(formsAuthenticationHandler)
            .setSocketTimeoutMillis(sharepointConfiguration.getWebservicesSocketTimeoutMills())
            .setReadTimeoutMillis(sharepointConfiguration.getWebservicesReadTimeoutMills())
            .setUserAgent(sharepointConfiguration.getSharePointUserAgent())
            .build();
    siteConnectorFactory =
        siteConnectorFactoryBuilder
            .setRequestContext(requestContext)
            .setXmlValidation(sharepointConfiguration.isPerformXmlValidation())
            .setActiveDirectoryClient(ActiveDirectoryClient.fromConfiguration())
            .build();
  }

  @Override
  public CheckpointCloseableIterable<IdentityUser> listUsers(byte[] checkpoint) throws IOException {
    return new CheckpointCloseableIterableImpl.Builder<IdentityUser>(Collections.emptyList())
        .build();
  }

  @Override
  public CheckpointCloseableIterable<IdentityGroup> listGroups(byte[] checkpoint)
      throws IOException {
    return sharepointConfiguration.isSiteCollectionUrl()
        ? getLocalGroupsSiteCollectionOnly()
        : getLocalGroupsVirtualServer();
  }

  @Override
  public void close() {
    MoreExecutors.shutdownAndAwaitTermination(scheduledExecutorService, 2, TimeUnit.SECONDS);
  }

  private CheckpointCloseableIterable<IdentityGroup> getLocalGroupsSiteCollectionOnly()
      throws IOException {
    SiteConnector scConnector = checkNotNull(getSiteConnectorForSiteCollectionOnly());
    return new CheckpointCloseableIterableImpl.Builder<IdentityGroup>(
            scConnector.getSharePointGroups(repositoryContext))
        .build();
  }

  private CheckpointCloseableIterable<IdentityGroup> getLocalGroupsVirtualServer()
      throws IOException {
    SiteConnector vsConnector = checkNotNull(getSiteConnectorForVirtualServer());
    VirtualServer vs = vsConnector.getSiteDataClient().getContentVirtualServer();
    List<IdentityGroup> allGroups = new ArrayList<>();
    for (ContentDatabases.ContentDatabase cdcd : vs.getContentDatabases().getContentDatabase()) {
      ContentDatabase cd;
      try {
        cd = vsConnector.getSiteDataClient().getContentContentDatabase(cdcd.getID(), true);
      } catch (IOException ex) {
        log.log(Level.WARNING, "Failed to get content database: " + cdcd.getID(), ex);
        continue;
      }
      if (cd.getSites() == null) {
        continue;
      }
      for (Sites.Site siteListing : cd.getSites().getSite()) {
        String siteString = vsConnector.encodeDocId(siteListing.getURL());
        siteString = getCanonicalUrl(siteString);
        SharePointUrl sharePointSiteUrl;
        try {
          sharePointSiteUrl = buildSharePointUrl(siteString);
          ntlmAuthenticator.addPermitForHost(sharePointSiteUrl.toURL());
        } catch (URISyntaxException e) {
          log.log(Level.WARNING, "Error parsing site url", e);
          continue;
        }
        SiteConnector scConnector = getSiteConnector(siteString, siteString);
        allGroups.addAll(scConnector.getSharePointGroups(repositoryContext));
      }
    }
    return new CheckpointCloseableIterableImpl.Builder<IdentityGroup>(allGroups).build();
  }

  private SiteConnector getSiteConnectorForSiteCollectionOnly() throws IOException {
    return getSiteConnector(
        sharepointConfiguration.getSharePointUrl().getUrl(),
        sharepointConfiguration.getSharePointUrl().getUrl());
  }

  private SiteConnector getSiteConnectorForVirtualServer() throws IOException {
    return getSiteConnector(
        sharepointConfiguration.getVirtualServerUrl(),
        sharepointConfiguration.getVirtualServerUrl());
  }

  private SharePointUrl buildSharePointUrl(String url) throws URISyntaxException {
    return new SharePointUrl.Builder(url)
        .setPerformBrowserLeniency(sharepointConfiguration.isPerformBrowserLeniency())
        .build();
  }

  private SiteConnector getSiteConnector(String site, String web) throws IOException {
    web = getCanonicalUrl(web);
    site = getCanonicalUrl(site);
    try {
      ntlmAuthenticator.addPermitForHost(new URL(web));
    } catch (MalformedURLException e) {
      throw new IOException(e);
    }
    return siteConnectorFactory.getInstance(site, web);
  }

  // Remove trailing slash from URLs as SharePoint doesn't like trailing slash
  // in SiteData.GetUrlSegments
  private static String getCanonicalUrl(String url) {
    if (!url.endsWith("/")) {
      return url;
    }
    return url.substring(0, url.length() - 1);
  }
}
