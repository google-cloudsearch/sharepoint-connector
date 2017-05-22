package com.google.enterprise.cloud.search.sharepoint;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.util.DateTime;
import com.google.api.client.util.Strings;
import com.google.api.services.springboardindex.model.ExternalGroup;
import com.google.api.services.springboardindex.model.Item;
import com.google.api.services.springboardindex.model.Principal;
import com.google.api.services.springboardindex.model.PushEntry;
import com.google.api.services.springboardindex.model.QueueEntry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.enterprise.adaptor.sharepoint.FormsAuthenticationHandler;
import com.google.enterprise.adaptor.sharepoint.SiteDataClient.Paginator;
import com.google.enterprise.springboard.sdk.Acl;
import com.google.enterprise.springboard.sdk.Acl.InheritanceType;
import com.google.enterprise.springboard.sdk.Config;
import com.google.enterprise.springboard.sdk.InvalidConfigurationException;
import com.google.enterprise.springboard.sdk.template.ApiOperation;
import com.google.enterprise.springboard.sdk.template.ApiOperations;
import com.google.enterprise.springboard.sdk.template.ClosableIterable;
import com.google.enterprise.springboard.sdk.template.IncrementalChanges;
import com.google.enterprise.springboard.sdk.template.Repository;
import com.google.enterprise.springboard.sdk.template.RepositoryDoc;
import com.google.enterprise.springboard.sdk.template.RepositoryException;
import com.microsoft.schemas.sharepoint.soap.ContentDatabase;
import com.microsoft.schemas.sharepoint.soap.ContentDatabases;
import com.microsoft.schemas.sharepoint.soap.ItemData;
import com.microsoft.schemas.sharepoint.soap.Lists;
import com.microsoft.schemas.sharepoint.soap.Scopes;
import com.microsoft.schemas.sharepoint.soap.Site;
import com.microsoft.schemas.sharepoint.soap.Sites;
import com.microsoft.schemas.sharepoint.soap.VirtualServer;
import com.microsoft.schemas.sharepoint.soap.Web;
import com.microsoft.schemas.sharepoint.soap.Webs;
import com.microsoft.schemas.sharepoint.soap.Xml;
import java.io.IOException;
import java.net.Authenticator;
import java.net.MalformedURLException;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.namespace.QName;
import javax.xml.ws.Holder;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class SharePointRepository implements Repository {
  private static final Logger log = Logger.getLogger(SharePointRepository.class.getName());
  /**
   * The data element within a self-describing XML blob. See
   * http://msdn.microsoft.com/en-us/library/windows/desktop/ms675943.aspx .
   */
  private static final QName DATA_ELEMENT = new QName("urn:schemas-microsoft-com:rowset", "data");
  /**
   * The row element within a self-describing XML blob. See
   * http://msdn.microsoft.com/en-us/library/windows/desktop/ms675943.aspx .
   */
  private static final QName ROW_ELEMENT = new QName("#RowsetSchema", "row");
  /**
   * Row attribute that contains a URL-like string identifying the object. Sometimes this can be
   * modified (by turning spaces into %20 and the like) to access the object. In general, this in
   * the string we provide to SP to resolve information about the object.
   */
  private static final String OWS_SERVERURL_ATTRIBUTE = "ows_ServerUrl";
  /** The last time metadata or content was modified. */
  private static final String OWS_MODIFIED_ATTRIBUTE = "ows_Modified";
  /**
   * Row attribute guaranteed to be in ListItem responses. See
   * http://msdn.microsoft.com/en-us/library/dd929205.aspx . Provides scope id used for permissions.
   * Note that the casing is different than documented; this is simply because of a documentation
   * bug.
   */
  private static final String OWS_SCOPEID_ATTRIBUTE = "ows_ScopeId";
  /** Relative folder path for an item */
  private static final String OWS_FILEDIRREF_ATTRIBUTE = "ows_FileDirRef";
  /**
   * Row attribute guaranteed to be in ListItem responses. See
   * http://msdn.microsoft.com/en-us/library/dd929205.aspx . Provides ability to distinguish between
   * folders and other list items.
   */
  private static final String OWS_FSOBJTYPE_ATTRIBUTE = "ows_FSObjType";
  /** Provides the number of attachments the list item has. */
  private static final String OWS_ATTACHMENTS_ATTRIBUTE = "ows_Attachments";
  /**
   * Row attribute that contains a hierarchial hex number that describes the type of object. See
   * http://msdn.microsoft.com/en-us/library/aa543822.aspx for more information about content type
   * IDs.
   */
  private static final String OWS_CONTENTTYPEID_ATTRIBUTE = "ows_ContentTypeId";
  /** As described at http://msdn.microsoft.com/en-us/library/aa543822.aspx . */
  private static final String CONTENTTYPEID_DOCUMENT_PREFIX = "0x0101";


  static final String VIRTUAL_SERVER_ID = "ROOT_NEW";
  static final String SITE_COLLECTION_ADMIN_FRAGMENT = "admin";

  private static final TimeZone gmt = TimeZone.getTimeZone("GMT");
  /** RFC 822 date format, as updated by RFC 1123. */
  private final ThreadLocal<DateFormat> dateFormatRfc1123 =
      ThreadLocal.withInitial(() -> {
        DateFormat df = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.ENGLISH);
        df.setTimeZone(gmt);
        return df;
      });

  private final ThreadLocal<DateFormat> modifiedDateFormat =
      ThreadLocal.withInitial(() -> {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
        df.setTimeZone(gmt);
        return df;
      });
  private final ThreadLocal<DateFormat> listLastModifiedDateFormat =
      ThreadLocal.withInitial(() -> {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss'Z'", Locale.ENGLISH);
        df.setTimeZone(gmt);
        return df;
      });

  /** Mapping of mime-types used by SharePoint to ones that the Cloud Search comprehends. */
  private static final Map<String, String> MIME_TYPE_MAPPING;

  static {
    Map<String, String> map = new HashMap<String, String>();
    // Mime types used by SharePoint that aren't IANA-registered.
    // Extension .xlsx
    map.put(
        "application/vnd.ms-excel.12",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    // Extension .pptx
    map.put(
        "application/vnd.ms-powerpoint.presentation.12",
        "application/" + "vnd.openxmlformats-officedocument.presentationml.presentation");
    // Extension .docx
    map.put(
        "application/vnd.ms-word.document.12",
        "application/" + "vnd.openxmlformats-officedocument.wordprocessingml.document");
    // Extension .ppsm
    map.put(
        "application/vnd.ms-powerpoint.show.macroEnabled.12",
        "application/" + "vnd.openxmlformats-officedocument.presentationml.presentation");
    // Extension .ppsx
    map.put(
        "application/vnd.ms-powerpoint.show.12",
        "application/" + "vnd.openxmlformats-officedocument.presentationml.presentation");
    // Extension .pptm
    map.put(
        "application/vnd.ms-powerpoint.macroEnabled.12",
        "application/" + "vnd.openxmlformats-officedocument.presentationml.presentation");
    // Extension .xlsm
    map.put(
        "application/vnd.ms-excel.macroEnabled.12",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");

    // IANA-registered mime types unknown to GSA 7.2.
    // Extension .docm
    map.put(
        "application/vnd.ms-word.document.macroEnabled.12",
        "application/" + "vnd.openxmlformats-officedocument.wordprocessingml.document");
    // Extension .pptm
    map.put(
        "application/vnd.ms-powerpoint.presentation.macroEnabled.12",
        "application/" + "vnd.openxmlformats-officedocument.presentationml.presentation");
    // Extension .xlsm
    map.put(
        "application/vnd.ms-excel.sheet.macroEnabled.12",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");

    MIME_TYPE_MAPPING = Collections.unmodifiableMap(map);
  }

  private static final Map<String, String> FILE_EXTENSION_TO_MIME_TYPE_MAPPING =
      new ImmutableMap.Builder<String, String>()
          // Map .msg files to mime type application/vnd.ms-outlook
          .put(".msg", "application/vnd.ms-outlook")
          .build();

  private SiteConnectorFactory siteConnectorFactory;
  private SharePointConfiguration sharepointConfiguration;
  private NtlmAuthenticator ntlmAuthenticator;
  // TODO(tvratak) : Add support for authentication handler implementations.
  private FormsAuthenticationHandler authenticationHandler = null;
  private boolean performBrowserLeniency;
  private HttpClient httpClient;
  
  private final HttpClientImpl.Builder httpClientBuilder;
  private final SiteConnectorFactoryImpl.Builder siteConnectorFactoryBuilder;

  SharePointRepository() {
    this(new HttpClientImpl.Builder(), new SiteConnectorFactoryImpl.Builder());
  }
  
  @VisibleForTesting
  SharePointRepository(
      HttpClientImpl.Builder httpClientBuilder,
      SiteConnectorFactoryImpl.Builder siteConnectorFactoryBuilder) {
    this.httpClientBuilder = checkNotNull(httpClientBuilder);
    this.siteConnectorFactoryBuilder = checkNotNull(siteConnectorFactoryBuilder);
  }

  @Override
  public Map<String, String> getDefaults() {
    Map<String, String> defaultConfig = new HashMap<String, String>();
    defaultConfig.put("sharepoint.server", "");
    // When running on Windows, Windows Authentication can log us in.
    defaultConfig.put(
        "sharepoint.username", System.getProperty("os.name").contains("Windows") ? "" : null);
    defaultConfig.put(
        "sharepoint.password", System.getProperty("os.name").contains("Windows") ? "" : null);
    // On any particular SharePoint instance, we expect that at least some
    // responses will not pass xml validation. We keep the option around to
    // allow us to improve the schema itself, but also allow enable users to
    // enable checking as a form of debugging.
    defaultConfig.put("sharepoint.xmlValidation", "false");
    defaultConfig.put("connector.namespace", "Default");
    // When running against ADFS authentication, set this to ADFS endpoint.
    defaultConfig.put("sharepoint.sts.endpoint", "");
    // When running against ADFS authentication, set this to realm value.
    // Normally realm value is either http://sharepointurl/_trust or
    // urn:sharepointenv:com format. You can use
    // Get-SPTrustedIdentityTokenIssuer to get "DefaultProviderRealm" value
    defaultConfig.put("sharepoint.sts.realm", "");
    // You can override default value of http://sharepointurl/_trust by
    // specifying this property.
    defaultConfig.put("sharepoint.sts.trustLocation", "");
    // You can override default value of
    // http://sharepointurl/_layouts/Authenticate.aspx by specifying this
    // property.
    defaultConfig.put("sharepoint.sts.login", "");
    // Set this to true when using Live authentication.
    defaultConfig.put("sharepoint.useLiveAuthentication", "false");
    // Set this to specific user-agent value to be used by connector while making
    // request to SharePoint
    defaultConfig.put("sharepoint.userAgent", "");
    // Set this to true when you want to index only single site collection
    defaultConfig.put("sharepoint.siteCollectionOnly", "");
    // Set this to positive integer value to configure maximum number of
    // URL redirects allowed to download document contents.
    defaultConfig.put("connector.maxRedirectsToFollow", "");
    // Set this to true when connector needs to encode redirect urls and perform
    // browser leniency for handling unsupported characters in urls.
    defaultConfig.put("connector.lenientUrlRulesAndCustomRedirect", "true");
    defaultConfig.put("sidLookup.host", "");
    defaultConfig.put("sidLookup.port", "3268");
    defaultConfig.put("sidLookup.username", "");
    defaultConfig.put("sidLookup.password", "");
    defaultConfig.put("sidLookup.method", "standard");
    // Set this to static factory method name which will return
    // custom SamlHandshakeManager object
    defaultConfig.put("customSamlManager.configPrefix", "customSamlManager");
    defaultConfig.put("sharepoint.customSamlManager", "");
    defaultConfig.put("sharepoint.webservices.socketTimeoutSecs", "30");
    defaultConfig.put("sharepoint.webservices.readTimeOutSecs", "180");
    return Collections.unmodifiableMap(defaultConfig);
  }

  @Override
  public void init(Config config) {
    checkNotNull(config);
    String sharePointServer = config.getValue("sharepoint.server");
    performBrowserLeniency =
        Boolean.parseBoolean(config.getValue("connector.lenientUrlRulesAndCustomRedirect"));
    String username = config.getValue("sharepoint.username");
    String password = config.getValue("sharepoint.password");
    ntlmAuthenticator = new NtlmAuthenticator(username, password);
    try {
      SharePointUrl configuredUrl = buildSharePointUrl(sharePointServer);
      sharepointConfiguration =
          new SharePointConfiguration.Builder(configuredUrl)
              .setSharePointSiteCollectionOnly(config.getValue("sharepoint.siteCollectionOnly"))
              .build();
      ntlmAuthenticator.addPermitForHost(configuredUrl.toURL());
    } catch (Exception e) {
      throw new InvalidConfigurationException("Error validating SharePoint URL", e);
    }
    if (!"".equals(username) && !"".equals(password)) {
      // Unfortunately, this is a JVM-wide modification.
      Authenticator.setDefault(ntlmAuthenticator);
    }
    String sharepointUserAgent = config.getValue("sharepoint.userAgent").trim();
    int socketTimeoutMillis =
        Integer.parseInt(config.getValue("sharepoint.webservices.socketTimeoutSecs")) * 1000;
    int readTimeOutMillis =
        Integer.parseInt(config.getValue("sharepoint.webservices.readTimeOutSecs")) * 1000;
    boolean xmlValidation = Boolean.parseBoolean(config.getValue("sharepoint.xmlValidation"));
    SharePointRequestContext requestContext = new SharePointRequestContext.Builder()
        .setAuthenticationHandler(authenticationHandler)
        .setSocketTimeoutMillis(socketTimeoutMillis)
        .setReadTimeoutMillis(readTimeOutMillis)
        .setUserAgent(sharepointUserAgent)
        .build();
    httpClient =
        httpClientBuilder
            .setSharePointRequestContext(requestContext)
            .setMaxRedirectsAllowed(20)
            .setPerformBrowserLeniency(performBrowserLeniency)
            .build();
    siteConnectorFactory =
        siteConnectorFactoryBuilder
            .setRequestContext(requestContext)
            .setXmlValidation(xmlValidation)
            .build();
  }

  @Override
  public ClosableIterable<ApiOperation> getIds() throws RepositoryException {
    log.entering("SharePointConnector", "traverse");
    ClosableIterable<ApiOperation> toReturn =
        sharepointConfiguration.isSiteCollectionUrl()
            ? getDocIdsSiteCollectionOnly()
            : getDocIdsVirtualServer();
    log.exiting("SharePointConnector", "traverse");
    return toReturn;
  }

  @Override
  public IncrementalChanges getChanges(byte[] checkpoint) {
    // TODO(tvartak): Auto-generated method stub
    return null;
  }

  @Override
  public ClosableIterable<ApiOperation> getAllDocs() {
    // TODO(tvartak): Auto-generated method stub
    return null;
  }

  @Override
  public ApiOperation getDoc(QueueEntry entry) throws RepositoryException {
    checkNotNull(entry);
    try {
      SharePointObject object = SharePointObject.parse(entry.decodePayload());
      String objectType = object.getObjectType();
      if (!object.isValid()) {
        //throw new RepositoryException("Invalid Object Type " + objectType);
        log.log(
            Level.WARNING,
            "Invalid SharePoint Objecct {0} on entry {1}",
            new Object[] {object, entry});
        throw new RepositoryException("Invalid payload");
        //return ApiOperations.deleteItem(entry.getId());
      }

      if (SharePointObject.NAMED_RESOURCE.equals(objectType)) {
        // Do not process named resource here.
        PushEntry notModified =
            new PushEntry()
                .setId(entry.getId())
                .setKind("notModified")
                .encodePayload(object.encodePayload());
        return ApiOperations.pushEntries(Collections.singletonList(notModified));
      }

      if (SharePointObject.VIRTUAL_SERVER.equals(objectType)) {
        return getVirtualServerDocContent();
      }

      SiteConnector siteConnector;
      try {
        siteConnector = getConnectorForDocId(entry.getId());
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
      if (siteConnector == null) {
        return ApiOperations.deleteItem(entry.getId());
      }

      if (SharePointObject.SITE_COLLECTION.equals(objectType)) {
        return getSiteCollectionDocContent(entry, siteConnector, object);
      }
      if (SharePointObject.WEB.equals(objectType)) {
        return getWebDocContent(entry, siteConnector, object);
      }
      if (SharePointObject.LIST.equals(objectType)) {
        return getListDocContent(entry, siteConnector, object);
      }
      if (SharePointObject.LIST_ITEM.equals(objectType)) {
        return getListItemDocContent(entry, siteConnector, object);
      }
      if (SharePointObject.ATTACHMENT.equals(objectType)) {
        return getAttachmentDocContent(entry, siteConnector, object);
      }
      PushEntry notModified =
          new PushEntry()
              .setId(entry.getId())
              .setKind("notModified")
              .encodePayload(object.encodePayload());
      return ApiOperations.pushEntries(Collections.singletonList(notModified));
    } catch (IOException e) {
      throw new RepositoryException(e);
    }
  }

  @Override
  public boolean exists(QueueEntry entry) {
    return false;
  }

  @Override
  public void close() {
  }

  private SiteConnector getConnectorForDocId(String url) throws IOException, URISyntaxException {
    if (VIRTUAL_SERVER_ID.equals(url)) {
      if (sharepointConfiguration.isSiteCollectionUrl()) {
        log.log(
            Level.FINE,
            "Returning null SiteConnector for root document "
                + " because connector is currently configured in site collection "
                + "mode for {0} only.",
            sharepointConfiguration.getSharePointUrl());
        return null;
      }
      return getSiteConnector(
          sharepointConfiguration.getVirtualServerUrl(),
          sharepointConfiguration.getVirtualServerUrl());
    }
    SharePointUrl docUrl =
        buildSharePointUrl(url);
    if (!ntlmAuthenticator.isPermittedHost(docUrl.toURL())) {
      log.log(Level.WARNING, "URL {0} not white listed", docUrl);
      return null;
    }
    String rootUrl = docUrl.getRootUrl();
    SiteConnector rootConnector = getSiteConnector(rootUrl, rootUrl);
    Holder<String> site = new Holder<String>();
    Holder<String> web = new Holder<String>();
    long result = rootConnector.getSiteDataClient().getSiteAndWeb(url, site, web);
    if (result != 0) {
      return null;
    }
    if (sharepointConfiguration.isSiteCollectionUrl()
        &&
        // Performing case sensitive comparison as mismatch in URL casing
        // between SharePoint Server and connector can result in broken ACL
        // inheritance chain on GSA.
        !sharepointConfiguration.getSharePointUrl().getUrl().equals(site.value)) {
      log.log(
          Level.FINE,
          "Returning null SiteConnector for {0} because "
              + "connector is currently configured in site collection mode "
              + "for {1} only.",
          new Object[] {url, sharepointConfiguration.getSharePointUrl()});
      return null;
    }
    return getSiteConnector(site.value, web.value);
  }

  private ClosableIterable<ApiOperation> getDocIdsVirtualServer() throws RepositoryException {
    try {
      List<ApiOperation> operations = new ArrayList<ApiOperation>();
      SharePointObject vsObject =
          new SharePointObject.Builder(SharePointObject.VIRTUAL_SERVER).build();
      PushEntry pushEntry =
          new PushEntry().setId(VIRTUAL_SERVER_ID).encodePayload(vsObject.encodePayload());
      operations.add(ApiOperations.pushEntries(Collections.singletonList(pushEntry)));
      SiteConnector vsConnector =
          getSiteConnector(
              sharepointConfiguration.getVirtualServerUrl(),
              sharepointConfiguration.getVirtualServerUrl());
      checkNotNull(vsConnector);
      VirtualServer vs = vsConnector.getSiteDataClient().getContentVirtualServer();
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
            sharePointSiteUrl =
                buildSharePointUrl(siteString);
            ntlmAuthenticator.addPermitForHost(sharePointSiteUrl.toURL());
          } catch (URISyntaxException e) {
            log.log(Level.WARNING, "Error parsing site url", e);
            continue;
          }

          SiteConnector siteConnector = getSiteConnector(siteString, siteString);
          Site site;
          try {
            site = siteConnector.getSiteDataClient().getContentSite();
          } catch (IOException ex) {
            log.log(Level.WARNING, "Failed to get local groups for site: "
                + siteString, ex);
            continue;
          }
          Collection<ExternalGroup> siteGroups =
              siteConnector.computeMembersForGroups(site.getGroups());
          siteGroups.forEach((group) -> operations.add(ApiOperations.updateExternalGroup(group)));
        }
      }
      return ApiOperations.wrapAsClosableIterable(operations.iterator());
    } catch (IOException e) {
      throw new RepositoryException(e);
    }
  }

  private ClosableIterable<ApiOperation> getDocIdsSiteCollectionOnly() throws RepositoryException {
    try {
      List<ApiOperation> operations = new ArrayList<ApiOperation>();
      SiteConnector scConnector =
          getSiteConnector(
              sharepointConfiguration.getSharePointUrl().getUrl(),
              sharepointConfiguration.getSharePointUrl().getUrl());
      Site site = scConnector.getSiteDataClient().getContentSite();
      String siteCollectionUrl = getCanonicalUrl(site.getMetadata().getURL());
      SharePointObject siteCollection =
          new SharePointObject.Builder(SharePointObject.SITE_COLLECTION)
              .setUrl(siteCollectionUrl)
              .setObjectId(site.getMetadata().getID())
              .setSiteId(site.getMetadata().getID())
              .setWebId(site.getMetadata().getID())
              .build();
      PushEntry pushEntry =
          new PushEntry().setId(siteCollectionUrl).encodePayload(siteCollection.encodePayload());
      operations.add(ApiOperations.pushEntries(Collections.singletonList(pushEntry)));
      Collection<ExternalGroup> siteGroups = scConnector.computeMembersForGroups(site.getGroups());
      siteGroups.forEach((group) -> operations.add(ApiOperations.updateExternalGroup(group)));
      return ApiOperations.wrapAsClosableIterable(operations.iterator());
    } catch (IOException e) {
      throw new RepositoryException(e);
    }
  }

  private ApiOperation getVirtualServerDocContent() throws RepositoryException {
    try {
      SiteConnector vsConnector =
          getSiteConnector(
              sharepointConfiguration.getVirtualServerUrl(),
              sharepointConfiguration.getVirtualServerUrl());
      VirtualServer vs = vsConnector.getSiteDataClient().getContentVirtualServer();
      Acl webApplicationPolicy = vsConnector.getWebApplicationPolicyAcl(vs);

      com.google.api.services.springboardindex.model.Item item =
          new com.google.api.services.springboardindex.model.Item();
      item.setId(VIRTUAL_SERVER_ID);
      webApplicationPolicy.applyTo(item);
      List<PushEntry> sites = new ArrayList<>();
      for (ContentDatabases.ContentDatabase cdcd : vs.getContentDatabases().getContentDatabase()) {
        try {
          ContentDatabase cd =
              vsConnector.getSiteDataClient().getContentContentDatabase(cdcd.getID(), true);
          if (cd.getSites() != null) {
            for (Sites.Site site : cd.getSites().getSite()) {
              String siteUrl = site.getURL();
              siteUrl = getCanonicalUrl(siteUrl);
              SharePointObject siteCollection =
                  new SharePointObject.Builder(SharePointObject.SITE_COLLECTION)
                      .setUrl(siteUrl)
                      .setObjectId(site.getID())
                      .setSiteId(site.getID())
                      .setWebId(site.getID())
                      .build();
              sites.add(
                  new PushEntry()
                      .setId(vsConnector.encodeDocId(siteUrl))
                      .encodePayload(siteCollection.encodePayload()));
            }
          }
        } catch (IOException ex) {
          log.log(Level.WARNING, "Error retriving sites from content database " + cdcd.getID(), ex);
        }
      }
      return new RepositoryDoc.Builder().setItem(item).setChildIds(sites).build();
    } catch (IOException e) {
      throw new RepositoryException(e);
    }
  }

  private ApiOperation getSiteCollectionDocContent(
      QueueEntry entry,
      SiteConnector scConnector,
      @SuppressWarnings("unused") SharePointObject siteCollection)
      throws IOException {
    List<ApiOperation> batchRequest = new ArrayList<ApiOperation>();
    Site site = scConnector.getSiteDataClient().getContentSite();
    Collection<ExternalGroup> siteGroups = scConnector.computeMembersForGroups(site.getGroups());
    siteGroups.forEach((group) -> batchRequest.add(ApiOperations.updateExternalGroup(group)));

    Web rootWeb = scConnector.getSiteDataClient().getContentWeb();
    List<Principal> admins = scConnector.getSiteCollectionAdmins(rootWeb);
    Acl.Builder siteAdmins = new Acl.Builder().setReaders(admins);
    Item item = new Item().setId(entry.getId());
    List<PushEntry> entries = new ArrayList<>();
    String siteAdminFragmentId = Acl.fragmentId(entry.getId(), SITE_COLLECTION_ADMIN_FRAGMENT);
    SharePointObject siteAdminObject =
        new SharePointObject.Builder(SharePointObject.NAMED_RESOURCE)
            .setSiteId(site.getMetadata().getID())
            .setObjectId(site.getMetadata().getID())
            .setUrl(siteAdminFragmentId)
            .build();
    entries.add(
        new PushEntry().setId(siteAdminFragmentId).encodePayload(siteAdminObject.encodePayload()));
    if (!sharepointConfiguration.isSiteCollectionUrl()) {
      siteAdmins.setInheritFrom(VIRTUAL_SERVER_ID);
      siteAdmins.setInheritanceType(InheritanceType.PARENT_OVERRIDE);
      item.setContainer(VIRTUAL_SERVER_ID);
    }

    Acl itemAcl =
        new Acl.Builder()
            .setReaders(scConnector.getWebAcls(rootWeb))
            .setInheritanceType(InheritanceType.PARENT_OVERRIDE)
            .setInheritFrom(entry.getId(), SITE_COLLECTION_ADMIN_FRAGMENT)
            .build();
    itemAcl.applyTo(item);
    entries.addAll(getChildWebEntries(scConnector, site.getMetadata().getID(), rootWeb));
    entries.addAll(getChildListEntries(scConnector, site.getMetadata().getID(), rootWeb));

    RepositoryDoc doc =
        new RepositoryDoc.Builder()
            .setItem(item)
            .setAclFragments(
                Collections.singletonMap(SITE_COLLECTION_ADMIN_FRAGMENT, siteAdmins.build()))
            .setChildIds(entries)
            .build();
    batchRequest.add(doc);
    return ApiOperations.batch(batchRequest.iterator());
  }

  private ApiOperation getWebDocContent(
      QueueEntry entry, SiteConnector scConnector, SharePointObject webObject) throws IOException {
    Web currentWeb = scConnector.getSiteDataClient().getContentWeb();
    String parentWebUrl = scConnector.getWebParentUrl();
    SiteConnector parentSiteConnector = getSiteConnector(scConnector.getSiteUrl(), parentWebUrl);
    Web parentWeb = parentSiteConnector.getSiteDataClient().getContentWeb();
    boolean inheritPermissions =
        Objects.equals(currentWeb.getMetadata().getScopeID(), parentWeb.getMetadata().getScopeID());
    Item item = new Item().setId(entry.getId()).setContainer(parentWebUrl);
    Acl.Builder aclBuilder = new Acl.Builder().setInheritanceType(InheritanceType.PARENT_OVERRIDE);
    if (inheritPermissions) {
      aclBuilder.setInheritFrom(parentWebUrl);
    } else {
      aclBuilder.setReaders(scConnector.getWebAcls(currentWeb));
      aclBuilder.setInheritFrom(scConnector.getSiteUrl(), SITE_COLLECTION_ADMIN_FRAGMENT);
    }
    aclBuilder.build().applyTo(item);
    List<PushEntry> entries = new ArrayList<>();
    entries.addAll(getChildWebEntries(scConnector, webObject.getSiteId(), currentWeb));
    entries.addAll(getChildListEntries(scConnector, webObject.getSiteId(), currentWeb));
    return new RepositoryDoc.Builder().setItem(item).setChildIds(entries).build();
  }

  private ApiOperation getListDocContent(
      QueueEntry entry, SiteConnector scConnector, SharePointObject listObject) throws IOException {
    com.microsoft.schemas.sharepoint.soap.List l =
        scConnector.getSiteDataClient().getContentList(listObject.getListId());
    String rootFolder = l.getMetadata().getRootFolder();
    if (Strings.isNullOrEmpty(rootFolder)) {
      return ApiOperations.deleteItem(entry.getId());
    }

    String rootFolderDocId = scConnector.encodeDocId(rootFolder);
    Item rootFolderItem =
        new Item()
            .setId(rootFolderDocId)
            .setId(rootFolderDocId)
            .setContainer(scConnector.getWebUrl());
    SharePointObject rootFolderPayload =
        new SharePointObject.Builder(SharePointObject.NAMED_RESOURCE)
            .setSiteId(listObject.getSiteId())
            .setWebId(listObject.getWebId())
            .setUrl(rootFolderDocId)
            .setListId(listObject.getListId())
            .setObjectId(listObject.getListId())
            .build();
    PushEntry rootFolderEntry =
        new PushEntry().setId(rootFolderDocId).encodePayload(rootFolderPayload.encodePayload());
    Web w = scConnector.getSiteDataClient().getContentWeb();
    String scopeId = l.getMetadata().getScopeID().toLowerCase(Locale.ENGLISH);
    String webScopeId = w.getMetadata().getScopeID().toLowerCase(Locale.ENGLISH);
    Acl.Builder rootFolderAcl =
        new Acl.Builder().setInheritanceType(InheritanceType.PARENT_OVERRIDE);
    if (scopeId.equals(webScopeId)) {
      rootFolderAcl.setInheritFrom(scConnector.getWebUrl());
    } else {
      rootFolderAcl.setReaders(scConnector.getListAcl(l));
      rootFolderAcl.setInheritFrom(scConnector.getSiteUrl(), SITE_COLLECTION_ADMIN_FRAGMENT);
    }
    rootFolderAcl.build().applyTo(rootFolderItem);
    RepositoryDoc listRootDoc =
        new RepositoryDoc.Builder()
            .setItem(rootFolderItem)
            .setChildIds(Collections.singletonList(rootFolderEntry))
            .build();
    Item listItem = new Item().setId(entry.getId()).setContainer(rootFolderDocId);
    new Acl.Builder()
        .setInheritanceType(InheritanceType.PARENT_OVERRIDE)
        .setInheritFrom(rootFolderDocId)
        .build()
        .applyTo(listItem);
    String path =
        "/".equals(l.getMetadata().getDefaultViewUrl())
            ? l.getMetadata().getRootFolder()
            : l.getMetadata().getDefaultViewUrl();
    String displayUrl = scConnector.encodeDocId(path);
    listItem.setViewUrl(displayUrl);
    String lastModified = l.getMetadata().getLastModified();
    try {
      listItem.setContentModifiedTime(
          new DateTime(listLastModifiedDateFormat.get().parse(lastModified)));
    } catch (ParseException ex) {
      log.log(Level.INFO, "Could not parse LastModified: {0}", lastModified);
    }
    RepositoryDoc listDoc =
        new RepositoryDoc.Builder()
            .setItem(listItem)
            .setChildIds(processFolder(scConnector, listObject.getListId(), "", listObject))
            .build();
    List<ApiOperation> operations = Arrays.asList(listRootDoc, listDoc);
    return ApiOperations.batch(operations.iterator());
  }

  private ApiOperation getListItemDocContent(
      QueueEntry entry, SiteConnector scConnector, SharePointObject itemObject) throws IOException {
    Holder<String> listId = new Holder<String>();
    Holder<String> itemId = new Holder<String>();
    boolean result = scConnector.getSiteDataClient().getUrlSegments(entry.getId(), listId, itemId);
    if (!result || itemId.value == null || listId.value == null) {
      log.log(
          Level.WARNING, "Unable to identify itemId for Item {0}. Deleting item", entry.getId());
      return ApiOperations.deleteItem(entry.getId());
    }
    Item item = new Item().setId(entry.getId());
    ItemData i = scConnector.getSiteDataClient().getContentItem(listId.value, itemId.value);

    Xml xml = i.getXml();
    Element data = getFirstChildWithName(xml, DATA_ELEMENT);
    Element row = getChildrenWithName(data, ROW_ELEMENT).get(0);

    String modifiedString = row.getAttribute(OWS_MODIFIED_ATTRIBUTE);
    if (modifiedString == null) {
      log.log(Level.FINE, "No last modified information for list item");
    } else {
      try {
        item.setContentModifiedTime(new DateTime(modifiedDateFormat.get().parse(modifiedString)));
      } catch (ParseException ex) {
        log.log(Level.INFO, "Could not parse ows_Modified: {0}", modifiedString);
      }
    }
    com.microsoft.schemas.sharepoint.soap.List l =
        scConnector.getSiteDataClient().getContentList(listId.value);
    // This should be in the form of "1234;#{GUID}". We want to extract the
    // {GUID}.
    String scopeId = row.getAttribute(OWS_SCOPEID_ATTRIBUTE).split(";#", 2)[1];
    scopeId = scopeId.toLowerCase(Locale.ENGLISH);
    String rawFileDirRef = row.getAttribute(OWS_FILEDIRREF_ATTRIBUTE);
    // This should be in the form of "1234;#site/list/path". We want to
    // extract the site/list/path. Path relative to host, even though it
    // doesn't have a leading '/'.
    String folderDocId = scConnector.encodeDocId("/" + rawFileDirRef.split(";#", 2)[1]);
    item.setContainer(folderDocId);
    String rootFolderDocId = scConnector.encodeDocId(l.getMetadata().getRootFolder());
    // If the parent is a list, folderDocId will be same as
    // rootFolderDocId. If inheritance chain is not
    // broken, item will inherit its permission from list root folder.
    // If parent is a folder, item will inherit its permissions from parent
    // folder.
    boolean parentIsList = folderDocId.equals(rootFolderDocId);
    String parentScopeId;
    // If current item has same scope id as list then inheritance is not
    // broken irrespective of current item is inside folder or not.
    String listScopeId = l.getMetadata().getScopeID().toLowerCase(Locale.ENGLISH);
    if (parentIsList || scopeId.equals(listScopeId)) {
      parentScopeId = listScopeId;
    } else {
      // Instead of using getUrlSegments and getContent(ListItem), we could
      // use just getContent(Folder). However, getContent(Folder) always
      // returns children which could make the call very expensive. In
      // addition, getContent(ListItem) returns all the metadata for the
      // folder instead of just its scope so if in the future we need more
      // metadata we will already have it. GetContentEx(Folder) may provide
      // a way to get the folder's scope without its children, but it wasn't
      // investigated.
      Holder<String> folderListId = new Holder<String>();
      Holder<String> folderItemId = new Holder<String>();
      boolean folderResult =
          scConnector.getSiteDataClient().getUrlSegments(folderDocId, folderListId, folderItemId);
      if (!folderResult) {
        throw new IOException("Could not find parent folder's itemId");
      }
      if (!listId.value.equals(folderListId.value)) {
        throw new AssertionError("Unexpected listId value");
      }
      ItemData folderItem =
          scConnector.getSiteDataClient().getContentItem(listId.value, folderItemId.value);
      Element folderData = getFirstChildWithName(folderItem.getXml(), DATA_ELEMENT);
      Element folderRow = getChildrenWithName(folderData, ROW_ELEMENT).get(0);
      parentScopeId =
          folderRow
              .getAttribute(OWS_SCOPEID_ATTRIBUTE)
              .split(";#", 2)[1]
              .toLowerCase(Locale.ENGLISH);
    }
    Acl.Builder aclBuilder = new Acl.Builder().setInheritanceType(InheritanceType.PARENT_OVERRIDE);
    if (scopeId.equals(parentScopeId)) {
      aclBuilder.setInheritFrom(folderDocId);
    } else {
      // We have to search for the correct scope within the scopes element.
      // The scope provided in the metadata is for the parent list, not for
      // the item
      Scopes scopes = getFirstChildOfType(xml, Scopes.class);
      boolean hasAcl = false;
      assert scopes != null;
      for (Scopes.Scope scope : scopes.getScope()) {
        if (scope.getId().toLowerCase(Locale.ENGLISH).equals(scopeId)) {
          aclBuilder
              .setReaders(scConnector.getScopeAcl(scope))
              .setInheritFrom(scConnector.getSiteUrl(), SITE_COLLECTION_ADMIN_FRAGMENT);
          hasAcl = true;
          break;
        }
      }
      if (!hasAcl) {
        throw new IOException("Unable to find permission scope for item: " + entry.getId());
      }
    }
    aclBuilder.build().applyTo(item);
    // This should be in the form of "1234;#0". We want to extract the 0.
    String type = row.getAttribute(OWS_FSOBJTYPE_ATTRIBUTE).split(";#", 2)[1];
    boolean isFolder = "1".equals(type);
    String serverUrl = row.getAttribute(OWS_SERVERURL_ATTRIBUTE);
    if (serverUrl.contains("&") || serverUrl.contains("=") || serverUrl.contains("%")) {
      throw new AssertionError();
    }
    if (isFolder) {
      String root = scConnector.encodeDocId(l.getMetadata().getRootFolder());
      root += "/";
      String folder = scConnector.encodeDocId(serverUrl);
      if (!folder.startsWith(root)) {
        throw new AssertionError();
      }
      try {
        String defaultViewUrl = scConnector.encodeDocId(l.getMetadata().getDefaultViewUrl());
        URI displayPage = buildSharePointUrl(defaultViewUrl).getURI();
        // SharePoint percent-encodes '/'s in serverUrl, but accepts them
        // encoded or unencoded. We leave them unencoded for simplicity of
        // implementation and to not deal with the possibility of
        // double-encoding.
        URI displayUrl =
            new URI(
                displayPage.getScheme(),
                displayPage.getAuthority(),
                displayPage.getPath(),
                "RootFolder=" + serverUrl,
                null);
        item.setViewUrl(displayUrl.toString());
      } catch (URISyntaxException ex) {
        throw new IOException(ex);
      }
      List<PushEntry> entries =
          processAttachments(scConnector, listId.value, itemId.value, row, itemObject);
      entries.addAll(
          processFolder(scConnector, listId.value, folder.substring(root.length()), itemObject));
      return
          new RepositoryDoc.Builder().setItem(item).setChildIds(entries).build();
    }
    String contentTypeId = row.getAttribute(OWS_CONTENTTYPEID_ATTRIBUTE);
    boolean isDocument =
        contentTypeId != null && contentTypeId.startsWith(CONTENTTYPEID_DOCUMENT_PREFIX);
    RepositoryDoc.Builder docBuilder = new RepositoryDoc.Builder().setItem(item);
    if (isDocument) {
      docBuilder.setContent(getFileContent(entry.getId(), item, true));
    } else {
      String defaultViewItemUrl = scConnector.encodeDocId(l.getMetadata().getDefaultViewItemUrl());
      try {
        URI displayPage = buildSharePointUrl(defaultViewItemUrl).getURI();
        URI viewItemUri =
            new URI(
                displayPage.getScheme(),
                displayPage.getAuthority(),
                displayPage.getPath(),
                "ID=" + itemId.value,
                null);
        item.setViewUrl(viewItemUri.toString());
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
      List<PushEntry> entries =
          processAttachments(scConnector, listId.value, itemId.value, row, itemObject);
      docBuilder.setChildIds(entries);
      docBuilder.setContent(
          ByteArrayContent.fromString("text/plain", "List Item " + entry.getId()));
    }
    return docBuilder.build();
  }

  private SharePointUrl buildSharePointUrl(String url) throws URISyntaxException {
    return new SharePointUrl.Builder(url)
        .setPerformBrowserLeniency(performBrowserLeniency)
        .build();
  }

  private ApiOperation getAttachmentDocContent(
      QueueEntry entry, SiteConnector scConnector, SharePointObject itemObject) throws IOException {
    Holder<String> listId = new Holder<String>();
    Holder<String> itemId = new Holder<String>();
    boolean result =
        scConnector.getSiteDataClient().getUrlSegments(itemObject.getItemId(), listId, itemId);
    if (!result || itemId.value == null || listId.value == null) {
      log.log(
          Level.WARNING, "Unable to identify itemId for Item {0}. Deleting item", entry.getId());
      return ApiOperations.deleteItem(entry.getId());
    }
    ItemData itemData = scConnector.getSiteDataClient().getContentItem(listId.value, itemId.value);
    Xml xml = itemData.getXml();
    Element data = getFirstChildWithName(xml, DATA_ELEMENT);
    assert data != null;
    String itemCount = data.getAttribute("ItemCount");
    if ("0".equals(itemCount)) {
      log.fine("Could not get parent list item as ItemCount is 0.");
      // Returing false here instead of returing 404 to avoid wrongly
      // identifying file documents as attachments when DocumentLibrary has
      // folder name Attachments. Returing false here would allow code
      // to see if this document was a regular file in DocumentLibrary.
      return ApiOperations.deleteItem(entry.getId());
    }
    Element row = getChildrenWithName(data, ROW_ELEMENT).get(0);
    String strAttachments = row.getAttribute(OWS_ATTACHMENTS_ATTRIBUTE);
    int attachments =
        ((strAttachments == null) || "".equals(strAttachments))
            ? 0
            : Integer.parseInt(strAttachments);
    if (attachments <= 0) {
      // Either the attachment has been removed or there was a really odd
      // collection of documents in a Document Library. Therefore, we let the
      // code continue to try to determine if this is a File.
      log.fine("Parent list item has no child attachments");
      return ApiOperations.deleteItem(entry.getId());
    }
    Item attachmentItem = new Item().setId(entry.getId());
    AbstractInputStreamContent content = getFileContent(entry.getId(), attachmentItem, false);
    Acl acl =
        new Acl.Builder()
            .setInheritanceType(InheritanceType.PARENT_OVERRIDE)
            .setInheritFrom(itemObject.getItemId())
            .build();
    acl.applyTo(attachmentItem);
    return new RepositoryDoc.Builder().setItem(attachmentItem).setContent(content).build();
  }

  private List<PushEntry> getChildListEntries(
      SiteConnector scConnector, String siteId, Web parentWeb) throws IOException {
    List<PushEntry> entries = new ArrayList<PushEntry>();
    if (parentWeb.getLists() != null) {
      for (Lists.List list : parentWeb.getLists().getList()) {
        if ("".equals(list.getDefaultViewUrl())) {
          com.microsoft.schemas.sharepoint.soap.List l =
              scConnector.getSiteDataClient().getContentList(list.getID());
          log.log(
              Level.INFO,
              "Ignoring List {0} in {1}, since it has no default view URL",
              new Object[] {l.getMetadata().getTitle(), parentWeb.getMetadata().getURL()});
          continue;
        }
        String listUrl = scConnector.encodeDocId(list.getDefaultViewUrl());
        SharePointObject payload =
            new SharePointObject.Builder(SharePointObject.LIST)
                .setSiteId(siteId)
                .setWebId(parentWeb.getMetadata().getID())
                .setUrl(listUrl)
                .setListId(list.getID())
                .setObjectId(list.getID())
                .build();
        entries.add(new PushEntry().setId(listUrl).encodePayload(payload.encodePayload()));
      }
    }
    return entries;
  }

  private List<PushEntry> getChildWebEntries(
      SiteConnector scConnector, String siteId, Web parentweb) throws IOException {
    List<PushEntry> entries = new ArrayList<>();
    if (parentweb.getWebs() != null) {
      for (Webs.Web web : parentweb.getWebs().getWeb()) {
        String childWebUrl = getCanonicalUrl(web.getURL());
        childWebUrl = scConnector.encodeDocId(childWebUrl);
        SharePointObject payload =
            new SharePointObject.Builder(SharePointObject.WEB)
                .setSiteId(siteId)
                .setWebId(web.getID())
                .setUrl(childWebUrl)
                .setObjectId(web.getID())
                .build();
        entries.add(new PushEntry().setId(childWebUrl).encodePayload(payload.encodePayload()));
      }
    }
    return entries;
  }

  private List<PushEntry> processFolder(
      SiteConnector scConnector, String listGuid, String folderPath, SharePointObject reference)
      throws IOException {
    Paginator<ItemData> folderPaginator =
        scConnector.getSiteDataClient().getContentFolderChildren(listGuid, folderPath);
    ItemData folder;
    List<PushEntry> entries = new ArrayList<PushEntry>();
    while ((folder = folderPaginator.next()) != null) {
      Xml xml = folder.getXml();

      Element data = getFirstChildWithName(xml, DATA_ELEMENT);
      for (Element row : getChildrenWithName(data, ROW_ELEMENT)) {
        String rowUrl = row.getAttribute(OWS_SERVERURL_ATTRIBUTE);
        String itemId = scConnector.encodeDocId(getCanonicalUrl(rowUrl));
        SharePointObject payload =
            new SharePointObject.Builder(SharePointObject.LIST_ITEM)
                .setListId(listGuid)
                .setSiteId(reference.getSiteId())
                .setWebId(reference.getWebId())
                .setUrl(itemId)
                .setObjectId("item")
                .build();
        entries.add(new PushEntry().setId(itemId).encodePayload(payload.encodePayload()));
      }
    }
    return entries;
  }

  private List<PushEntry> processAttachments(
      SiteConnector scConnector,
      String listId,
      String itemId,
      Element row,
      SharePointObject reference)
      throws IOException {
    List<PushEntry> entries = new ArrayList<>();
    String strAttachments = row.getAttribute(OWS_ATTACHMENTS_ATTRIBUTE);
    int attachments =
        (strAttachments == null || "".equals(strAttachments))
            ? 0
            : Integer.parseInt(strAttachments);
    if (attachments > 0) {
      SharePointObject.Builder payloadBuilder =
          new SharePointObject.Builder(SharePointObject.ATTACHMENT)
              .setSiteId(reference.getSiteId())
              .setWebId(reference.getWebId())
              .setListId(listId)
              .setItemId(reference.getUrl());
      com.microsoft.schemas.sharepoint.soap.Item item =
          scConnector.getSiteDataClient().getContentListItemAttachments(listId, itemId);

      for (com.microsoft.schemas.sharepoint.soap.Item.Attachment attachment :
          item.getAttachment()) {

        String attachmentUrl = scConnector.encodeDocId(attachment.getURL());
        payloadBuilder.setUrl(attachmentUrl).setObjectId(attachmentUrl);
        entries.add(
            new PushEntry()
                .setId(attachmentUrl)
                .encodePayload(payloadBuilder.build().encodePayload()));
      }
    }
    return entries;
  }

  private AbstractInputStreamContent getFileContent(
      String fileUrl, Item item, boolean setLastModified) throws IOException {
    SharePointUrl sharepointFileUrl;
    try {
      sharepointFileUrl =
          buildSharePointUrl(fileUrl);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    item.setViewUrl(fileUrl);
    String filePath = sharepointFileUrl.getURI().getPath();
    String fileExtension = "";
    if (filePath.lastIndexOf('.') > 0) {
      fileExtension = filePath.substring(filePath.lastIndexOf('.')).toLowerCase(Locale.ENGLISH);
    }
    FileInfo fi = httpClient.issueGetRequest(sharepointFileUrl.toURL());
    if (FILE_EXTENSION_TO_MIME_TYPE_MAPPING.containsKey(fileExtension)) {
      String contentType = FILE_EXTENSION_TO_MIME_TYPE_MAPPING.get(fileExtension);
      log.log(
          Level.FINER,
          "Overriding content type as {0} for file extension {1}",
          new Object[] {contentType, fileExtension});
      item.setMimeType(contentType);
    } else {
      String contentType = fi.getFirstHeaderWithName("Content-Type");
      if (contentType != null) {
        String lowerType = contentType.toLowerCase(Locale.ENGLISH);
        if (MIME_TYPE_MAPPING.containsKey(lowerType)) {
          contentType = MIME_TYPE_MAPPING.get(lowerType);
        }
        item.setMimeType(contentType);
      }
    }
    String lastModifiedString = fi.getFirstHeaderWithName("Last-Modified");
    if (lastModifiedString != null && setLastModified) {
      try {
        item.setContentModifiedTime(
            new DateTime(dateFormatRfc1123.get().parse(lastModifiedString)));
      } catch (ParseException ex) {
        log.log(Level.INFO, "Could not parse Last-Modified: {0}", lastModifiedString);
      }
    }
    AbstractInputStreamContent content =
        new ByteArrayContent(item.getMimeType(), ByteStreams.toByteArray(fi.getContents()));
    try {
      fi.getContents().close();
    } catch (IOException e) {
      log.log(Level.WARNING, "Could not close content stream", e);
    }
    return content;
  }

  private static boolean elementHasName(Element ele, QName name) {
    return name.getLocalPart().equals(ele.getLocalName())
        && name.getNamespaceURI().equals(ele.getNamespaceURI());
  }

  private static Element getFirstChildWithName(Xml xml, QName name) {
    for (Object oChild : xml.getAny()) {
      if (!(oChild instanceof Element)) {
        continue;
      }
      Element child = (Element) oChild;
      if (elementHasName(child, name)) {
        return child;
      }
    }
    return null;
  }

  private static <T> T getFirstChildOfType(Xml xml, Class<T> type) {
    for (Object oChild : xml.getAny()) {
      if (!type.isInstance(oChild)) {
        continue;
      }
      return type.cast(oChild);
    }
    return null;
  }

  private static List<Element> getChildrenWithName(Element ele, QName name) {
    List<Element> l = new ArrayList<Element>();
    NodeList nl = ele.getChildNodes();
    for (int i = 0; i < nl.getLength(); i++) {
      Node n = nl.item(i);
      if (!(n instanceof Element)) {
        continue;
      }
      Element child = (Element) n;
      if (elementHasName(child, name)) {
        l.add(child);
      }
    }
    return l;
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

  //Remove trailing slash from URLs as SharePoint doesn't like trailing slash
  // in SiteData.GetUrlSegments
  private static String getCanonicalUrl(String url) {
    if (!url.endsWith("/")) {
      return url;
    }
    return url.substring(0, url.length() - 1);
  }

  private static class NtlmAuthenticator extends Authenticator {
    private final String username;
    private final char[] password;
    private final Set<String> permittedHosts = new HashSet<String>();

    NtlmAuthenticator(String username, String password) {
      this.username = username;
      this.password = password.toCharArray();
    }

    void addPermitForHost(URL urlContainingHost) {
      permittedHosts.add(urlToHostString(urlContainingHost));
    }

    private boolean isPermittedHost(URL toVerify) {
      return permittedHosts.contains(urlToHostString(toVerify));
    }

    private String urlToHostString(URL url) {
      // If the port is missing (so that the default is used), we replace it
      // with the default port for the protocol in order to prevent being able
      // to prevent being tricked into connecting to a different port (consider
      // being configured for https, but then getting tricked to use http and
      // evenything being in the clear).
      return ""
          + url.getHost()
          + ":"
          + (url.getPort() != -1 ? url.getPort() : url.getDefaultPort());
    }

    @Override
    protected PasswordAuthentication getPasswordAuthentication() {
      URL url = getRequestingURL();
      if (isPermittedHost(url)) {
        return new PasswordAuthentication(username, password);
      } else {
        return super.getPasswordAuthentication();
      }
    }
  }

  @VisibleForTesting
  static class SharePointConfiguration {
    private final SharePointUrl sharePointUrl;
    private final String virtualServerUrl;
    private final boolean siteCollectionOnly;
    private final Set<String> siteCollectionsToInclude;

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SharePointConfiguration that = (SharePointConfiguration) o;
      return siteCollectionOnly == that.siteCollectionOnly &&
          Objects.equals(sharePointUrl, that.sharePointUrl) &&
          Objects.equals(virtualServerUrl, that.virtualServerUrl) &&
          Objects.equals(siteCollectionsToInclude, that.siteCollectionsToInclude);
    }

    @Override
    public int hashCode() {
      return Objects
          .hash(sharePointUrl, virtualServerUrl, siteCollectionOnly, siteCollectionsToInclude);
    }

    private static class Builder {
      private SharePointUrl sharePointUrl;
      private String sharePointSiteCollectionOnly = "";
      private Set<String> siteCollectionsToInclude = new HashSet<String>();

      Builder(SharePointUrl sharePointUrl) {
        this.sharePointUrl = sharePointUrl;
      }

      Builder setSharePointSiteCollectionOnly(String sharePointSiteCollectionOnly) {
        this.sharePointSiteCollectionOnly = sharePointSiteCollectionOnly;
        return this;
      }

      @SuppressWarnings("unused")
      Builder setSiteCollectionsToInclude(Set<String> siteCollectionsToInclude) {
        this.siteCollectionsToInclude = siteCollectionsToInclude;
        return this;
      }

      SharePointConfiguration build() throws URISyntaxException {
        if (sharePointUrl == null
            || sharePointSiteCollectionOnly == null
            || siteCollectionsToInclude == null) {
          throw new InvalidConfigurationException();
        }
        sharePointSiteCollectionOnly = sharePointSiteCollectionOnly.trim();
        return new SharePointConfiguration(this);
      }

    }
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

    @Override
    public String toString() {
      return String.format(
          "SharePointConfiguration("
              + "SharePointUrl %s VirtualServer "
              + "%s SiteCllectionOnly %s SiteCollectionsToInclude %s)",
          sharePointUrl, virtualServerUrl, siteCollectionOnly, siteCollectionsToInclude);
    }
  }
}
