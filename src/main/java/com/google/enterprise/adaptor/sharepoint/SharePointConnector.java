// Copyright 2012 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.enterprise.adaptor.sharepoint;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.util.DateTime;
import com.google.api.services.springboardindex.model.ExternalGroup;
import com.google.api.services.springboardindex.model.PollQueueRequest;
import com.google.api.services.springboardindex.model.Principal;
import com.google.api.services.springboardindex.model.PushEntry;
import com.google.api.services.springboardindex.model.QueueEntry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.primitives.Ints;
import com.google.enterprise.adaptor.sharepoint.ActiveDirectoryClientFactory.ActiveDirectoryClientFactoryImpl;
import com.google.enterprise.adaptor.sharepoint.RareModificationCache.CachedList;
import com.google.enterprise.adaptor.sharepoint.RareModificationCache.CachedVirtualServer;
import com.google.enterprise.adaptor.sharepoint.RareModificationCache.CachedWeb;
import com.google.enterprise.adaptor.sharepoint.SamlAuthenticationHandler.SamlHandshakeManager;
import com.google.enterprise.adaptor.sharepoint.SiteDataClient.CursorPaginator;
import com.google.enterprise.adaptor.sharepoint.SiteDataClient.Paginator;
import com.google.enterprise.adaptor.sharepoint.SiteDataClient.WebServiceIOException;
import com.google.enterprise.adaptor.sharepoint.SiteDataClient.XmlProcessingException;
import com.google.enterprise.springboard.sdk.Acl;
import com.google.enterprise.springboard.sdk.Application;
import com.google.enterprise.springboard.sdk.Config;
import com.google.enterprise.springboard.sdk.Connector;
import com.google.enterprise.springboard.sdk.ConnectorContext;
import com.google.enterprise.springboard.sdk.ContentTemplate;
import com.google.enterprise.springboard.sdk.IncrementalChangeHandler;
import com.google.enterprise.springboard.sdk.IndexingService;
import com.google.enterprise.springboard.sdk.InvalidConfigurationException;
import com.google.enterprise.springboard.sdk.ItemRetriever;
import com.google.enterprise.springboard.sdk.traverser.TraverserConfiguration;
import com.microsoft.schemas.sharepoint.soap.ContentDatabase;
import com.microsoft.schemas.sharepoint.soap.ContentDatabases;
import com.microsoft.schemas.sharepoint.soap.Files;
import com.microsoft.schemas.sharepoint.soap.FolderData;
import com.microsoft.schemas.sharepoint.soap.Folders;
import com.microsoft.schemas.sharepoint.soap.GroupMembership;
import com.microsoft.schemas.sharepoint.soap.Item;
import com.microsoft.schemas.sharepoint.soap.ItemData;
import com.microsoft.schemas.sharepoint.soap.Lists;
import com.microsoft.schemas.sharepoint.soap.ObjectType;
import com.microsoft.schemas.sharepoint.soap.Permission;
import com.microsoft.schemas.sharepoint.soap.PolicyUser;
import com.microsoft.schemas.sharepoint.soap.SPContentDatabase;
import com.microsoft.schemas.sharepoint.soap.SPList;
import com.microsoft.schemas.sharepoint.soap.SPListItem;
import com.microsoft.schemas.sharepoint.soap.SPSite;
import com.microsoft.schemas.sharepoint.soap.SPWeb;
import com.microsoft.schemas.sharepoint.soap.Scopes;
import com.microsoft.schemas.sharepoint.soap.Site;
import com.microsoft.schemas.sharepoint.soap.SiteDataSoap;
import com.microsoft.schemas.sharepoint.soap.Sites;
import com.microsoft.schemas.sharepoint.soap.TrueFalseType;
import com.microsoft.schemas.sharepoint.soap.UserDescription;
import com.microsoft.schemas.sharepoint.soap.VirtualServer;
import com.microsoft.schemas.sharepoint.soap.Web;
import com.microsoft.schemas.sharepoint.soap.Webs;
import com.microsoft.schemas.sharepoint.soap.Xml;
import com.microsoft.schemas.sharepoint.soap.authentication.AuthenticationSoap;
import com.microsoft.schemas.sharepoint.soap.directory.GetUserCollectionFromSiteResponse;
import com.microsoft.schemas.sharepoint.soap.directory.GetUserCollectionFromSiteResponse.GetUserCollectionFromSiteResult;
import com.microsoft.schemas.sharepoint.soap.directory.User;
import com.microsoft.schemas.sharepoint.soap.directory.UserGroupSoap;
import com.microsoft.schemas.sharepoint.soap.people.ArrayOfPrincipalInfo;
import com.microsoft.schemas.sharepoint.soap.people.ArrayOfString;
import com.microsoft.schemas.sharepoint.soap.people.PeopleSoap;
import com.microsoft.schemas.sharepoint.soap.people.PrincipalInfo;
import com.microsoft.schemas.sharepoint.soap.people.SPPrincipalType;
import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.PasswordAuthentication;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.namespace.QName;
import javax.xml.ws.BindingProvider;
import javax.xml.ws.EndpointReference;
import javax.xml.ws.Holder;
import javax.xml.ws.Service;
import javax.xml.ws.WebServiceException;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.wsaddressing.W3CEndpointReferenceBuilder;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/** SharePoint Connector for the GSA. */
public class SharePointConnector implements Connector, ItemRetriever, IncrementalChangeHandler {
  private static final String XMLNS_DIRECTORY =
      "http://schemas.microsoft.com/sharepoint/soap/directory/";

  /** SharePoint's namespace. */
  private static final String XMLNS = "http://schemas.microsoft.com/sharepoint/soap/";

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
   * Row attribute guaranteed to be in ListItem responses. See
   * http://msdn.microsoft.com/en-us/library/dd929205.aspx . Provides ability to
   * distinguish between folders and other list items.
   */
  private static final String OWS_FSOBJTYPE_ATTRIBUTE = "ows_FSObjType";
  private static final String OWS_AUTHOR_ATTRIBUTE = "ows_Author";
  /**
   * Row attribute that contains a URL-like string identifying the object.
   * Sometimes this can be modified (by turning spaces into %20 and the like) to
   * access the object. In general, this in the string we provide to SP to
   * resolve information about the object.
   */
  private static final String OWS_SERVERURL_ATTRIBUTE = "ows_ServerUrl";
  /**
   * Row attribute that contains a hierarchial hex number that describes the
   * type of object. See http://msdn.microsoft.com/en-us/library/aa543822.aspx
   * for more information about content type IDs.
   */
  private static final String OWS_CONTENTTYPEID_ATTRIBUTE = "ows_ContentTypeId";
  /**
   * Row attribute guaranteed to be in ListItem responses. See
   * http://msdn.microsoft.com/en-us/library/dd929205.aspx . Provides scope id
   * used for permissions. Note that the casing is different than documented;
   * this is simply because of a documentation bug.
   */
  private static final String OWS_SCOPEID_ATTRIBUTE = "ows_ScopeId";
  private static final String OWS_FILEDIRREF_ATTRIBUTE = "ows_FileDirRef";
  /**
   * As described at http://msdn.microsoft.com/en-us/library/aa543822.aspx .
   */
  private static final String CONTENTTYPEID_DOCUMENT_PREFIX = "0x0101";
  /** Provides the number of attachments the list item has. */
  private static final String OWS_ATTACHMENTS_ATTRIBUTE = "ows_Attachments";
  /** The last time metadata or content was modified. */
  private static final String OWS_MODIFIED_ATTRIBUTE = "ows_Modified";
  /**
   * Matches a SP-encoded value that contains one or more values. See {@link
   * SiteConnector.addMetadata}.
   */
  private static final Pattern ALTERNATIVE_VALUE_PATTERN = Pattern.compile("^\\d+;#");

  static final long LIST_ITEM_MASK =
      SPBasePermissions.OPEN | SPBasePermissions.VIEWPAGES | SPBasePermissions.VIEWLISTITEMS;

  private static final long READ_SECURITY_LIST_ITEM_MASK =
      SPBasePermissions.OPEN
          | SPBasePermissions.VIEWPAGES
          | SPBasePermissions.VIEWLISTITEMS
          | SPBasePermissions.MANAGELISTS;

  private static final long FULL_READ_PERMISSION_MASK = SPBasePermissions.OPEN
      | SPBasePermissions.VIEWLISTITEMS | SPBasePermissions.OPENITEMS
      | SPBasePermissions.VIEWVERSIONS | SPBasePermissions.VIEWPAGES
      | SPBasePermissions.VIEWUSAGEDATA | SPBasePermissions.BROWSEDIRECTORIES
      | SPBasePermissions.VIEWFORMPAGES | SPBasePermissions.ENUMERATEPERMISSIONS
      | SPBasePermissions.BROWSEUSERINFO | SPBasePermissions.USEREMOTEAPIS
      | SPBasePermissions.USECLIENTINTEGRATION;

  private static final int LIST_READ_SECURITY_ENABLED = 2;

  private static final String IDENTITY_CLAIMS_PREFIX = "i:0";

  private static final String OTHER_CLAIMS_PREFIX = "c:0";

  private static final String METADATA_OBJECT_TYPE = "google:objecttype";
  private static final String METADATA_PARENT_WEB_TITLE = "sharepoint:parentwebtitle";
  private static final String METADATA_LIST_GUID = "sharepoint:listguid";

  private static final Pattern METADATA_ESCAPE_PATTERN = Pattern.compile("_x([0-9a-f]{4})_");

  private static final Pattern INTEGER_PATTERN = Pattern.compile("[0-9]+");

  private static final String HTML_NAME = "[a-zA-Z:_][a-zA-Z:_0-9.-]*";
  private static final Pattern HTML_TAG_PATTERN =
      Pattern.compile(
          // Tag and attributes
          "<"
              + HTML_NAME
              + "(?:[ \n\t]+"
              + HTML_NAME
              + "="
              // Attribute values
              + "(?:'[^']*'|\"[^\"]*\"|[a-zA-Z0-9._:-]*))*[ \n\t]*/?>"
              // Close tags
              + "|</"
              + HTML_NAME
              + ">",
          Pattern.DOTALL);
  private static final Pattern HTML_ENTITY_PATTERN = Pattern.compile("&(#[0-9]+|[a-zA-Z0-9]+);");
  private static final Map<String, String> HTML_ENTITIES;
  static {
    HashMap<String, String> map = new HashMap<String, String>();
    map.put("quot", "\"");
    map.put("amp", "&");
    map.put("lt", "<");
    map.put("gt", ">");
    map.put("nbsp", "\u00a0");
    map.put("apos", "'");
    HTML_ENTITIES = Collections.unmodifiableMap(map);
  }

  private static final String SITE_COLLECTION_ADMIN_FRAGMENT = "admin";

  private static final int DEFAULT_MAX_REDIRECTS_TO_FOLLOW = 20;

  private int socketTimeoutMillis;
  private int readTimeOutMillis;
  private int maxRedirectsToFollow;

  // flag controls whether connector leniently corrects common URL mistakes
  // like presence of brackets "[]" in path or "_" in hostname. if true
  // also makes connector handle redirects instead of relying on default
  // Java HTTP redirects, because otherwise those URLs wouldn't receive
  // this lenient encoding treatment.
  private boolean performBrowserLeniency;

  private boolean performSidLookup;

  private IndexingService indexingService;

  /**
   * Mapping of mime-types used by SharePoint to ones that the GSA comprehends.
   */
  private static final Map<String, String> MIME_TYPE_MAPPING;
  static {
    Map<String, String> map = new HashMap<String, String>();
    // Mime types used by SharePoint that aren't IANA-registered.
    // Extension .xlsx
    map.put("application/vnd.ms-excel.12",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    // Extension .pptx
    map.put("application/vnd.ms-powerpoint.presentation.12", "application/"
        + "vnd.openxmlformats-officedocument.presentationml.presentation");
    // Extension .docx
    map.put("application/vnd.ms-word.document.12", "application/"
        + "vnd.openxmlformats-officedocument.wordprocessingml.document");
    // Extension .ppsm
    map.put("application/vnd.ms-powerpoint.show.macroEnabled.12", "application/"
        + "vnd.openxmlformats-officedocument.presentationml.presentation");
    // Extension .ppsx
    map.put("application/vnd.ms-powerpoint.show.12", "application/"
        + "vnd.openxmlformats-officedocument.presentationml.presentation");
    // Extension .pptm
    map.put("application/vnd.ms-powerpoint.macroEnabled.12", "application/"
        + "vnd.openxmlformats-officedocument.presentationml.presentation");
    // Extension .xlsm
    map.put("application/vnd.ms-excel.macroEnabled.12",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");

    // IANA-registered mime types unknown to GSA 7.2.
    // Extension .docm
    map.put("application/vnd.ms-word.document.macroEnabled.12", "application/"
        + "vnd.openxmlformats-officedocument.wordprocessingml.document");
    // Extension .pptm
    map.put(
        "application/vnd.ms-powerpoint.presentation.macroEnabled.12",
        "application/" + "vnd.openxmlformats-officedocument.presentationml.presentation");
    // Extension .xlsm
    map.put("application/vnd.ms-excel.sheet.macroEnabled.12",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");

    MIME_TYPE_MAPPING = Collections.unmodifiableMap(map);
  }

  private static final Map<String, String> FILE_EXTENSION_TO_MIME_TYPE_MAPPING;
  static {
    Map<String, String> map = new HashMap<String, String>();

    // Map .msg files to mime type application/vnd.ms-outlook
    map.put(".msg", "application/vnd.ms-outlook");

    FILE_EXTENSION_TO_MIME_TYPE_MAPPING = Collections.unmodifiableMap(map);
  }

  private static final Logger log = Logger.getLogger(SharePointConnector.class.getName());

  /** Map from Site or Web URL to SiteConnector object used to communicate with that Site/Web. */
  private final ConcurrentMap<String, SiteConnector> siteConnectors =
      new ConcurrentSkipListMap<String, SiteConnector>();
  private static final String VIRTUAL_SERVER_ID = "ROOT";
  /**
   * The URL of Virtual Server or Site Collection that we use to bootstrap our SP instance
   * knowledge.
   */
  private SharePointUrl sharePointUrl;
  /**
   * Cache that provides immutable {@link MemberIdMapping} instances for the provided site URL key.
   * Since {@code MemberIdMapping} is immutable, updating the cache creates new mapping instances
   * that replace the previous value.
   */
  private LoadingCache<String, MemberIdMapping> memberIdsCache =
      CacheBuilder.newBuilder()
          .refreshAfterWrite(30, TimeUnit.MINUTES)
          .expireAfterWrite(45, TimeUnit.MINUTES)
          .build(new MemberIdsCacheLoader());

  private LoadingCache<String, MemberIdMapping> siteUserCache =
      CacheBuilder.newBuilder()
          .refreshAfterWrite(30, TimeUnit.MINUTES)
          .expireAfterWrite(45, TimeUnit.MINUTES)
          .build(new SiteUserCacheLoader());
  private RareModificationCache rareModCache;
  /** Map from SharePoint Object GUID to last known Change Token for that object. */
  private final ConcurrentSkipListMap<String, String> objectGuidToChangeIdMapping =
      new ConcurrentSkipListMap<String, String>();
  private final SoapFactory soapFactory;
  /** Client for initiating raw HTTP connections. */
  private final HttpClient httpClient;
  private final Callable<ExecutorService> executorFactory;

  private final AuthenticationClientFactory authenticationClientFactory;
  private final ActiveDirectoryClientFactory adClientFactory;

  /** Executor service to perform background tasks */
  private ExecutorService executor;
  private boolean xmlValidation;
  private String sharepointUserAgent;

  private ScheduledThreadPoolExecutor scheduledExecutor;
  private String defaultNamespace;
  /** Authenticator instance that authenticates with SP. */
  /**
   * Cached value of whether we are talking to a SP 2007 server or not. This
   * value is used in case of error in certain situations.
   */
  private boolean isSp2007;
  private NtlmAuthenticator ntlmAuthenticator;
  private boolean needToResetDefaultAuthenticator;

  private FormsAuthenticationHandler authenticationHandler;
  private ActiveDirectoryClient adClient;
  private static final TimeZone gmt = TimeZone.getTimeZone("GMT");
  /** RFC 822 date format, as updated by RFC 1123. */
  private final ThreadLocal<DateFormat> dateFormatRfc1123 =
      new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
          DateFormat df = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.ENGLISH);
          df.setTimeZone(gmt);
          return df;
        }
      };

  private final ThreadLocal<DateFormat> modifiedDateFormat =
      new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
          DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
          df.setTimeZone(gmt);
          return df;
        }
      };
  private final ThreadLocal<DateFormat> listLastModifiedDateFormat =
      new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
          DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss'Z'", Locale.ENGLISH);
          df.setTimeZone(gmt);
          return df;
        }
      };

  public SharePointConnector() {
    this(new SoapFactoryImpl(), new HttpClientImpl(),
        new CachedThreadPoolFactory(), new AuthenticationClientFactoryImpl(),
        new ActiveDirectoryClientFactoryImpl());
  }

  @VisibleForTesting
  SharePointConnector(
      SoapFactory soapFactory,
      HttpClient httpClient,
      Callable<ExecutorService> executorFactory,
      AuthenticationClientFactory authenticationClientFactory,
      ActiveDirectoryClientFactory adClientFactory) {
    if (soapFactory == null
        || httpClient == null
        || executorFactory == null
        || authenticationClientFactory == null
        || adClientFactory == null) {
      throw new NullPointerException();
    }
    this.soapFactory = soapFactory;
    this.httpClient = httpClient;
    this.executorFactory = executorFactory;
    this.authenticationClientFactory = authenticationClientFactory;
    this.adClientFactory = adClientFactory;
  }

  /**
   * Method to cause static initialization of the class. Mainly useful to tests
   * so that the cost of initializing the class does not count toward the first
   * test case run.
   */
  @VisibleForTesting
  static void init() {}

  @Override
  public void initConfig(Config config) {
    boolean onWindows = System.getProperty("os.name").contains("Windows");
    config.addKey("sharepoint.server", null);
    // When running on Windows, Windows Authentication can log us in.
    config.addKey("sharepoint.username", onWindows ? "" : null);
    config.addKey("sharepoint.password", onWindows ? "" : null);
    // On any particular SharePoint instance, we expect that at least some
    // responses will not pass xml validation. We keep the option around to
    // allow us to improve the schema itself, but also allow enable users to
    // enable checking as a form of debugging.
    config.addKey("sharepoint.xmlValidation", "false");
    config.addKey("connector.namespace", "Default");
    // When running against ADFS authentication, set this to ADFS endpoint.
    config.addKey("sharepoint.sts.endpoint", "");
    // When running against ADFS authentication, set this to realm value.
    // Normally realm value is either http://sharepointurl/_trust or
    // urn:sharepointenv:com format. You can use
    // Get-SPTrustedIdentityTokenIssuer to get "DefaultProviderRealm" value
    config.addKey("sharepoint.sts.realm", "");
    // You can override default value of http://sharepointurl/_trust by
    // specifying this property.
    config.addKey("sharepoint.sts.trustLocation", "");
    // You can override default value of
    // http://sharepointurl/_layouts/Authenticate.aspx by specifying this
    // property.
    config.addKey("sharepoint.sts.login", "");
    // Set this to true when using Live authentication.
    config.addKey("sharepoint.useLiveAuthentication", "false");
    // Set this to specific user-agent value to be used by connector while making
    // request to SharePoint
    config.addKey("sharepoint.userAgent", "");
    // Set this to true when you want to index only single site collection
    config.addKey("sharepoint.siteCollectionOnly", "");
    // Set this to positive integer value to configure maximum number of
    // URL redirects allowed to download document contents.
    config.addKey("connector.maxRedirectsToFollow", "");
    // Set this to true when connector needs to encode redirect urls and perform
    // browser leniency for handling unsupported characters in urls.
    config.addKey("connector.lenientUrlRulesAndCustomRedirect", "true");
    config.addKey("sidLookup.host", "");
    config.addKey("sidLookup.port", "3268");
    config.addKey("sidLookup.username", "");
    config.addKey("sidLookup.password", "");
    config.addKey("sidLookup.method", "standard");
    // Set this to static factory method name which will return
    // custom SamlHandshakeManager object
    config.addKey("customSamlManager.configPrefix", "customSamlManager");
    config.addKey("sharepoint.customSamlManager", "");
    config.addKey("sharepoint.webservices.socketTimeoutSecs", "30");
    config.addKey("sharepoint.webservices.readTimeOutSecs", "180");
  }

  @Override
  public void init(ConnectorContext context) throws Exception {
    checkNotNull(context);
    context.registerTraverser(
        new TraverserConfiguration.Builder()
            .pollRequest(new PollQueueRequest().setStatus(Arrays.asList("modified", "unindexed")))
            .itemRetriever(this)
            .hostload(6)
            .timeout(3, TimeUnit.MINUTES)
            .build());
    this.indexingService = checkNotNull(context.getIndexingService());
    indexingService.unreserve("");
    Config config = context.getConfig();
    SharePointUrl configuredSharePointUrl =
        new SharePointUrl(config.getValue("sharepoint.server"),
            config.getValue("sharepoint.siteCollectionOnly"));
    String username = config.getValue("sharepoint.username");
    String password = config.getValue("sharepoint.password");
    xmlValidation = Boolean.parseBoolean(config.getValue("sharepoint.xmlValidation"));
    defaultNamespace = config.getValue("connector.namespace");
    String stsendpoint = config.getValue("sharepoint.sts.endpoint");
    String stsrealm = config.getValue("sharepoint.sts.realm");
    boolean useLiveAuthentication = Boolean.parseBoolean(
        config.getValue("sharepoint.useLiveAuthentication"));
    String customSamlManager = config.getValue("sharepoint.customSamlManager");
    socketTimeoutMillis =
        Integer.parseInt(config.getValue("sharepoint.webservices.socketTimeoutSecs")) * 1000;
    readTimeOutMillis =
        Integer.parseInt(config.getValue("sharepoint.webservices.readTimeOutSecs")) * 1000;
    sharepointUserAgent = config.getValue("sharepoint.userAgent").trim();
    String maxRedirectsToFollowStr = config.getValue("connector.maxRedirectsToFollow");
    performBrowserLeniency =
        Boolean.parseBoolean(config.getValue("connector.lenientUrlRulesAndCustomRedirect"));
    if (performBrowserLeniency && "".equals(maxRedirectsToFollowStr)) {
      maxRedirectsToFollow = DEFAULT_MAX_REDIRECTS_TO_FOLLOW;
    } else if (performBrowserLeniency) {
      if (!isNumeric(maxRedirectsToFollowStr)) {
        throw new InvalidConfigurationException(
            "Invalid configuration value "
                + "for maximum number of url redirects "
                + "allowed (connector.maxRedirectsToFollow).");
      } else {
        maxRedirectsToFollow = Integer.parseInt(maxRedirectsToFollowStr);
        if (maxRedirectsToFollow < 0) {
          throw new InvalidConfigurationException(
              "Invalid configuration value "
                  + "for maximum number of url redirects "
                  + "allowed (connector.maxRedirectsToFollow).");
        }
      }
    } else if (!performBrowserLeniency && !"".equals(maxRedirectsToFollowStr)) {
      throw new InvalidConfigurationException(
          "Unexpected configuration value "
              + "for maximum number of url redirects "
              + "allowed (connector.maxRedirectsToFollow), as connector is not "
              + "configured to perform browser leniency "
              + "(connector.lenientUrlRulesAndCustomRedirect).");
    } else if (!performBrowserLeniency && "".equals(maxRedirectsToFollowStr)) {
      maxRedirectsToFollow = -1;
    }

    String sidLookupHost = config.getValue("sidLookup.host");
    String sidLookupUsername = null;
    String sidLookupPassword = null;
    String sidLookupMethod = null;
    int sidLookupPort = 0;
    if (Strings.isNullOrEmpty(sidLookupHost)) {
      performSidLookup = false;
    } else {
      sidLookupUsername = config.getValue("sidLookup.username");
      sidLookupPassword = config.getValue("sidLookup.password");
      if ("".equals(sidLookupUsername) || "".equals(sidLookupPassword)) {
        throw new InvalidConfigurationException(
            "Connector is configured to "
                + "perfom SID based lookup. please specify valid username and "
                + "password to perform SID lookup");
      }
      sidLookupPort = Integer.parseInt(config.getValue("sidLookup.port"));
      sidLookupMethod = config.getValue("sidLookup.method");
      performSidLookup = true;
    }

    log.log(Level.CONFIG, "SharePoint Url: {0}", configuredSharePointUrl);
    log.log(Level.CONFIG, "Username: {0}", username);
    log.log(Level.CONFIG, "Password: {0}", password);
    log.log(Level.CONFIG, "Default Namespace: {0}", defaultNamespace);
    log.log(Level.CONFIG, "STS Endpoint: {0}", stsendpoint);
    log.log(Level.CONFIG, "STS Realm: {0}", stsrealm);
    log.log(Level.CONFIG, "Use Live Authentication: {0}",
        useLiveAuthentication);
    log.log(Level.CONFIG, "Custom SAML provider: {0}", customSamlManager);
    log.log(Level.CONFIG, "Connector user agent: {0}", sharepointUserAgent);
    log.log(Level.CONFIG, "Run in Site Collection Only mode: {0}",
        configuredSharePointUrl.isSiteCollectionUrl());
    log.log(Level.CONFIG, "Perform SID Lookup for domain groups: {0}",
        performSidLookup);
    if(performSidLookup) {
      log.log(Level.CONFIG, "SID Lookup Host: {0}", sidLookupHost);
      log.log(Level.CONFIG, "SID Lookup Username: {0}", sidLookupUsername);
      log.log(Level.CONFIG, "SID Lookup Password: {0}", sidLookupPassword);
      log.log(Level.CONFIG, "SID Lookup Port: {0}", sidLookupPort);
    }

    if (configuredSharePointUrl.isSiteCollectionUrl()) {
      log.info(
          "Connector is configured to use site collection only mode. "
              + "ACLs and anonymous access settings at web application policy "
              + "level will be ignored.");
    }

    ntlmAuthenticator = new NtlmAuthenticator(username, password);
    if (!"".equals(username) && !"".equals(password)) {
      // Unfortunately, this is a JVM-wide modification.
      Authenticator.setDefault(ntlmAuthenticator);
      needToResetDefaultAuthenticator = true;
    }

    URL virtualServerUrl =
        new URL(configuredSharePointUrl.getVirtualServerUrl());
    ntlmAuthenticator.addPermitForHost(virtualServerUrl);
    scheduledExecutor = new ScheduledThreadPoolExecutor(1);
    String authenticationType;
    if (!"".equals(customSamlManager)) {
      authenticationType = "Custom SAML Provider";
      // Creating map for config values instead of passing config object, as
      // it will be consumed by third party code. Also config object is
      // not immutable and there is a risk of possible alteration to values
      // from third party code.
      // TODO : Add helper method to config object to get map of configuration
      // values. Current helper method doesn't return decoded values.
      Map<String, String> connectorConfig =
          Collections.unmodifiableMap(
              config.getConfigByPrefix(config.getValue("customSamlManager.configPrefix")));
      SamlHandshakeManager manager =
          authenticationClientFactory.newCustomSamlAuthentication(
              customSamlManager, connectorConfig);
      authenticationHandler =
          new SamlAuthenticationHandler.Builder(username, password, scheduledExecutor, manager)
              .build();
    } else if (useLiveAuthentication)  {
      if ("".equals(username) || "".equals(password)) {
        throw new InvalidConfigurationException(
            "Connector is configured to "
                + "use Live authentication. Please specify valid username "
                + "and password.");
      }
      SamlHandshakeManager manager = authenticationClientFactory
          .newLiveAuthentication(configuredSharePointUrl.getVirtualServerUrl(),
              username, password);
      authenticationHandler =
          new SamlAuthenticationHandler.Builder(username, password, scheduledExecutor, manager)
              .build();
      authenticationType = "Live";
    } else if (!"".equals(stsendpoint) && !"".equals(stsrealm)) {
      if ("".equals(username) || "".equals(password)) {
        throw new InvalidConfigurationException(
            "Connector is configured to "
                + "use ADFS authentication. Please specify valid username "
                + "and password.");
      }
      SamlHandshakeManager manager = authenticationClientFactory
          .newAdfsAuthentication(configuredSharePointUrl.getSharePointUrl(),
              username, password, stsendpoint, stsrealm,
              config.getValue("sharepoint.sts.login"),
              config.getValue("sharepoint.sts.trustLocation"));
      authenticationHandler =
          new SamlAuthenticationHandler.Builder(username, password, scheduledExecutor, manager)
              .build();
      authenticationType = "ADFS";
    } else {
      AuthenticationSoap authenticationSoap = authenticationClientFactory
          .newSharePointFormsAuthentication(
              configuredSharePointUrl.getSharePointUrl(), username, password);
      if (!"".equals(sharepointUserAgent)) {
        ((BindingProvider) authenticationSoap).getRequestContext().put(
            MessageContext.HTTP_REQUEST_HEADERS, Collections.singletonMap(
                "User-Agent", Collections.singletonList(sharepointUserAgent)));
      }
      addSocketTimeoutConfiguration((BindingProvider) authenticationSoap);
      authenticationHandler = new SharePointFormsAuthenticationHandler
          .Builder(username, password, scheduledExecutor, authenticationSoap)
          .build();
      authenticationType = "SharePoint";
    }

    try {
      log.log(Level.INFO, "Using {0} authentication.", authenticationType);
      authenticationHandler.start();
    } catch (WebServiceException ex) {
      if (ex.getCause() instanceof UnknownHostException) {
        // This may be due to  DNS issue or transiant network error.
        // Just rethrow excption and allow connector to retry.
        throw new IOException(
            String.format(
                "Cannot find SharePoint server \"%s\" -- please make sure it is "
                    + "specified properly.",
                configuredSharePointUrl.getSharePointUrl()),
            ex);
      }
      if (ex.getCause() instanceof ConnectException
          || ex.getCause() instanceof SocketTimeoutException) {
        // SharePoint might be down. Just rethrow exception and allow connector to
        // retry. We get ConnectException when IIS / web site is down and
        // SocketTimeOutException when SharePoint server does not respond in
        // timely manner.
        throw new IOException(
            String.format(
                "Unable to connect to SharePoint server \"%s\" -- please make "
                    + "sure it is specified properly and is available.",
                configuredSharePointUrl.getSharePointUrl()),
            ex);
      }
      String adfsWarning =
          "ADFS".equals(authenticationType)
              ? " Also verify if stsendpoint and stsrealm is specified correctly "
                  + "and ADFS environment is available."
              : "";
      String warning =
          String.format(
              "Failed to initialize connector using %s authentication."
                  + " Please verify connector configuration for SharePoint url,"
                  + " username and password.%s",
              authenticationType, adfsWarning);
      throw new IOException(warning, ex);
    }

    try {
      executor = executorFactory.call();
      SiteConnector spConnector =
          getSiteConnector(
              configuredSharePointUrl.getSharePointUrl(),
              configuredSharePointUrl.getSharePointUrl());
      SiteDataClient sharePointSiteDataClient = spConnector.getSiteDataClient();
      rareModCache = new RareModificationCache(sharePointSiteDataClient, executor);
      if (performSidLookup) {
        adClient =
            adClientFactory.newActiveDirectoryClient(
                sidLookupHost,
                sidLookupPort,
                sidLookupUsername,
                sidLookupPassword,
                sidLookupMethod);
      }

      if (!configuredSharePointUrl.isSiteCollectionUrl()) {
        sharePointUrl = configuredSharePointUrl;
      } else {
        Site site = sharePointSiteDataClient.getContentSite();
        CursorPaginator<SPSite, String> changesPaginator =
            sharePointSiteDataClient.getChangesSPSite(
                site.getMetadata().getID(),
                site.getMetadata().getChangeId(), isSp2007);
        String siteCollectionUrl = null;
        SPSite spSite = changesPaginator.next();
        if (spSite != null) {
          SPSite.Site sc = spSite.getSite();
          if (sc != null && sc.getMetadata() != null) {
            siteCollectionUrl = spSite.getSite().getMetadata().getURL();
          }
        }
        if(Strings.isNullOrEmpty(siteCollectionUrl)){
          log.log(Level.WARNING, "Unable to get exact url for site "
              + "collection url. Using {0} instead.",
              site.getMetadata().getURL());
          siteCollectionUrl = site.getMetadata().getURL();
        }
        sharePointUrl = new SharePointUrl(siteCollectionUrl,
            config.getValue("sharepoint.siteCollectionOnly"));
        return;
      }
      // Test out configuration.
      VirtualServer vs = sharePointSiteDataClient.getContentVirtualServer();

      // Check Full Read permission for Connector User on SharePoint
      checkFullReadPermissionForConnectorUser(vs, username);

      String version = vs.getMetadata().getVersion();
      log.log(Level.INFO, "SharePoint Version : {0}", version);
      // Version is missing for SP 2007 (but its version is 12).
      // Version for SP2010 is 14. Version for SP2013 is 15.
      isSp2007 = (version == null);
      log.log(Level.FINE, "isSP2007 : {0}", isSp2007);

      boolean urlAvailableInAlternateAccessMapping = false;
      // Loop through all host-named site collections and add them to
      // whitelist for authenticator.
      for (ContentDatabases.ContentDatabase cdcd : vs.getContentDatabases().getContentDatabase()) {
        ContentDatabase cd;
        try {
          cd = sharePointSiteDataClient.getContentContentDatabase(
              cdcd.getID(), true);
        } catch (IOException ex) {
          log.log(Level.WARNING, "Failed to get sites for database: " + cdcd.getID(), ex);
          continue;
        }
        if (cd.getSites() == null) {
          continue;
        }
        for (Sites.Site siteListing : cd.getSites().getSite()) {
          String siteString = spConnector.encodeDocId(siteListing.getURL());
          if (sharePointUrl.getVirtualServerUrl()
              .equalsIgnoreCase(siteString)) {
            urlAvailableInAlternateAccessMapping = true;
          }
          ntlmAuthenticator.addPermitForHost(spUrlToUri(siteString).toURL());
        }
      }
      if (!urlAvailableInAlternateAccessMapping) {
        log.log(
            Level.WARNING,
            "Virtual Server URL {0} is not availble in "
                + "SharePoint Alternate Access Mapping as Public URL. "
                + "Due to this mismatch some of the connector functionality might "
                + "not work as expected. Also include / exclude patterns "
                + "configured on GSA as per Virtual server URL might not "
                + "work as expected. Please make sure that connector is configured "
                + "to use Public URL instead on internal URL.",
            configuredSharePointUrl.getVirtualServerUrl());
      }
    } catch (WebServiceIOException ex) {
      String warning;
      Throwable cause = ex.getCause();
      if (cause instanceof UnknownHostException) {
        warning = String.format("Cannot find SharePoint server \"%s\" -- "
            + "please make sure it is specified properly.",
            configuredSharePointUrl.getSharePointUrl());
        // Note: even this exception should not be treated as a "Permanent
        // configuration error" -- it can be caused by transient network down
        // or DNS down.
      } else if (username.equals("")) {
        warning = String.format("Cannot connect to server \"%s\" as the "
            + "current user.  Please make sure the server is specified "
            + "correctly, and that the user has sufficient permission to "
            + "access the SharePoint server.  If the SharePoint server is "
            + "currently down, please try again later.",
            configuredSharePointUrl.getSharePointUrl());
      } else {
        warning = String.format("Cannot connect to server \"%s\" as user "
            + "\"%s\" with the specified password.  Please make sure they are "
            + "specified correctly, and that the user has sufficient "
            + "permission to access the SharePoint server.  If the SharePoint "
            + "server is currently down, please try again later.",
            configuredSharePointUrl.getSharePointUrl(), username);
      }
      throw new IOException(warning, ex);
    } catch (Exception e) {
      // Don't leak the executor.
      destroy();
      throw e;
    }
  }

  @Override
  public void destroy() {
    shutdownExecutor(executor);
    shutdownExecutor(scheduledExecutor);
    executor = null;
    scheduledExecutor = null;
    rareModCache = null;
    if (needToResetDefaultAuthenticator) {
      // Reset authenticator
      Authenticator.setDefault(null);
    }
    ntlmAuthenticator = null;
  }
  /**
   * Method to check full read permission for connector user on SharePoint. Returns -1 if connector
   * user is not available in web application policy. Returns 0 if connector user is having other
   * than Full Read permission Returns 1 if connector user is having exact Full Read permission.
   */
  @VisibleForTesting
  static int checkFullReadPermissionForConnectorUser(VirtualServer vs, String username) {
    String connectorUser = getConnectorUser(username);
    if (connectorUser == null) {
      log.log(Level.WARNING, "Unable to get connector user name");
      return -1;
    }
    PolicyUser p = getPolicyUserForAdatorUser(vs, connectorUser);
    if (p == null) {
      log.log(
          Level.INFO,
          "Connector user [{0}] not available in web "
              + "application policy to verify permissions.",
          connectorUser);
      return -1;
    }
    if (isOtherThanFullReadForPolicyUser( p.getGrantMask().longValue())) {
      log.log(
          Level.WARNING,
          "Connector user [{0}] is not having exact full "
              + "read permission on SharePoint. Having excess or lesser "
              + "permissions on SharePoint for connector user might affect connector "
              + "functionality.",
          connectorUser);
      return 0;
    } else {
      log.log(
          Level.FINE,
          "Connector user [{0}] is having full read " + "permissions on SharePoint.",
          connectorUser);
      return 1;
    }
  }

  private static boolean isNumeric(String input) {
    if (input == null) {
      return false;
    }
    return null != Ints.tryParse(input);
  }
  private static PolicyUser getPolicyUserForAdatorUser(VirtualServer vs, String connectorUser) {
    if (connectorUser == null) {
      return null;
    }
    for(PolicyUser p : vs.getPolicies().getPolicyUser()) {
      String policyUser = decodeClaim(
          p.getLoginName(), p.getLoginName());
      if (policyUser == null) {
        // Un-supported claim type
        continue;
      }
      // parse out authentication provider for forms authenticated user
      policyUser = policyUser.substring(policyUser.indexOf(":") + 1);
      if (connectorUser.equalsIgnoreCase(policyUser)) {
        return p;
      }
    }
    return null;
  }

  private static boolean isOtherThanFullReadForPolicyUser(long permissionMask) {
    return (FULL_READ_PERMISSION_MASK != permissionMask);
  }

  @VisibleForTesting
  static String getConnectorUser(String username) {
    if (!"".equals(username)) {
      return username;
    }

    // USERNAME and USERDOMAIN environment variables are applicable for
    // Windows only. On non windows connector machine user name will
    // not be empty.
    // Using Ssytem.getenv instead on System.getProperty("user.name") because
    // System.getProperty("user.name") returns username without domain.
    if (System.getenv("USERDOMAIN") == null || System.getenv("USERNAME") == null) {
      return null;
    }
    return  System.getenv("USERDOMAIN") + "\\" + System.getenv("USERNAME");
  }

  private synchronized void shutdownExecutor(ExecutorService executor) {
    if (executor == null) {
      return;
    }
    executor.shutdown();
    try {
      executor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
    executor.shutdownNow();
  }

  @Override
  public void process(QueueEntry entry) throws IOException {
    long startMillis = System.currentTimeMillis();
    log.entering("SharePointConnector", "process", entry);
    SiteConnector connectorForDocId = getConnectorForDocId(entry);
    if (connectorForDocId == null) {
      log.log(Level.FINE, "responding not found as site adptor for {0} is null", entry);

      indexingService.deleteItem(entry.getId());
      log.exiting("SharePointConnector", "process");
      return;
    }

    if (entry.getId().equals(VIRTUAL_SERVER_ID)) {
      connectorForDocId.getVirtualServerDocContent();
    } else {
      connectorForDocId.getDocContent(entry);
    }
    log.log(
        Level.FINE,
        "Duration: getDocContent {0} : {1,number,#} ms",
        new Object[] {entry.getId(), System.currentTimeMillis() - startMillis});
    log.exiting("SharePointConnector", "process");
  }

  @Override
  public void traverse() throws InterruptedException, IOException {
    log.entering("SharePointConnector", "traverse");
    if (sharePointUrl.isSiteCollectionUrl()) {
      getDocIdsSiteCollectionOnly();
    } else {
      getDocIdsVirtualServer();
    }
    log.exiting("SharePointConnector", "traverse");
  }

  private void getDocIdsSiteCollectionOnly() throws IOException {
    log.entering("SharePointConnector", "getDocIdsSiteCollectionOnly");
    SiteConnector scConnector =
        getSiteConnector(sharePointUrl.getSharePointUrl(), sharePointUrl.getSharePointUrl());
    SiteDataClient scClient = scConnector.getSiteDataClient();
    Site site = scClient.getContentSite();
    String siteCollectionUrl = getCanonicalUrl(site.getMetadata().getURL());
    // Reset site collection URL instance to use correct URL.
    scConnector = getSiteConnector(siteCollectionUrl, siteCollectionUrl);
    String siteCollectionDocId = scConnector.encodeDocId(siteCollectionUrl);
    List<PushEntry> entries = Arrays.asList(new PushEntry().setId(siteCollectionDocId));
    indexingService.push(entries);
    Map<ExternalGroup, Collection<Principal>> groupDefs =
        new HashMap<ExternalGroup, Collection<Principal>>();
    groupDefs.putAll(scConnector.computeMembersForGroups(site.getGroups()));
    for (ExternalGroup group : groupDefs.keySet()) {
      indexingService.updateExternalGroup(group);
    }
    log.exiting("SharePointConnector", "getDocIdsSiteCollectionOnly");
  }

  private void getDocIdsVirtualServer() throws IOException {
    log.entering("SharePointConnector", "getDocIdsVirtualServer");
    SiteConnector vsConnector =
        getSiteConnector(sharePointUrl.getVirtualServerUrl(), sharePointUrl.getVirtualServerUrl());
    SiteDataClient vsClient = vsConnector.getSiteDataClient();
    List<PushEntry> entries = Arrays.asList(new PushEntry().setId(VIRTUAL_SERVER_ID));
    indexingService.push(entries);
    VirtualServer vs = vsClient.getContentVirtualServer();
    Map<ExternalGroup, Collection<Principal>> defs =
        new HashMap<ExternalGroup, Collection<Principal>>();
    for (ContentDatabases.ContentDatabase cdcd
        : vs.getContentDatabases().getContentDatabase()) {
      ContentDatabase cd;
      try {
        cd = vsClient.getContentContentDatabase(cdcd.getID(), true);
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
        ntlmAuthenticator.addPermitForHost(spUrlToUri(siteString).toURL());
        SiteConnector siteConnector = getSiteConnector(siteString, siteString);
        Site site;
        try {
          site = siteConnector.getSiteDataClient().getContentSite();
        } catch (IOException ex) {
          log.log(Level.WARNING, "Failed to get local groups for site: "
              + siteString, ex);
          continue;
        }
        Map<ExternalGroup, Collection<Principal>> siteDefs =
            siteConnector.computeMembersForGroups(site.getGroups());
        for (Map.Entry<ExternalGroup, Collection<Principal>> me : siteDefs.entrySet()) {
          defs.put(me.getKey(), me.getValue());
        }
      }
    }
    for (ExternalGroup group : defs.keySet()) {
      indexingService.updateExternalGroup(group);
    }
    log.exiting("SharePointConnector", "getDocIdsVirtualServer");
  }

  @Override
  public void handleIncrementalChanges() throws InterruptedException, IOException {
    log.entering("SharePointConnector", "getModifiedDocIds");
    if (sharePointUrl.isSiteCollectionUrl()) {
      getModifiedDocIdsSiteCollection();
    } else {
      getModifiedDocIdsVirtualServer();
    }
    log.exiting("SharePointConnector", "getModifiedDocIds");
  }

  private void getModifiedDocIdsVirtualServer() throws IOException {
    log.entering("SharePointConnector", "getModifiedDocIdsVirtualServer");
    SiteConnector siteConnector;
    try {
      siteConnector =
          getSiteConnector(
              sharePointUrl.getVirtualServerUrl(), sharePointUrl.getVirtualServerUrl());
    } catch (IOException ex) {
      // The call should never fail, and it is the only IOException-throwing
      // call that we can't recover from. Handling it this way allows us to
      // remove IOException from the signature and ensure that we handle the
      // exception gracefully throughout this method.
      throw new RuntimeException(ex);
    }
    SiteDataClient client = siteConnector.getSiteDataClient();
    VirtualServer vs = null;
    try {
      vs = client.getContentVirtualServer();
    } catch (IOException ex) {
      log.log(Level.WARNING, "Could not retrieve list of content databases",
          ex);
    }
    Set<String> discoveredContentDatabases;
    if (vs == null) {
      // Retrieving list of databases failed, but we can continue without it.
      discoveredContentDatabases = new HashSet<String>(objectGuidToChangeIdMapping.keySet());
    } else {
      discoveredContentDatabases = new HashSet<String>();
      if (vs.getContentDatabases() != null) {
        for (ContentDatabases.ContentDatabase cd
            : vs.getContentDatabases().getContentDatabase()) {
          discoveredContentDatabases.add(cd.getID());
        }
      }
    }
    List<PushEntry> pushEntries = new ArrayList<>();
    Set<String> knownContentDatabases = new HashSet<String>(objectGuidToChangeIdMapping.keySet());
    Set<String> removedContentDatabases = new HashSet<String>(knownContentDatabases);
    removedContentDatabases.removeAll(discoveredContentDatabases);
    Set<String> newContentDatabases = new HashSet<String>(discoveredContentDatabases);
    newContentDatabases.removeAll(knownContentDatabases);
    Set<String> updatedContentDatabases = new HashSet<String>(knownContentDatabases);
    updatedContentDatabases.retainAll(discoveredContentDatabases);
    if (!removedContentDatabases.isEmpty()
        || !newContentDatabases.isEmpty()) {
      pushEntries.add(new PushEntry().setId(VIRTUAL_SERVER_ID).setKind("modified"));
    }
    indexingService.push(pushEntries);
    for (String contentDatabase : removedContentDatabases) {
      objectGuidToChangeIdMapping.remove(contentDatabase);
    }
    for (String contentDatabase : newContentDatabases) {
      ContentDatabase cd;
      try {
        cd = client.getContentContentDatabase(contentDatabase, false);
      } catch (IOException ex) {
        log.log(Level.WARNING, "Could not retrieve change id for content "
            + "database: " + contentDatabase, ex);
        // Continue processing. Hope that next time works better.
        continue;
      }
      String changeId = cd.getMetadata().getChangeId();
      objectGuidToChangeIdMapping.put(contentDatabase, changeId);
    }
    for (String contentDatabase : updatedContentDatabases) {

      String changeId = objectGuidToChangeIdMapping.get(contentDatabase);
      if (changeId == null) {
        // The item was removed from objectGuidToChangeIdMapping, so apparently
        // this database is gone.
        continue;
      }
      CursorPaginator<SPContentDatabase, String> changesPaginator =
          client.getChangesContentDatabase(contentDatabase, changeId, isSp2007);
      List<PushEntry> modifiedEntries = new ArrayList<>();
      Set<String> updatedSiteSecurity = new HashSet<String>();
      try {
        while (true) {
          try {
            SPContentDatabase changes = changesPaginator.next();
            if (changes == null) {
              break;
            }
            getModifiedDocIdsContentDatabase(changes, modifiedEntries, updatedSiteSecurity);
          } catch (XmlProcessingException ex) {
            log.log(Level.WARNING, "Error parsing changes from content "
                + "database: " + contentDatabase, ex);
            // The cursor is guaranteed to be advanced past the position that
            // failed parsing, so we just ignore the failure and continue
            // looping.
          }
          objectGuidToChangeIdMapping.put(contentDatabase,
              changesPaginator.getCursor());
        }
      } catch (IOException ex) {
        log.log(Level.WARNING, "Error getting changes from content database: "
            + contentDatabase, ex);
        // Continue processing. Hope that next time works better.
      }
      pushIncrementalUpdatesAndGroups(siteConnector, modifiedEntries, updatedSiteSecurity);

    }
    log.exiting("SharePointConnector", "getModifiedDocIdsVirtualServer");
  }

  private void pushIncrementalUpdatesAndGroups(
      SiteConnector siteConnector, List<PushEntry> pushEntries, Set<String> updatedSiteSecurity)
      throws IOException {
    indexingService.push(pushEntries);
    if (updatedSiteSecurity.isEmpty()) {
      return;
    }
    Map<ExternalGroup, Collection<Principal>> groupDefs =
        new HashMap<ExternalGroup, Collection<Principal>>();
    for (String siteUrl : updatedSiteSecurity) {
      Site site;
      try {
        site = getSiteConnector(siteUrl, siteUrl).getSiteDataClient().getContentSite();
      } catch (IOException ex) {
        log.log(Level.WARNING, "Failed to get local groups for site: " + siteUrl, ex);
        continue;
      }
      groupDefs.putAll(siteConnector.computeMembersForGroups(site.getGroups()));
    }
    for (ExternalGroup group : groupDefs.keySet()) {
      indexingService.updateExternalGroup(group);
    }
  }

  @VisibleForTesting
  void getModifiedDocIdsContentDatabase(
      SPContentDatabase changes,
      List<PushEntry> modifiedEntries,
      Collection<String> updatedSiteSecurity)
      throws IOException {
    log.entering(
        "SharePointConnector",
        "getModifiedDocIdsContentDatabase",
        new Object[] {changes, modifiedEntries});
    if (!"Unchanged".equals(changes.getChange())) {
      modifiedEntries.add(new PushEntry().setId(VIRTUAL_SERVER_ID).setKind("modified"));
    }
    List<SPSite> changedSites = changes.getSPSite();
    if (changedSites == null) {
      log.exiting("SharePointConnector", "getModifiedDocIdsContentDatabase");
      return;
    }
    for (SPSite site : changes.getSPSite()) {
      getModifiedDocIdsSite(site, modifiedEntries, updatedSiteSecurity);
    }
    log.exiting("SharePointConnector", "getModifiedDocIdsContentDatabase");
  }

  @VisibleForTesting
  void getModifiedDocIdsSiteCollection() throws IOException {
    SiteConnector siteConnector;
    try {
      siteConnector =
          getSiteConnector(sharePointUrl.getSharePointUrl(), sharePointUrl.getSharePointUrl());
    } catch (IOException ex) {
      // The call should never fail, and it is the only IOException-throwing
      // call that we can't recover from. Handling it this way allows us to
      // remove IOException from the signature and ensure that we handle the
      // exception gracefully throughout this method.
      throw new RuntimeException(ex);
    }

    SiteDataClient client = siteConnector.getSiteDataClient();
    Site site;
    try {
      site = client.getContentSite();
    } catch (IOException ex) {
      // If we can't get hold of Site object, we can not proceed with change
      // detection at Site Collection level. So we gracefully
      // handle IO exception here.
      throw new RuntimeException(ex);
    }
    if (site.getMetadata() == null
        || site.getMetadata().getID() == null
        || site.getMetadata().getChangeId() == null) {
      log.log(Level.WARNING, "Invalid Site object. Unable to propcess incremental updates.");
      return;
    }
    String siteId = site.getMetadata().getID();
    if (!objectGuidToChangeIdMapping.containsKey(siteId)) {
      objectGuidToChangeIdMapping.put(siteId, site.getMetadata().getChangeId());
    }

    List<PushEntry> modifiedEntries = new ArrayList<>();
    Set<String> updatedSiteSecurity = new HashSet<String>();
    try {
      CursorPaginator<SPSite, String> changesPaginator =
          client.getChangesSPSite(siteId, objectGuidToChangeIdMapping.get(siteId), isSp2007);
      while(true) {
        SPSite changes = changesPaginator.next();
        if (changes == null) {
          break;
        }
        getModifiedDocIdsSite(changes, modifiedEntries, updatedSiteSecurity);
        objectGuidToChangeIdMapping.put(siteId, changesPaginator.getCursor());
      }
    } catch (IOException ex) {
      log.log(
          Level.WARNING,
          "Error getting changes from Site Collection : " + site.getMetadata().getURL(),
          ex);
      // Continue processing. Hope that next time works better.
    }
    pushIncrementalUpdatesAndGroups(siteConnector, modifiedEntries, updatedSiteSecurity);
  }

  private void getModifiedDocIdsSite(
      SPSite changes, List<PushEntry> modifiedEntries, Collection<String> updatedSiteSecurity)
      throws IOException {
    log.entering(
        "SharePointConnector", "getModifiedDocIdsSite", new Object[] {changes, modifiedEntries});
    if (isModified(changes.getChange())) {
      String siteUrl = changes.getServerUrl() + changes.getDisplayUrl();
      siteUrl = getCanonicalUrl(siteUrl);
      modifiedEntries.add(new PushEntry().setId(siteUrl).setKind("modified"));
      // Add modified site to whitelist for authenticator as this might be new
      // host name site collection.
      ntlmAuthenticator.addPermitForHost(spUrlToUri(siteUrl).toURL());
      if ("UpdateSecurity".equals(changes.getChange())) {
        updatedSiteSecurity.add(siteUrl);
      }
    }
    List<SPWeb> changedWebs = changes.getSPWeb();
    if (changedWebs == null) {
      log.exiting("SharePointConnector", "getModifiedDocIdsSite");
      return;
    }
    for (SPWeb web : changedWebs) {
      getModifiedDocIdsWeb(web, modifiedEntries);
    }
    log.exiting("SharePointConnector", "getModifiedDocIdsSite");
  }

  private void getModifiedDocIdsWeb(SPWeb changes, List<PushEntry> modifiedEntries) {
    log.entering(
        "SharePointConnector", "getModifiedDocIdsWeb", new Object[] {changes, modifiedEntries});
    if (isModified(changes.getChange())) {
      String webUrl = changes.getServerUrl() + changes.getDisplayUrl();
      webUrl = getCanonicalUrl(webUrl);
      modifiedEntries.add(new PushEntry().setId(webUrl).setKind("modified"));
    }

    List<Object> spObjects = changes.getSPFolderOrSPListOrSPFile();
    if (spObjects == null) {
      log.exiting("SharePointConnector", "getModifiedDocIdsWeb");
      return;
    }
    for (Object choice : spObjects) {
      if (choice instanceof SPList) {
        getModifiedDocIdsList((SPList) choice, modifiedEntries);
      }
    }
    log.exiting("SharePointConnector", "getModifiedDocIdsWeb");
  }

  private void getModifiedDocIdsList(SPList changes, List<PushEntry> modifiedEntries) {
    log.entering(
        "SharePointConnector", "getModifiedDocIdsList", new Object[] {changes, modifiedEntries});
    if (isModified(changes.getChange())) {
      String listUrl = changes.getServerUrl() + changes.getDisplayUrl();
      modifiedEntries.add(new PushEntry().setId(listUrl).setKind("modified"));
    }
    List<Object> spObjects = changes.getSPViewOrSPListItem();
    if (spObjects == null) {
      log.exiting("SharePointConnector", "getModifiedDocIdsList");
      return;
    }
    for (Object choice : spObjects) {
      // Ignore view change detection.

      if (choice instanceof SPListItem) {
        getModifiedDocIdsListItem((SPListItem) choice, modifiedEntries);
      }
    }
    log.exiting("SharePointConnector", "getModifiedDocIdsList");
  }

  private void getModifiedDocIdsListItem(SPListItem changes, List<PushEntry> modifiedEntries) {
    log.entering(
        "SharePointConnector",
        "getModifiedDocIdsListItem",
        new Object[] {changes, modifiedEntries});
    if (isModified(changes.getChange())) {
      SPListItem.ListItem listItem = changes.getListItem();
      if (listItem == null) {
        log.exiting("SharePointConnector", "getModifiedDocIdsListItem");
        return;
      }
      Object oData = listItem.getAny();
      if (!(oData instanceof Element)) {
        log.log(Level.WARNING, "Unexpected object type for data: {0}",
            oData.getClass());
      } else {
        Element data = (Element) oData;
        String serverUrl = data.getAttribute(OWS_SERVERURL_ATTRIBUTE);
        if (serverUrl == null) {
          log.log(Level.WARNING, "Could not find server url attribute for "
              + "list item {0}", changes.getId());
        } else {
          String url = changes.getServerUrl() + serverUrl;
          modifiedEntries.add(new PushEntry().setId(url).setKind("modified"));
        }
      }
    }
    log.exiting("SharePointConnector", "getModifiedDocIdsListItem");
  }

  private boolean isModified(String change) {
    return !"Unchanged".equals(change) && !"Delete".equals(change);
  }

  private SiteConnector getSiteConnector(String site, String web) throws IOException {
    web = getCanonicalUrl(web);
    SiteConnector siteConnector = siteConnectors.get(web);
    if (siteConnector == null) {
      site = getCanonicalUrl(site);
      ntlmAuthenticator.addPermitForHost(new URL(web));
      String endpoint = spUrlToUri(web + "/_vti_bin/SiteData.asmx").toString();
      SiteDataSoap siteDataSoap = soapFactory.newSiteData(endpoint);

      String endpointUserGroup = spUrlToUri(site + "/_vti_bin/UserGroup.asmx")
          .toString();
      UserGroupSoap userGroupSoap = soapFactory.newUserGroup(endpointUserGroup);
      String endpointPeople = spUrlToUri(site + "/_vti_bin/People.asmx")
          .toString();
      PeopleSoap peopleSoap = soapFactory.newPeople(endpointPeople);

      addRequestHeaders((BindingProvider) siteDataSoap);
      addRequestHeaders((BindingProvider) userGroupSoap);
      addRequestHeaders((BindingProvider) peopleSoap);

      addSocketTimeoutConfiguration((BindingProvider) siteDataSoap);
      addSocketTimeoutConfiguration((BindingProvider) userGroupSoap);
      addSocketTimeoutConfiguration((BindingProvider) peopleSoap);

      siteConnector =
          new SiteConnector(
              site,
              web,
              siteDataSoap,
              userGroupSoap,
              peopleSoap,
              new MemberIdMappingCallable(site),
              new SiteUserIdMappingCallable(site));
      siteConnectors.putIfAbsent(web, siteConnector);
      siteConnector = siteConnectors.get(web);
    }
    return siteConnector;
  }

  private void addRequestHeaders(BindingProvider port) {
    Map<String, List<String>> headers = new HashMap<String, List<String>>();
    // Add forms authentication cookies or disable forms authentication
    if (authenticationHandler.getAuthenticationCookies().isEmpty()) {
      // To access a SharePoint site that uses multiple authentication
      // providers by using a set of Windows credentials, need to add
      // "X-FORMS_BASED_AUTH_ACCEPTED" request header to web service request
      // and set its value to "f"
      // http://msdn.microsoft.com/en-us/library/hh124553(v=office.14).aspx
      headers.put("X-FORMS_BASED_AUTH_ACCEPTED", Collections.singletonList("f"));
    } else {
      headers.put("Cookie", authenticationHandler.getAuthenticationCookies());
    }

    // Set User-Agent value
    if (!"".equals(sharepointUserAgent)) {
      headers.put("User-Agent", Collections.singletonList(sharepointUserAgent));
    }

    // Set request headers
    port.getRequestContext().put(MessageContext.HTTP_REQUEST_HEADERS, headers);
  }

  private void addSocketTimeoutConfiguration(BindingProvider port) {
    port.getRequestContext().put("com.sun.xml.internal.ws.connect.timeout",
        socketTimeoutMillis);
    port.getRequestContext().put("com.sun.xml.internal.ws.request.timeout",
        readTimeOutMillis);
    port.getRequestContext().put("com.sun.xml.ws.connect.timeout",
        socketTimeoutMillis);
    port.getRequestContext().put("com.sun.xml.ws.request.timeout",
        readTimeOutMillis);
  }

  static URI spUrlToUri(String url) throws IOException {
    // Because SP is silly, the path of the URI is unencoded, but the rest of
    // the URI is correct. Thus, we split up the path from the host, and then
    // turn them into URIs separately, and then turn everything into a
    // properly-escaped string.
    String[] parts = url.split("/", 4);
    if (parts.length < 3) {
      throw new IllegalArgumentException("Too few '/'s: " + url);
    }
    String host = parts[0] + "/" + parts[1] + "/" + parts[2];
    // Host must be properly-encoded already.
    URI hostUri = URI.create(host);
    if (parts.length == 3) {
      // There was no path.
      return hostUri;
    }
    URI pathUri;
    try {
      pathUri = new URI(null, null, "/" + parts[3], null);
    } catch (URISyntaxException ex) {
      throw new IOException(ex);
    }
    return hostUri.resolve(pathUri);
  }

  /**
   * Encode input url by
   *   1. Handle unicode in URL by converting to ASCII string.
   *   2. Perform browser leniency by either encoding or escaping unsupported
   *      characters in URL.
   */
  @VisibleForTesting
  static URL encodeSharePointUrl(String url, boolean performBrowserLeniency)
      throws IOException {
    if (!performBrowserLeniency) {
      // If no need to perform browser leniency, just return properly escaped
      // string using toASCIIString() to handle unicode.
      return new URL(spUrlToUri(url).toASCIIString());
    }
    String[] urlParts = url.split("\\?", 2);
    URI encodedUri = spUrlToUri(urlParts[0]);
    if (urlParts.length == 1) {
      return new URL(encodedUri.toASCIIString());
    }
    URI queryUri;
    try {
      // Special handling for path when path is empty. e.g. for URL
      // http://sharepoint.example.com?ID=1 generates 400 bad request
      // in Java code but in browser it works fine.
      // Following code block will generate URL as
      // http://sharepoint.example.com/?ID=1 which works fine in Java code.
      String path = "".equals(encodedUri.getPath()) ? "/" : encodedUri.getPath();
      // Create new URI with query parameters
      queryUri = new URI(encodedUri.getScheme(), encodedUri.getAuthority(),
          path, urlParts[1], encodedUri.getFragment());
      return new URL(queryUri.toASCIIString());
    } catch (URISyntaxException ex) {
      throw new IOException(ex);
    }
  }

  // Remove trailing slash from URLs as SharePoint doesn't like trailing slash
  // in SiteData.GetUrlSegments
  static String getCanonicalUrl(String url) {
    if (!url.endsWith("/")) {
      return url;
    }
    return url.substring(0, url.length() - 1);
  }

  /**
   * SharePoint encodes special characters as _x????_ where the ? are hex
   * digits. Each such encoding is a UTF-16 character. For example, _x0020_ is
   * space and _xFFE5_ is the fullwidth yen sign.
   */
  @VisibleForTesting
  static String decodeMetadataName(String name) {
    Matcher m = METADATA_ESCAPE_PATTERN.matcher(name);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      char c = (char) Integer.parseInt(m.group(1), 16);
      m.appendReplacement(sb, Matcher.quoteReplacement("" + c));
    }
    m.appendTail(sb);
    return sb.toString();
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    Application application = new Application.Builder(new SharePointConnector(), args).build();
    application.start();
  }

  private SiteConnector getConnectorForDocId(QueueEntry entry) throws IOException {
    if (VIRTUAL_SERVER_ID.equals(entry.getId())) {
      if (sharePointUrl.isSiteCollectionUrl()) {
        log.log(
            Level.FINE,
            "Returning null SiteConnector for root document "
                + " because connector is currently configured in site collection "
                + "mode for {0} only.",
            sharePointUrl.getSharePointUrl());
        return null;
      }
      return getSiteConnector(
          sharePointUrl.getVirtualServerUrl(), sharePointUrl.getVirtualServerUrl());
    }
    URI uri = spUrlToUri(entry.getId());
    if (!ntlmAuthenticator.isPermittedHost(uri.toURL())) {
      log.log(Level.WARNING, "URL {0} not white listed", uri);
      return null;
    }
    String rootUrl;
    try {
      rootUrl = getRootUrl(uri);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    SiteConnector rootConnector = getSiteConnector(rootUrl, rootUrl);
    SiteConnector connectorForUrl = rootConnector.getConnectorForUrl(entry.getId());
    if (connectorForUrl == null) {
      return null;
    }
    if (sharePointUrl.isSiteCollectionUrl()
        &&
        // Performing case sensitive comparison as mismatch in URL casing
        // between SharePoint Server and connector can result in broken ACL
        // inheritance chain on GSA.
        !sharePointUrl.getSharePointUrl().equals(connectorForUrl.siteUrl)) {
      log.log(
          Level.FINE,
          "Returning null SiteConnector for {0} because "
              + "connector is currently configured in site collection mode "
              + "for {1} only.",
          new Object[] {entry.getId(), sharePointUrl.getSharePointUrl()});
      return null;
    }
    return connectorForUrl;
  }

  private String getRootUrl(URI uri) throws URISyntaxException {
    return new URI(
        uri.getScheme(), uri.getAuthority(), null, null, null).toString();
  }

  /**
   * Convert from text/html to text/plain. Although we hope for good fidelity,
   * getting the conversion perfect is not necessary.
   */
  @VisibleForTesting
  static String stripHtml(String html) {
    html = HTML_TAG_PATTERN.matcher(html).replaceAll("");
    Matcher m = HTML_ENTITY_PATTERN.matcher(html);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      String entity = m.group(1);
      String decodedEntity;
      if (entity.startsWith("#")) {
        entity = entity.substring(1);
        try {
          // HTML entities are only in UCS-2 range, so no need to worry about
          // converting to surrogates.
          char c = (char) Integer.parseInt(entity);
          decodedEntity = Character.toString(c);
        } catch (NumberFormatException ex) {
          log.log(Level.FINE, "Could not decode entity", ex);
          decodedEntity = "";
        }
      } else {
        entity = entity.toLowerCase(Locale.ENGLISH);
        decodedEntity = HTML_ENTITIES.get(entity);
        if (decodedEntity == null) {
          decodedEntity = "";
        }
      }
      m.appendReplacement(sb, Matcher.quoteReplacement(decodedEntity));
    }
    m.appendTail(sb);
    return sb.toString();
  }

  @VisibleForTesting
  static String decodeClaim(String loginName, String name) {
    if (!loginName.startsWith(IDENTITY_CLAIMS_PREFIX)
        && !loginName.startsWith(OTHER_CLAIMS_PREFIX)) {
      return loginName;
    }
    // AD User
    if (loginName.startsWith("i:0#.w|")) {
      return loginName.substring(7);
      // AD Group
    } else if (loginName.startsWith("c:0+.w|")) {
      return name;
    } else if (loginName.equals("c:0(.s|true")) {
      return "Everyone";
    } else if (loginName.equals("c:0!.s|windows")) {
      return "NT AUTHORITY\\authenticated users";
      // Forms authentication role
    } else if (loginName.startsWith("c:0-.f|")) {
      return loginName.substring(7).replace("|", ":");
      // Forms authentication user
    } else if (loginName.startsWith("i:0#.f|")) {
      return loginName.substring(7).replace("|", ":");
      // Identity and role claims for trusted providers such as ADFS
    } else if (loginName.matches("^([i|c]\\:0.\\.t\\|).*$")) {
      String[] parts = loginName.split(Pattern.quote("|"), 3);
      if (parts.length == 3) {
        return parts[2];
      }
    }
    log.log(Level.WARNING, "Unsupported claims value {0}", loginName);
    return null;
  }

  // TODO(tvartak) : Move this to SDK once we add namespace support
  static String getPrincipalName(String name, String namespace) {
    if (Strings.isNullOrEmpty(namespace)) return name;
    return String.format("%s:%s", name, namespace);
  }

  static String getFragmentId(String id, String fragment) {
    return String.format("%s?%s", id, fragment);
  }

  /**
   * Method to get decoded login name for users and groups principals. For claims encoded domain
   * groups if performSidLookup is true, this method will use {@link ActiveDirectoryClient} to
   * convert sid to domain\groupname. If conversion fails method will return displayName as login
   * name. In other cases decodeClaims method will decode input loginName.
   */
  private String getLoginNameForPrincipal(
      String loginName, String displayName, boolean isDomainGroup) {
    if (isDomainGroup && performSidLookup && loginName.startsWith("c:0+.w|")) {
      try {
        String groupSid = loginName.substring(7);
        String principal = adClient.getUserAccountBySid(loginName.substring(7));
        if (principal == null) {
          log.log(Level.WARNING, "Could not resolve login name for SID {0}."
              + " Using display name {1} as fallback.",
              new Object[]{groupSid, displayName});
          return displayName;
        } else {
          return principal;
        }
      } catch (IOException ex) {
        log.log(Level.WARNING, String.format("Error performing SID lookup for "
            + "User %s. Returing display name %s as fallback.",
            loginName, displayName), ex);
        return displayName;
      }
    } else {
      return decodeClaim(loginName, displayName);
    }
  }

  @VisibleForTesting
  class SharePointUrl {
    private final String sharePointUrl;
    private final String virtualServerUrl;
    private final boolean siteCollectionOnly;

    public SharePointUrl(String sharePointUrl, String siteCollectionOnlyModeConfig)
        throws InvalidConfigurationException {
      if (sharePointUrl == null || siteCollectionOnlyModeConfig == null) {
        throw new NullPointerException();
      }
      sharePointUrl = sharePointUrl.trim();
      siteCollectionOnlyModeConfig = siteCollectionOnlyModeConfig.trim();

      this.sharePointUrl = getCanonicalUrl(sharePointUrl);

      if (!"".equals(siteCollectionOnlyModeConfig)) {
        this.siteCollectionOnly = Boolean.parseBoolean(
            siteCollectionOnlyModeConfig);
      } else {
        this.siteCollectionOnly =  this.sharePointUrl.split("/").length > 3;
      }

      try {
        this.virtualServerUrl =
            getRootUrl(encodeSharePointUrl(this.sharePointUrl, performBrowserLeniency).toURI());
      } catch (MalformedURLException exMalformed) {
        throw new InvalidConfigurationException(
            "Connector is configured with malformed SharePoint URL ["
                + sharePointUrl
                + "]. Please specify valid SharePoint URL.",
            exMalformed);
      } catch (URISyntaxException exSyntax) {
        throw new InvalidConfigurationException(
            "Connector is configured with invalid SharePoint URL ["
                + sharePointUrl
                + "]. Please specify valid SharePoint URL.",
            exSyntax);
      } catch (IOException ex) {
        throw new InvalidConfigurationException(
            "Connector is configured with invalid SharePoint URL ["
                + sharePointUrl
                + "]. Please specify valid SharePoint URL.",
            ex);
      } catch (IllegalArgumentException exIllegal) {
        throw new InvalidConfigurationException(
            "Connector is configured with invalid SharePoint URL ["
                + sharePointUrl
                + "]. Please specify valid SharePoint URL.",
            exIllegal);
      }
    }

    public boolean isSiteCollectionUrl() {
      return this.siteCollectionOnly;
    }

    public String getVirtualServerUrl() {
      return this.virtualServerUrl;
    }
    public String getSharePointUrl() {
      return this.sharePointUrl;
    }

    @Override
    public String toString() {
      return "SharePointUrl(sharePointUrl = " + sharePointUrl
          + ", virtualServerUrl = " + virtualServerUrl
          + ", siteCollectionOnly = " + siteCollectionOnly + ")";
    }
  }

  @VisibleForTesting
  class SiteConnector {
    private final SiteDataClient siteDataClient;
    private final UserGroupSoap userGroup;
    private final PeopleSoap people;
    private final String siteUrl;
    private final String webUrl;
    /**
     * Callable for accessing an up-to-date instance of {@link MemberIdMapping}.
     * Using a callable instead of accessing {@link #memberIdsCache} directly as
     * this allows mocking out the cache during testing.
     */
    private final Callable<MemberIdMapping> memberIdMappingCallable;
    private final Callable<MemberIdMapping> siteUserIdMappingCallable;

    /**
     * Lock for refreshing MemberIdMapping. We use a unique lock because it is
     * held while waiting on I/O.
     */
    private final Object refreshMemberIdMappingLock = new Object();

    /**
     * Lock for refreshing SiteUserMapping. We use a unique lock because it is
     * held while waiting on I/O.
     */
    private final Object refreshSiteUserMappingLock = new Object();

    public SiteConnector(String site, String web, SiteDataSoap siteDataSoap,
        UserGroupSoap userGroupSoap, PeopleSoap people,
        Callable<MemberIdMapping> memberIdMappingCallable,
        Callable<MemberIdMapping> siteUserIdMappingCallable) {
      log.entering("SiteConnector", "SiteConnector", new Object[] {site, web, siteDataSoap});
      if (site.endsWith("/")) {
        throw new AssertionError();
      }
      if (web.endsWith("/")) {
        throw new AssertionError();
      }
      if (memberIdMappingCallable == null) {
        throw new NullPointerException();
      }
      this.siteUrl = site;
      this.webUrl = web;
      this.userGroup = userGroupSoap;
      this.people = people;
      this.siteDataClient = new SiteDataClient(siteDataSoap, xmlValidation);
      this.memberIdMappingCallable = memberIdMappingCallable;
      this.siteUserIdMappingCallable = siteUserIdMappingCallable;
      log.exiting("SiteConnector", "SiteConnector");
    }

    private MemberIdMapping getMemberIdMapping() throws IOException {
      try {
        return memberIdMappingCallable.call();
      } catch (IOException ex) {
        throw ex;
      } catch (Exception ex) {
        throw new IOException(ex);
      }
    }

    /**
     * Provide a more recent MemberIdMapping than {@code mapping}, because the
     * mapping is known to be out-of-date.
     */
    private MemberIdMapping refreshMemberIdMapping(MemberIdMapping mapping)
        throws IOException {
      // Synchronize callers to prevent a rush of invalidations due to multiple
      // callers noticing that the map was out of date at the same time.
      synchronized (refreshMemberIdMappingLock) {
        // NOTE: This may block on I/O, so we must be wary of what locks are
        // held.
        MemberIdMapping maybeNewMapping = getMemberIdMapping();
        if (mapping != maybeNewMapping) {
          // The map has already been refreshed.
          return maybeNewMapping;
        }
        memberIdsCache.invalidate(siteUrl);
      }
      return getMemberIdMapping();
    }

    /**
     * Provide a more recent SiteUserMapping than {@code mapping}, because the
     * mapping is known to be out-of-date.
     */
    private MemberIdMapping refreshSiteUserMapping(MemberIdMapping mapping)
        throws IOException {
      // Synchronize callers to prevent a rush of invalidations due to multiple
      // callers noticing that the map was out of date at the same time.
      synchronized (refreshSiteUserMappingLock) {
        // NOTE: This may block on I/O, so we must be wary of what locks are
        // held.
        MemberIdMapping maybeNewMapping = getSiteUserMapping();
        if (mapping != maybeNewMapping) {
          // The map has already been refreshed.
          return maybeNewMapping;
        }
        siteUserCache.invalidate(siteUrl);
      }
      return getSiteUserMapping();
    }

    private MemberIdMapping getSiteUserMapping() throws IOException {
      try {
        return siteUserIdMappingCallable.call();
      } catch (IOException ex) {
        throw ex;
      } catch (Exception ex) {
        throw new IOException(ex);
      }
    }

    public void getDocContent(QueueEntry entry) throws IOException {
      log.entering("SiteConnector", "getDocContent", entry);
      String url = entry.getId();
      // SiteData.GetUrlSegment call fails for URLs with trailing slash.
      // Responding not found as DocId ends with trailing slash.
      if (url.endsWith("/")) {
        log.log(Level.WARNING,
            "Responding not found as DocId {0} ends with trailing slash", url);
        indexingService.deleteItem(entry.getId());
        log.exiting("SiteConnector", "getDocContent");
        return;
      }
      if (getAttachmentDocContent(entry)) {
        // Success, it was an attachment.
        log.exiting("SiteConnector", "getDocContent");
        return;
      }

      Holder<String> listId = new Holder<String>();
      Holder<String> itemId = new Holder<String>();
      // No need to retrieve webId, since it isn't populated when you contact a
      // web's SiteData.asmx page instead of its parent site's.
      boolean result = siteDataClient.getUrlSegments(entry.getId(), listId, itemId);
      if (!result) {
        // It may still be an aspx page.
        if (entry.getId().toLowerCase(Locale.ENGLISH).endsWith(".aspx")) {
          getAspxDocContent(entry);
        } else {
          log.log(Level.FINE, "responding not found");
          indexingService.deleteItem(entry.getId());
        }
        log.exiting("SiteConnector", "getDocContent");
        return;
      }
      if (itemId.value != null) {
        getListItemDocContent(entry, listId.value, itemId.value);
      } else if (listId.value != null) {
        getListDocContent(entry, listId.value);
      } else {
        // Assume it is a top-level site.
        getSiteDocContent(entry);
      }
      log.exiting("SiteConnector", "getDocContent");
    }

    private String encodeDocId(String url) {
      log.entering("SiteConnector", "encodeDocId", url);
      if (url.toLowerCase().startsWith("https://")
          || url.toLowerCase().startsWith("http://")) {
        // Leave as-is.
      } else if (!url.startsWith("/")) {
        url = webUrl + "/" + url;
      } else {
        // Rip off everthing after the third slash (including the slash).
        // Get http://example.com from http://example.com/some/folder.
        String[] parts = webUrl.split("/", 4);
        url = parts[0] + "//" + parts[2] + url;
      }
      log.exiting("SiteConnector", "encodeDocId", url);
      return url;
    }

    private URI docIdToUri(String url) throws IOException {
      return spUrlToUri(url);
    }

    /**
     * Handles converting from relative paths to fully qualified URIs and
     * dealing with SharePoint's lack of encoding paths (spaces in SP are kept
     * as spaces in URLs, instead of becoming %20).
     */
    private URI sharePointUrlToUri(String path) throws IOException {
      return docIdToUri(encodeDocId(path));
    }

    private void getVirtualServerDocContent() throws IOException {
      log.entering("SiteConnector", "getVirtualServerDocContent");
      VirtualServer vs = siteDataClient.getContentVirtualServer();

      final long necessaryPermissionMask = LIST_ITEM_MASK;
      List<Principal> permits = new ArrayList<Principal>();
      List<Principal> denies = new ArrayList<Principal>();

      // A PolicyUser is either a user or group, but we aren't provided with
      // which. We make a web service call to determine which. When using claims
      // is enabled, we actually do know the type, but we need additional
      // information to produce a clear ACL. As such, we blindly get more info
      // for all the PolicyUsers at once in a single batch.
      Map<String, PrincipalInfo> resolvedPolicyUsers;
      {
        List<String> policyUsers = new ArrayList<String>();
        for (PolicyUser policyUser : vs.getPolicies().getPolicyUser()) {
          policyUsers.add(policyUser.getLoginName());
        }
        resolvedPolicyUsers = resolvePrincipals(policyUsers);
      }

      for (PolicyUser policyUser : vs.getPolicies().getPolicyUser()) {
        String loginName = policyUser.getLoginName();
        PrincipalInfo p = resolvedPolicyUsers.get(loginName);
        if (p == null || !p.isIsResolved()) {
          log.log(Level.WARNING, "Unable to resolve Policy User = {0}", loginName);
          continue;
        }
        // TODO(ejona): special case NT AUTHORITY\LOCAL SERVICE.
        if (p.getPrincipalType() != SPPrincipalType.SECURITY_GROUP
            && p.getPrincipalType() != SPPrincipalType.USER) {
          log.log(Level.WARNING, "Principal {0} is an unexpected type: {1}",
              new Object[] {p.getAccountName(), p.getPrincipalType()});
          continue;
        }
        boolean isGroup = p.getPrincipalType() == SPPrincipalType.SECURITY_GROUP;
        String accountName =
            getLoginNameForPrincipal(p.getAccountName(), p.getDisplayName(), isGroup);
        if (accountName == null) {
          log.log(Level.WARNING, "Unable to decode claim. Skipping policy user {0}", loginName);
          continue;
        }
        log.log(Level.FINER, "Policy User accountName = {0}", accountName);
        Principal principal;
        if (isGroup) {
          principal =
              Acl.getExternalGroupPrincipal(getPrincipalName(accountName, defaultNamespace));
        } else {
          principal = Acl.getExternalUserPrincipal(getPrincipalName(accountName, defaultNamespace));
        }
        long grant = policyUser.getGrantMask().longValue();
        if ((necessaryPermissionMask & grant) == necessaryPermissionMask) {
          permits.add(principal);
        }
        long deny = policyUser.getDenyMask().longValue();
        // If at least one necessary bit is masked, then deny user.
        if ((necessaryPermissionMask & deny) != 0) {
          denies.add(principal);
        }
      }
      com.google.api.services.springboardindex.model.Item item =
          new com.google.api.services.springboardindex.model.Item();
      item.setId(VIRTUAL_SERVER_ID);
      item.setReaders(permits).setDeniedReaders(denies);
      indexingService.updateItem(item, true);
      List<PushEntry> sites = new ArrayList<>();
      for (ContentDatabases.ContentDatabase cdcd
          : vs.getContentDatabases().getContentDatabase()) {
        try {
          ContentDatabase cd = siteDataClient.getContentContentDatabase(cdcd.getID(), true);
          if (cd.getSites() != null) {
            for (Sites.Site site : cd.getSites().getSite()) {
              String siteUrl = site.getURL();
              siteUrl = getCanonicalUrl(siteUrl);
              sites.add(new PushEntry().setId(encodeDocId(siteUrl)));
            }
          }
        } catch (IOException ex) {
          log.log(Level.WARNING, "Error retriving sites from content database " + cdcd.getID(), ex);
        }
      }
      indexingService.push(sites);
      log.exiting("SiteConnector", "getVirtualServerDocContent");
    }


    /**
     * Returns the url of the parent of the web. The parent url is not the same
     * as the siteUrl, since there may be multiple levels of webs. It is an
     * error to call this method when there is no parent, which is the case iff
     * {@link #isWebSiteCollection} is {@code true}.
     */
    private String getWebParentUrl() {
      if (isWebSiteCollection()) {
        throw new IllegalStateException();
      }
      int slashIndex = webUrl.lastIndexOf("/");
      return webUrl.substring(0, slashIndex);
    }

    /** Returns true if webUrl is a site collection. */
    private boolean isWebSiteCollection() {
      return siteUrl.equals(webUrl);
    }

    /**
     * Returns {@code true} if the current web should not be indexed. This
     * method may issue a request for the web content for all parent webs, so it
     * is expensive, although it uses cached responses to reduce cost.
     */
    private boolean isWebNoIndex(CachedWeb w) throws IOException {
      if ("True".equals(w.noIndex)) {
        return true;
      }
      if (isWebSiteCollection()) {
        return false;
      }
      SiteConnector siteConnector = getSiteConnector(siteUrl, getWebParentUrl());
      return siteConnector.isWebNoIndex(rareModCache.getWeb(siteConnector.siteDataClient));
    }

    private void getSiteDocContent(QueueEntry entry) throws IOException {
      log.entering("SiteConnector", "getSiteDocContent", entry);
      Web w = siteDataClient.getContentWeb();

      if (isWebNoIndex(new CachedWeb(w))) {
        log.fine("Document marked for NoIndex");
        indexingService.deleteItem(entry.getId());
        log.exiting("SiteConnector", "getSiteDocContent");
        return;
      }

      if (webUrl.endsWith("/")) {
        throw new AssertionError();
      }

      if (isWebSiteCollection()) {
        List<Principal> admins = new ArrayList<>();
        for (UserDescription user : w.getUsers().getUser()) {
          if (user.getIsSiteAdmin() != TrueFalseType.TRUE) {
            continue;
          }
          Principal principal = userDescriptionToPrincipal(user);
          if (principal == null) {
            log.log(
                Level.WARNING,
                "Unable to determine login name. Skipping admin user with ID " + "{0}",
                user.getID());
            continue;
          }
          admins.add(principal);
        }
        com.google.api.services.springboardindex.model.Item adminFragment =
            new com.google.api.services.springboardindex.model.Item();
        adminFragment.setId(getFragmentId(siteUrl, SITE_COLLECTION_ADMIN_FRAGMENT));
        adminFragment.setReaders(admins);
        if (!sharePointUrl.isSiteCollectionUrl()) {
          adminFragment
              .setInheritanceType(Acl.InheritanceType.PARENT_OVERRIDE.toString())
              .setInheritFrom(VIRTUAL_SERVER_ID);
        } else {
          log.log(
              Level.INFO,
              "Not inheriting from Web application policy "
                  + "since connector is configured for site collection only mode.");
        }
        indexingService.updateItem(adminFragment, true);
        GroupMembership groups = siteDataClient.getContentSite().getGroups();
        Map<ExternalGroup, Collection<Principal>> groupDefs =
            new HashMap<ExternalGroup, Collection<Principal>>();
        groupDefs.putAll(computeMembersForGroups(groups));
        for (ExternalGroup externalGroup : groupDefs.keySet()) {
          indexingService.updateExternalGroup(externalGroup);
        }
      }

      boolean allowAnonymousAccess =
          isAllowAnonymousReadForWeb(new CachedWeb(w))
              // Check if anonymous access is denied by web application policy
              && (!isDenyAnonymousAccessOnVirtualServer());

      com.google.api.services.springboardindex.model.Item item =
          new com.google.api.services.springboardindex.model.Item();
      item.setId(entry.getId());

      if (!allowAnonymousAccess) {
        item.setInheritanceType(Acl.InheritanceType.PARENT_OVERRIDE.toString());
        final boolean includePermissions;
        if (isWebSiteCollection()) {
          includePermissions = true;
        } else {
          SiteConnector parentSiteConnector = getSiteConnector(siteUrl, getWebParentUrl());
          Web parentW = parentSiteConnector.siteDataClient.getContentWeb();
          String parentScopeId = parentW.getMetadata().getScopeID().toLowerCase(Locale.ENGLISH);
          String scopeId = w.getMetadata().getScopeID().toLowerCase(Locale.ENGLISH);
          includePermissions = !scopeId.equals(parentScopeId);
        }
        if (includePermissions) {
          List<Permission> permissions = w.getACL().getPermissions().getPermission();
          item.setReaders(generateAcl(permissions, LIST_ITEM_MASK))
              .setInheritFrom(getFragmentId(siteUrl, SITE_COLLECTION_ADMIN_FRAGMENT));
        } else {
          item.setInheritFrom(getWebParentUrl());
        }
      }

      item.setViewUrl(spUrlToUri(w.getMetadata().getURL()).toString());
      List<PushEntry> entries = new ArrayList<>();
      if (w.getWebs() != null) {
        for (Webs.Web web : w.getWebs().getWeb()) {
          String childWebUrl = getCanonicalUrl(web.getURL());
          entries.add(new PushEntry().setId(encodeDocId(childWebUrl)));
        }
      }
      if (w.getLists() != null) {
        for (Lists.List list : w.getLists().getList()) {
          if ("".equals(list.getDefaultViewUrl())) {
            // Do some I/O to give a good informational message. This is
            // expected to be a very rare case.
            com.microsoft.schemas.sharepoint.soap.List l =
                siteDataClient.getContentList(list.getID());
            log.log(Level.INFO,
                "Ignoring List {0} in {1}, since it has no default view URL",
                new Object[] {l.getMetadata().getTitle(), webUrl});
            continue;
          }
          entries.add(new PushEntry().setId(encodeDocId(list.getDefaultViewUrl())));
        }
      }
      if (w.getFPFolder() != null) {
        FolderData f = w.getFPFolder();
        if (!f.getFolders().isEmpty()) {
          for (Folders folders : f.getFolders()) {
            if (folders.getFolder() != null) {
              for (Folders.Folder folder : folders.getFolder()) {
                // Lists is always present in the listing but never exists.
                if ("Lists".equals(folder.getURL())) {
                  continue;
                }
                entries.add(new PushEntry().setId(encodeDocId(folder.getURL())));
              }
            }
          }
        }
        if (!f.getFiles().isEmpty()) {
          for (Files files : f.getFiles()) {
            if (files.getFile() != null) {
              for (Files.File file : files.getFile()) {
                entries.add(new PushEntry().setId(encodeDocId(file.getURL())));
              }
            }
          }
        }
      }
      indexingService.push(entries);
      log.log(Level.INFO, "Pushing Item {0}", item);
      indexingService.updateItem(item, true);
      log.exiting("SiteConnector", "getSiteDocContent");
    }

    private void getListDocContent(QueueEntry entry, String id) throws IOException {
      log.entering("SiteConnector", "getListDocContent", new Object[] {entry, id});
      com.microsoft.schemas.sharepoint.soap.List l = siteDataClient.getContentList(id);
      Web w = siteDataClient.getContentWeb();

      if (TrueFalseType.TRUE.equals(l.getMetadata().getNoIndex())
          || isWebNoIndex(new CachedWeb(w))) {
        log.fine("Document marked for NoIndex");
        indexingService.deleteItem(entry.getId());
        log.exiting("SiteConnector", "getListDocContent");
        return;
      }

      boolean allowAnonymousAccess =
          isAllowAnonymousReadForList(new CachedList(l))
              && isAllowAnonymousPeekForWeb(new CachedWeb(w))
              && (!isDenyAnonymousAccessOnVirtualServer());
      com.google.api.services.springboardindex.model.Item item =
          new com.google.api.services.springboardindex.model.Item();
      item.setId(entry.getId());
      if (!allowAnonymousAccess) {
        String scopeId =
            l.getMetadata().getScopeID().toLowerCase(Locale.ENGLISH);
        String webScopeId =
            w.getMetadata().getScopeID().toLowerCase(Locale.ENGLISH);

        String rootFolderDocId = encodeDocId(l.getMetadata().getRootFolder());
        com.google.api.services.springboardindex.model.Item rootFolder =
            new com.google.api.services.springboardindex.model.Item();
        rootFolder.setId(rootFolderDocId);

        if (scopeId.equals(webScopeId)) {
          rootFolder.setInheritFrom(webUrl);
          rootFolder.setInheritanceType(Acl.InheritanceType.PARENT_OVERRIDE.toString());
        } else {
          List<Permission> permissions =
              l.getACL().getPermissions().getPermission();
          rootFolder
              .setReaders(generateAcl(permissions, LIST_ITEM_MASK))
              .setInheritFrom(getFragmentId(siteUrl, SITE_COLLECTION_ADMIN_FRAGMENT))
              .setInheritanceType(Acl.InheritanceType.PARENT_OVERRIDE.toString());

        }
        indexingService.updateItem(rootFolder, true);
        item.setInheritFrom(rootFolderDocId)
            .setInheritanceType(Acl.InheritanceType.PARENT_OVERRIDE.toString());
      }

      URI displayUri = sharePointUrlToUri(
          "/".equals(l.getMetadata().getDefaultViewUrl())
          ? l.getMetadata().getRootFolder()
          : l.getMetadata().getDefaultViewUrl());

      item.setViewUrl(displayUri.toString());

      String lastModified = l.getMetadata().getLastModified();
      try {
        item.setContentModifiedTime(
            new DateTime(listLastModifiedDateFormat.get().parse(lastModified)));
      } catch (ParseException ex) {
        log.log(Level.INFO, "Could not parse LastModified: {0}", lastModified);
      }
      // TODO(tvartak) Add Html for List doc
      processFolder(id, "");
      indexingService.updateItem(item, true);
      log.exiting("SiteConnector", "getListDocContent");
    }

    /** {@code writer} should already have had {@link HtmlResponseWriter#start} called. */
    private void processFolder(String listGuid, String folderPath) throws IOException {
      log.entering("SiteConnector", "processFolder", new Object[] {listGuid, folderPath});
      Paginator<ItemData> folderPaginator =
          siteDataClient.getContentFolderChildren(listGuid, folderPath);
      ItemData folder;
      List<PushEntry> entries = new ArrayList<>();
      while ((folder = folderPaginator.next()) != null) {
        Xml xml = folder.getXml();

        Element data = getFirstChildWithName(xml, DATA_ELEMENT);
        for (Element row : getChildrenWithName(data, ROW_ELEMENT)) {
          String rowUrl = row.getAttribute(OWS_SERVERURL_ATTRIBUTE);
          entries.add(new PushEntry().setId(encodeDocId(getCanonicalUrl(rowUrl))));
        }
      }
      if (!entries.isEmpty()) {
        indexingService.push(entries);
      }
      log.exiting("SiteConnector", "processFolder");
    }

    private boolean elementHasName(Element ele, QName name) {
      return name.getLocalPart().equals(ele.getLocalName())
          && name.getNamespaceURI().equals(ele.getNamespaceURI());
    }

    private Element getFirstChildWithName(Xml xml, QName name) {
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

    private <T> T getFirstChildOfType(Xml xml, Class<T> type) {
      for (Object oChild : xml.getAny()) {
        if (!type.isInstance(oChild)) {
          continue;
        }
        return type.cast(oChild);
      }
      return null;
    }

    private List<Element> getChildrenWithName(Element ele, QName name) {
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

    private List<Attr> getAllAttributes(Element ele) {
      NamedNodeMap map = ele.getAttributes();
      List<Attr> attrs = new ArrayList<Attr>(map.getLength());
      for (int i = 0; i < map.getLength(); i++) {
        attrs.add((Attr) map.item(i));
      }
      return attrs;
    }

    private void addMetadata(String name, String value, Multimap<String, String> metadata) {
      if ("ows_MetaInfo".equals(name)) {
        // ows_MetaInfo is parsed out into other fields for us by SharePoint.
        // We filter it since it only duplicates those other fields.
        return;
      }
      if (name.startsWith("ows_")) {
        name = name.substring("ows_".length());
      }
      name = decodeMetadataName(name);
      if (ALTERNATIVE_VALUE_PATTERN.matcher(value).find()) {
        // This is a lookup field. We need to take alternative values only.
        // Ignore the integer part. 314;#pi;#42;#the answer
        String[] parts = value.split(";#", 0);
        for (int i = 1; i < parts.length; i += 2) {
          if (parts[i].isEmpty()) {
            continue;
          }
          metadata.put(name, parts[i]);
        }
      } else if (value.startsWith(";#") && value.endsWith(";#")) {
        // This is a multi-choice field. Values will be in the form:
        // ;#value1;#value2;#
        for (String part : value.split(";#", 0)) {
          if (part.isEmpty()) {
            continue;
          }
          metadata.put(name, part);
        }
      } else {
        metadata.put(name, value);
      }
    }

    private List<Principal> generateAcl(
        List<Permission> permissions, final long necessaryPermissionMask) throws IOException {
      List<Principal> permits = new LinkedList<Principal>();
      MemberIdMapping mapping = getMemberIdMapping();
      boolean memberIdMappingRefreshed = false;
      MemberIdMapping siteUserMapping = null;
      boolean siteUserMappingRefreshed = false;
      for (Permission permission : permissions) {
        // Although it is named "mask", this is really a bit-field of
        // permissions.
        long mask = permission.getMask().longValue();
        if ((necessaryPermissionMask & mask) != necessaryPermissionMask) {
          continue;
        }
        Integer id = permission.getMemberid();
        Principal principal = mapping.getPrincipal(id);
        if (principal == null) {
          log.log(Level.FINE, "Member id {0} is not available in memberid"
              + " mapping for Web [{1}] under Site Collection [{2}].",
              new Object[] {id, webUrl, siteUrl});
          if (siteUserMapping == null) {
            siteUserMapping = getSiteUserMapping();
          }
          principal = siteUserMapping.getPrincipal(id);
        }
        if (principal == null && !memberIdMappingRefreshed) {
          // Try to refresh member id mapping and check again.
          mapping = refreshMemberIdMapping(mapping);
          memberIdMappingRefreshed = true;
          principal = mapping.getPrincipal(id);
        }
        if (principal == null && !siteUserMappingRefreshed) {
          // Try to refresh site user mapping and check again.
          siteUserMapping = refreshSiteUserMapping(siteUserMapping);
          siteUserMappingRefreshed = true;
          principal = siteUserMapping.getPrincipal(id);
        }

        if (principal == null) {
          log.log(
              Level.WARNING,
              "Could not resolve member id {0} for Web " + "[{1}] under Site Collection [{2}].",
              new Object[] {id, webUrl, siteUrl});
          continue;
        }
        permits.add(principal);
      }
      return permits;
    }

    private Principal getUserPrincipal(int userId) throws IOException {
      if (userId == -1) {
        return null;
      }
      Principal principal = getMemberIdMapping().getPrincipal(userId);
      // MemberIdMapping will have information about users with explicit
      // permissions on SharePoint or users which are direct members of
      // SharePoint groups. MemberIdMapping might not have information
      // about all valid SharePoint Users. To get all valid SharePoint users
      // under SiteCollection, use SiteUserMapping.
      if (principal == null) {
        principal = getSiteUserMapping().getPrincipal(userId);
      }
      if (principal == null) {
        log.log(Level.WARNING, "Could not resolve user id {0}", userId);

      }
      return principal;
    }

    private boolean isPermitted(long permission,
        long necessaryPermission) {
      return (necessaryPermission & permission) == necessaryPermission;
    }

    private boolean isAllowAnonymousPeekForWeb(CachedWeb w) {
      return isPermitted(w.anonymousPermMask, SPBasePermissions.OPEN);
    }

    private boolean isAllowAnonymousReadForWeb(CachedWeb w) {
      boolean allowAnonymousRead =
          (w.allowAnonymousAccess == TrueFalseType.TRUE)
              && (w.anonymousViewListItems == TrueFalseType.TRUE)
              && isPermitted(w.anonymousPermMask, LIST_ITEM_MASK);
      return allowAnonymousRead;
    }

    private boolean isAllowAnonymousReadForList(CachedList l) {
      boolean allowAnonymousRead =
          (l.readSecurity != LIST_READ_SECURITY_ENABLED)
              && (l.allowAnonymousAccess == TrueFalseType.TRUE)
              && (l.anonymousViewListItems == TrueFalseType.TRUE)
              && isPermitted(l.anonymousPermMask, SPBasePermissions.VIEWLISTITEMS);
      return allowAnonymousRead;
    }

    private boolean isDenyAnonymousAccessOnVirtualServer() throws IOException {
      // Since Connector is configured to use Site Collection Only mode we are
      // ignoring web application policy.
      if (sharePointUrl.isSiteCollectionUrl()) {
        log.fine(
            "Ignoring web application policy acls since connector is "
                + "configured for site collection only mode.");
        return false;
      }
      CachedVirtualServer vs = rareModCache.getVirtualServer();
      if ((LIST_ITEM_MASK & vs.anonymousDenyMask) != 0) {
        return true;
      }
      // Anonymous access is denied if deny read policy is specified for any
      // user or group.
      return vs.policyContainsDeny;
    }

    private void getAspxDocContent(QueueEntry entry) throws IOException {
      log.entering("SiteConnector", "getAspxDocContent", entry);

      CachedWeb w = rareModCache.getWeb(siteDataClient);
      if (isWebNoIndex(w)) {
        log.fine("Document marked for NoIndex");
        indexingService.deleteItem(entry.getId());
        log.exiting("SiteConnector", "getAspxDocContent");
        return;
      }

      String aspxId = entry.getId();
      String parentId = aspxId.substring(0, aspxId.lastIndexOf('/'));
      boolean isDirectChild = webUrl.equalsIgnoreCase(parentId);
      // Check for valid ASPX pages
      // Process only direct child for current web
      if (!isDirectChild) {
        // Alternative approach to this string comparison is to make a
        // additional web service call for SiteData.GetContentWeb and
        // check if ASPX page is available under Web.getFPFolder().getFiles()
        log.log(Level.FINE, "Document [{0}] is not a direct child of Web [{1}]",
            new Object[] {aspxId, webUrl});
        indexingService.deleteItem(entry.getId());
        log.exiting("SiteConnector", "getAspxDocContent");
        return;
      }

      boolean allowAnonymousAccess =
          isAllowAnonymousReadForWeb(w)
              // Check if anonymous access is denied by web application policy
              && (!isDenyAnonymousAccessOnVirtualServer());
      com.google.api.services.springboardindex.model.Item item =
          new com.google.api.services.springboardindex.model.Item();
      item.setId(entry.getId());
      if (!allowAnonymousAccess) {
        item.setInheritanceType(Acl.InheritanceType.PARENT_OVERRIDE.toString())
            .setInheritFrom(parentId);
      }
      getFileDocContent(entry, item, true);
      log.exiting("SiteConnector", "getAspxDocContent");
    }

    /**
     * Blindly retrieve contents of DocId as if it were a file's URL. To prevent security issues,
     * this should only be used after the DocId has been verified to be a valid document on the
     * SharePoint instance. In addition, ACLs and other metadata and security measures should be set
     * before making this call.
     */
    private void getFileDocContent(
        QueueEntry entry,
        com.google.api.services.springboardindex.model.Item item,
        boolean setLastModified)
        throws IOException {
      log.entering("SiteConnector", "getFileDocContent", entry);
      String contentUrl = entry.getId();
      URI displayUrl = spUrlToUri(entry.getId());
      long startMillis = System.currentTimeMillis();
      FileInfo fi =
          httpClient.issueGetRequest(
              encodeSharePointUrl(entry.getId(), performBrowserLeniency),
              authenticationHandler.getAuthenticationCookies(),
              sharepointUserAgent,
              maxRedirectsToFollow,
              performBrowserLeniency);
      if (fi == null) {
        indexingService.deleteItem(entry.getId());
        return;
      }
      log.log(
          Level.FINE,
          "Duration: fetch headers {0} : {1,number,#} ms",
          new Object[] {contentUrl, System.currentTimeMillis() - startMillis});
      try {
        item.setViewUrl(contentUrl);
        String filePath = displayUrl.getPath();
        String fileExtension = "";
        if (filePath.lastIndexOf('.') > 0) {
          fileExtension = filePath.substring(
              filePath.lastIndexOf('.')).toLowerCase(Locale.ENGLISH);
        }
        if (FILE_EXTENSION_TO_MIME_TYPE_MAPPING.containsKey(fileExtension)) {
          String contentType =
              FILE_EXTENSION_TO_MIME_TYPE_MAPPING.get(fileExtension);
          log.log(Level.FINER,
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
            log.log(Level.INFO, "Could not parse Last-Modified: {0}",
                lastModifiedString);
          }
        }
        long contentDownloadStart = System.currentTimeMillis();
        indexingService.updateItemAndContent(
            item, new InputStreamContent(item.getMimeType(), fi.getContents()), true);
        log.log(
            Level.FINE,
            "Duration: downlaod content {0} : {1,number,#} ms",
            new Object[] {contentUrl, System.currentTimeMillis() - contentDownloadStart});
      } finally {
        fi.getContents().close();
      }
      log.log(
          Level.FINE,
          "Duration: getFileDocContent {0} : {1,number,#} ms",
          new Object[] {contentUrl, System.currentTimeMillis() - startMillis});
      log.exiting("SiteConnector", "getFileDocContent");
    }

    private void getListItemDocContent(QueueEntry entry, String listId, String itemId)
        throws IOException {
      log.entering("SiteConnector", "getListItemDocContent", new Object[] {entry, listId, itemId});
      CachedList l = rareModCache.getList(siteDataClient, listId);

      CachedWeb w = rareModCache.getWeb(siteDataClient);
      if (TrueFalseType.TRUE.equals(l.noIndex) || isWebNoIndex(w)) {
        log.fine("Document marked for NoIndex");
        indexingService.deleteItem(entry.getId());
        log.exiting("SiteConnector", "getListItemDocContent");
        return;
      }

      com.google.api.services.springboardindex.model.Item item =
          new com.google.api.services.springboardindex.model.Item();
      item.setId(entry.getId());

      boolean applyReadSecurity =
          (l.readSecurity == LIST_READ_SECURITY_ENABLED);
      ItemData i = siteDataClient.getContentItem(listId, itemId);

      Xml xml = i.getXml();
      Element data = getFirstChildWithName(xml, DATA_ELEMENT);
      Element row = getChildrenWithName(data, ROW_ELEMENT).get(0);

      String modifiedString = row.getAttribute(OWS_MODIFIED_ATTRIBUTE);
      Date lastModified = null;
      if (modifiedString == null) {
        log.log(Level.FINE, "No last modified information for list item");
      } else {
        try {
          item.setContentModifiedTime(new DateTime(modifiedDateFormat.get().parse(modifiedString)));
        } catch (ParseException ex) {
          log.log(Level.INFO, "Could not parse ows_Modified: {0}",
              modifiedString);
        }
      }

      // This should be in the form of "1234;#{GUID}". We want to extract the
      // {GUID}.
      String scopeId = row.getAttribute(OWS_SCOPEID_ATTRIBUTE).split(";#", 2)[1];
      scopeId = scopeId.toLowerCase(Locale.ENGLISH);

      // Anonymous access is disabled if read security is applicable for list.
      // Anonymous access for list items is disabled if it does not inherit
      // its effective permissions from list.

      // Even if anonymous access is enabled on list, it can be turned off
      // on Web level by setting Anonymous access to "Nothing" on Web.
      // Anonymous User must have minimum "Open" permission on Web
      // for anonymous access to work on List and List Items.
      boolean allowAnonymousAccess = isAllowAnonymousReadForList(l)
          && scopeId.equals(l.scopeId.toLowerCase(Locale.ENGLISH))
          && isAllowAnonymousPeekForWeb(w)
          && (!isDenyAnonymousAccessOnVirtualServer());

      if (!allowAnonymousAccess) {
        if (!applyReadSecurity) {
          String rawFileDirRef = row.getAttribute(OWS_FILEDIRREF_ATTRIBUTE);
          // This should be in the form of "1234;#site/list/path". We want to
          // extract the site/list/path. Path relative to host, even though it
          // doesn't have a leading '/'.
          String folderDocId = encodeDocId("/" + rawFileDirRef.split(";#", 2)[1]);
          String rootFolderDocId = encodeDocId(l.rootFolder);
          // If the parent is a list, folderDocId will be same as
          // rootFolderDocId. If inheritance chain is not
          // broken, item will inherit its permission from list root folder.
          // If parent is a folder, item will inherit its permissions from parent
          // folder.
          boolean parentIsList = folderDocId.equals(rootFolderDocId);
          String parentScopeId;
          // If current item has same scope id as list then inheritance is not
          // broken irrespective of current item is inside folder or not.
          if (parentIsList || scopeId.equals(l.scopeId.toLowerCase(Locale.ENGLISH))) {
            parentScopeId = l.scopeId.toLowerCase(Locale.ENGLISH);
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
            boolean result = siteDataClient.getUrlSegments(folderDocId, folderListId, folderItemId);
            if (!result) {
              throw new IOException("Could not find parent folder's itemId");
            }
            if (!listId.equals(folderListId.value)) {
              throw new AssertionError("Unexpected listId value");
            }
            ItemData folderItem = siteDataClient.getContentItem(listId, folderItemId.value);
            Element folderData = getFirstChildWithName(folderItem.getXml(), DATA_ELEMENT);
            Element folderRow = getChildrenWithName(folderData, ROW_ELEMENT).get(0);
            parentScopeId =
                folderRow
                    .getAttribute(OWS_SCOPEID_ATTRIBUTE)
                    .split(";#", 2)[1]
                    .toLowerCase(Locale.ENGLISH);
          }
          if (scopeId.equals(parentScopeId)) {
            item.setInheritanceType(Acl.InheritanceType.PARENT_OVERRIDE.toString())
                .setInheritFrom(folderDocId);
          } else {
            // We have to search for the correct scope within the scopes element.
            // The scope provided in the metadata is for the parent list, not for
            // the item
            Scopes scopes = getFirstChildOfType(xml, Scopes.class);
            boolean hasAcl = false;
            for (Scopes.Scope scope : scopes.getScope()) {
              if (scope.getId().toLowerCase(Locale.ENGLISH).equals(scopeId)) {
                item.setReaders(generateAcl(scope.getPermission(), LIST_ITEM_MASK))
                    .setInheritFrom(getFragmentId(siteUrl, SITE_COLLECTION_ADMIN_FRAGMENT))
                    .setInheritanceType(Acl.InheritanceType.PARENT_OVERRIDE.toString());
                hasAcl = true;
                break;
              }
            }
            if (!hasAcl) {
              throw new IOException("Unable to find permission scope for item: " + entry.getId());
            }
          }
        } else {
          final String fragmentName = "readSecurity";
          List<Permission> permission = null;
          Scopes scopes = getFirstChildOfType(xml, Scopes.class);
          for (Scopes.Scope scope : scopes.getScope()) {
            if (scope.getId().toLowerCase(Locale.ENGLISH).equals(scopeId)) {
              permission = scope.getPermission();
              break;
            }
          }
          if (permission == null) {
            permission = i.getMetadata().getScope().getPermissions().getPermission();
          }
          item.setReaders(generateAcl(permission, LIST_ITEM_MASK))
              .setInheritFrom(getFragmentId(entry.getId(), fragmentName))
              .setInheritanceType(Acl.InheritanceType.AND_BOTH_PERMIT.toString());
          int authorId = -1;
          String authorValue = row.getAttribute(OWS_AUTHOR_ATTRIBUTE);
          if (authorValue != null) {
            String[] authorInfo = authorValue.split(";#", 2);
            if (authorInfo.length == 2) {
              authorId = Integer.parseInt(authorInfo[0]);
            }
          }
          com.google.api.services.springboardindex.model.Item namedResource =
              new com.google.api.services.springboardindex.model.Item();
          namedResource.setId(getFragmentId(entry.getId(), fragmentName));
          List<Principal> readers = generateAcl(permission, READ_SECURITY_LIST_ITEM_MASK);
          Principal author = getUserPrincipal(authorId);
          if (author != null) {
            readers.add(author);
          }
          namedResource
              .setReaders(readers)
              .setInheritFrom(getFragmentId(siteUrl, SITE_COLLECTION_ADMIN_FRAGMENT))
              .setInheritanceType(Acl.InheritanceType.PARENT_OVERRIDE.toString());
          log.log(Level.INFO, "Updating item {0}", namedResource);
          indexingService.updateItem(namedResource, true);
        }
      }

      // This should be in the form of "1234;#0". We want to extract the 0.
      String type = row.getAttribute(OWS_FSOBJTYPE_ATTRIBUTE).split(";#", 2)[1];
      boolean isFolder = "1".equals(type);
      String serverUrl = row.getAttribute(OWS_SERVERURL_ATTRIBUTE);
      Multimap<String, String> metadata = TreeMultimap.create();
      for (Attr attribute : getAllAttributes(row)) {
        addMetadata(attribute.getName(), attribute.getValue(), metadata);
      }
      addMetadata(METADATA_PARENT_WEB_TITLE, w.webTitle, metadata);
      addMetadata(METADATA_LIST_GUID, listId, metadata);
      // TODO(tvartak) : Implement 204 handling
      boolean canRespondWithNoContent = false;

      if (isFolder) {
        String root = encodeDocId(l.rootFolder);
        root += "/";
        String folder = encodeDocId(serverUrl);
        if (!folder.startsWith(root)) {
          throw new AssertionError();
        }
        URI displayPage = sharePointUrlToUri(l.defaultViewUrl);
        if (serverUrl.contains("&") || serverUrl.contains("=")
            || serverUrl.contains("%")) {
          throw new AssertionError();
        }
        try {
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
        addMetadata(METADATA_OBJECT_TYPE, ObjectType.FOLDER.value(), metadata);
        if (canRespondWithNoContent) {
          log.log(
              Level.FINER,
              "Folder: Responding with 204 as Last-Modified "
                  + "is {0} and last access time is {1}",
              new Object[] {lastModified, entry.getItemUpdatedTime()});
          indexingService.updateItem(item, true);
          log.exiting("SiteConnector", "getListItemDocContent");
          return;
        }
        processAttachments(listId, itemId, row);
        processFolder(listId, folder.substring(root.length()));
        writeMetadataAsContent(item, metadata);
        log.exiting("SiteConnector", "getListItemDocContent");
        return;
      }
      String contentTypeId = row.getAttribute(OWS_CONTENTTYPEID_ATTRIBUTE);
      if (contentTypeId != null
          && contentTypeId.startsWith(CONTENTTYPEID_DOCUMENT_PREFIX)) {
        // This is a file (or "Document" in SharePoint-speak), so display its
        // contents.
        addMetadata(METADATA_OBJECT_TYPE, "Document", metadata);
        if (canRespondWithNoContent) {
          log.log(
              Level.FINER,
              "Document: Responding with 204 as Last-Modified "
                  + "is {0} and last access time is {1}",
              new Object[] {lastModified, entry.getItemUpdatedTime()});
          indexingService.updateItem(item, true);
          log.exiting("SiteConnector", "getListItemDocContent");
          return;
        }
        getFileDocContent(entry, item, false);
      } else {
        // Some list item.
        URI displayPage = sharePointUrlToUri(l.defaultViewItemUrl);
        try {
          URI viewItemUri =
              new URI(
                  displayPage.getScheme(),
                  displayPage.getAuthority(),
                  displayPage.getPath(),
                  "ID=" + itemId,
                  null);
          item.setViewUrl(viewItemUri.toString());
        } catch (URISyntaxException ex) {
          throw new IOException(ex);
        }
        addMetadata(METADATA_OBJECT_TYPE, ObjectType.LIST_ITEM.value(), metadata);
        if (canRespondWithNoContent) {
          log.log(
              Level.FINER,
              "Folder: Responding with 204 as Last-Modified "
                  + "is {0} and last access time is {1}",
              new Object[] {lastModified, entry.getItemUpdatedTime()});
          indexingService.updateItem(item, true);
          log.exiting("SiteConnector", "getListItemDocContent");
          return;
        }
        processAttachments(listId, itemId, row);
        writeMetadataAsContent(item, metadata);
      }
      log.exiting("SiteConnector", "getListItemDocContent");
    }

    private void processAttachments(String listId, String itemId, Element row) throws IOException {
      String strAttachments = row.getAttribute(OWS_ATTACHMENTS_ATTRIBUTE);
      int attachments = (strAttachments == null || "".equals(strAttachments))
          ? 0 : Integer.parseInt(strAttachments);
      if (attachments > 0) {
        Item item = siteDataClient.getContentListItemAttachments(listId, itemId);
        List<PushEntry> entries = new ArrayList<>();
        for (Item.Attachment attachment : item.getAttachment()) {
          entries.add(new PushEntry().setId(encodeDocId(attachment.getURL())));
        }
        indexingService.push(entries);
      }
    }

    /** Write out metadata as content so that snippets can be more helpful. */
    private void writeMetadataAsContent(
        com.google.api.services.springboardindex.model.Item item, Multimap<String, String> metadata)
        throws IOException {
      // TODO(tvartak) : Use appropriate column for Title
      List<String> fields = new ArrayList<>(metadata.keySet());
      ContentTemplate template =
          new ContentTemplate.ContentTemplateBuilder()
              .setTitle("Title")
              .setLowContent(fields)
              .build();
      String generated = template.apply(metadata);
      log.log(Level.INFO, "Updating item {0}", item);
      indexingService.updateItemAndContent(
          item, ByteArrayContent.fromString("text/html", generated), true);
    }

    private boolean getAttachmentDocContent(QueueEntry entry) throws IOException {
      log.entering("SiteConnector", "getAttachmentDocContent", entry);
      String url = entry.getId();
      if (!url.contains("/Attachments/")) {
        log.fine("Not an attachment: does not contain /Attachments/");
        log.exiting("SiteConnector", "getAttachmentDocContent", false);
        return false;
      }
      String[] parts = url.split("/Attachments/", 2);
      String listBase = parts[0];
      parts = parts[1].split("/", 2);
      if (parts.length != 2) {
        log.fine("Could not separate attachment file name and list item id");
        log.exiting("SiteConnector", "getAttachmentDocContent", false);
        return false;
      }
      String itemId = parts[0];
      log.log(Level.FINE, "Detected possible attachment: "
          + "listBase={0}, itemId={1}", new Object[] {listBase, itemId});
      if (!INTEGER_PATTERN.matcher(itemId).matches()) {
        log.fine("Item Id isn't an integer, so it isn't actually an id");
        log.exiting("SiteConnector", "getAttachmentDocContent", false);
        return false;
      }
      String listRedirectLocation = httpClient.getRedirectLocation(
          spUrlToUri(listBase).toURL(),
          authenticationHandler.getAuthenticationCookies(), sharepointUserAgent);
      // if listRedirectLocation is null, use listBase as list url. This is
      // possible if list has no views defined.
      // if listRedirectLocation is not null, it should begin with listBase
      // to be considered as valid list location else use listBase as listUrl.
      String listUrl =
          listRedirectLocation == null
              ? listBase
              : listRedirectLocation.startsWith(listBase) ? listRedirectLocation : listBase;

      log.log(Level.FINER, "List url {0}", listUrl);
      Holder<String> listIdHolder = new Holder<String>();
      boolean result = siteDataClient.getUrlSegments(listUrl,
          listIdHolder, null);
      // There is a possiblity that default view url for a list is empty but
      // list contains additional non default view.
      // In this case, SharePoint will redirect to non default view url.
      // getUrlSegments call fails for a non default view url.
      // So lets try with listBase for getUrlSegments.
      if (!result && !listUrl.equals(listBase)) {
        result = siteDataClient.getUrlSegments(listBase, listIdHolder, null);
      }
      // For valid lists, one of the getUrlSegments calls above should work on
      // SP2010 and SP2013. It can still fail for SP2007. So lets try with
      // ItemId url. This will fail if parent item is inside a folder.
      if (!result) {
        log.fine("Could not get list id from list url");
        // AllItems.aspx may not be the default view, so hope that list items
        // follow the ${id}_.000-style format and that there aren't any folders.
        result = siteDataClient.getUrlSegments(
            listBase + "/" + itemId + "_.000", listIdHolder, null);
        if (!result) {
          log.fine("Could not get list id from list item url");
          log.exiting("SiteConnector", "getAttachmentDocContent", false);
          return false;
        }
      }
      String listId = listIdHolder.value;
      if (listId == null) {
        log.fine("List URL does not point to a list");
        log.exiting("SiteConnector", "getAttachmentDocContent", false);
        return false;
      }
      // We have verified that the part before /Attachments/ is a List. Since
      // lists can't have "Attachments" as a child folder, we are very certain
      // that if the document exists it is an attachment.
      log.fine("Suspected attachment verified as being an attachment, assuming "
          + "it exists.");
      CachedList l = rareModCache.getList(siteDataClient, listId);
      CachedWeb w = rareModCache.getWeb(siteDataClient);
      if (TrueFalseType.TRUE.equals(l.noIndex) || isWebNoIndex(w)) {
        log.fine("Document marked for NoIndex");
        indexingService.deleteItem(entry.getId());
        log.exiting("SiteConnector", "getAttachmentDocContent", true);
        return true;
      }

      ItemData itemData = siteDataClient.getContentItem(listId, itemId);
      Xml xml = itemData.getXml();
      Element data = getFirstChildWithName(xml, DATA_ELEMENT);
      String itemCount = data.getAttribute("ItemCount");
      if ("0".equals(itemCount)) {
        log.fine("Could not get parent list item as ItemCount is 0.");
        log.exiting("SiteConnector", "getAttachmentDocContent", false);
        // Returing false here instead of returing 404 to avoid wrongly
        // identifying file documents as attachments when DocumentLibrary has
        // folder name Attachments. Returing false here would allow code
        // to see if this document was a regular file in DocumentLibrary.
        return false;
      }
      Element row = getChildrenWithName(data, ROW_ELEMENT).get(0);
      String scopeId = row.getAttribute(OWS_SCOPEID_ATTRIBUTE).split(";#", 2)[1];
      scopeId = scopeId.toLowerCase(Locale.ENGLISH);

      String strAttachments = row.getAttribute(OWS_ATTACHMENTS_ATTRIBUTE);
      int attachments = (strAttachments == null || "".equals(strAttachments))
          ? 0 : Integer.parseInt(strAttachments);
      if (attachments <= 0) {
        // Either the attachment has been removed or there was a really odd
        // collection of documents in a Document Library. Therefore, we let the
        // code continue to try to determine if this is a File.
        log.fine("Parent list item has no child attachments");
        log.exiting("SiteConnector", "getAttachmentDocContent", false);
        return false;
      }
      com.google.api.services.springboardindex.model.Item item =
          new com.google.api.services.springboardindex.model.Item();
      item.setId(entry.getId());

      boolean allowAnonymousAccess = isAllowAnonymousReadForList(l)
          && scopeId.equals(l.scopeId.toLowerCase(Locale.ENGLISH))
          && isAllowAnonymousPeekForWeb(w)
          && (!isDenyAnonymousAccessOnVirtualServer());
      if (!allowAnonymousAccess) {
        String listItemUrl = row.getAttribute(OWS_SERVERURL_ATTRIBUTE);
        item.setInheritanceType(Acl.InheritanceType.PARENT_OVERRIDE.toString())
            .setInheritFrom(encodeDocId(listItemUrl));
      }

      // If the attachment doesn't exist, then this responds Not Found.
      getFileDocContent(entry, item, true);
      log.exiting("SiteConnector", "getAttachmentDocContent", true);
      return true;
    }

    private Map<String, PrincipalInfo> resolvePrincipals(
        List<String> principalsToResolve) {
      Map<String, PrincipalInfo> resolved = new HashMap<String, PrincipalInfo>();
      if (principalsToResolve.isEmpty()) {
        return resolved;
      }
      ArrayOfString aos = new ArrayOfString();
      aos.getString().addAll(principalsToResolve);
      ArrayOfPrincipalInfo resolvePrincipals = people.resolvePrincipals(
          aos, SPPrincipalType.ALL, false);
      List<PrincipalInfo> principals = resolvePrincipals.getPrincipalInfo();
      // using loginname from input list principalsToResolve as a key
      // instead of returned PrincipalInfo.getAccountName() as with claims
      // authentication PrincipalInfo.getAccountName() is always encoded.
      // e.g. if login name from Policy is NT Authority\Local Service
      // returned account name is i:0#.w|NT Authority\Local Service
      for (int i = 0; i < principalsToResolve.size(); i++) {
        resolved.put(principalsToResolve.get(i), principals.get(i));
      }
      return resolved;
    }

    private MemberIdMapping retrieveMemberIdMapping() throws IOException {
      log.entering("SiteConnector", "retrieveMemberIdMapping");
      Site site = siteDataClient.getContentSite();
      Map<Integer, Principal> map = new HashMap<Integer, Principal>();
      for (GroupMembership.Group group : site.getGroups().getGroup()) {
        String sharepointLocalGroup =
            getPrincipalName(
                group.getGroup().getName(), defaultNamespace + "_" + site.getMetadata().getURL());
        Principal localGroup = Acl.getExternalGroupPrincipal(sharepointLocalGroup);
        map.put(group.getGroup().getID(), localGroup);
      }
      for (UserDescription user : site.getWeb().getUsers().getUser()) {
        Principal principal = userDescriptionToPrincipal(user);
        if (principal == null) {
          log.log(Level.WARNING,
              "Unable to determine login name. Skipping user with ID {0}",
              user.getID());
          continue;
        }
        map.put(user.getID(), principal);
      }
      MemberIdMapping mapping = new MemberIdMapping(map);
      log.exiting("SiteConnector", "retrieveMemberIdMapping", mapping);
      return mapping;
    }

    private Map<ExternalGroup, Collection<Principal>> computeMembersForGroups(
        GroupMembership groups) {
      Map<ExternalGroup, Collection<Principal>> defs =
          new HashMap<ExternalGroup, Collection<Principal>>();
      for (GroupMembership.Group group : groups.getGroup()) {
        String sharepointLocalGroup =
            getPrincipalName(group.getGroup().getName(), defaultNamespace + "_" + siteUrl);
        List<Principal> members = new ArrayList<Principal>();
        if (group.getUsers() == null) {
          for (UserDescription user : group.getUsers().getUser()) {
            Principal principal = userDescriptionToPrincipal(user);
            if (principal == null) {
              log.log(
                  Level.WARNING,
                  "Unable to determine login name. Skipping user with ID {0}",
                  user.getID());
              continue;
            }
            members.add(principal);
          }
        }
        defs.put(Acl.getExternalGroup(sharepointLocalGroup, members), members);
      }
      return defs;
    }

    private Principal userDescriptionToPrincipal(UserDescription user) {
      boolean isDomainGroup = (user.getIsDomainGroup() == TrueFalseType.TRUE);
      String userName = getLoginNameForPrincipal(
          user.getLoginName(), user.getName(), isDomainGroup);
      if (userName == null) {
        return null;
      }
      if (isDomainGroup) {
        return Acl.getExternalGroupPrincipal(getPrincipalName(userName, defaultNamespace));
      } else {
        return Acl.getExternalUserPrincipal(getPrincipalName(userName, defaultNamespace));
      }
    }

    private MemberIdMapping retrieveSiteUserMapping() {
      log.entering("SiteConnector", "retrieveSiteUserMapping");
      GetUserCollectionFromSiteResponse.GetUserCollectionFromSiteResult result =
          userGroup.getUserCollectionFromSite();
      Map<Integer, Principal> map = new HashMap<Integer, Principal>();
      MemberIdMapping mapping;
      if (result == null) {
        mapping = new MemberIdMapping(map);
        log.exiting("SiteConnector", "retrieveSiteUserMapping", mapping);
        return mapping;
      }
      GetUserCollectionFromSiteResult.GetUserCollectionFromSite siteUsers =
          result.getGetUserCollectionFromSite();
      if (siteUsers.getUsers() == null) {
        mapping = new MemberIdMapping(map);
        log.exiting("SiteConnector", "retrieveSiteUserMapping", mapping);
        return mapping;
      }
      for (User user : siteUsers.getUsers().getUser()) {
        boolean isDomainGroup = (user.getIsDomainGroup()
            == com.microsoft.schemas.sharepoint.soap.directory.TrueFalseType.TRUE);

        String userName = getLoginNameForPrincipal(
            user.getLoginName(), user.getName(), isDomainGroup);
        if (userName == null) {
          log.log(Level.WARNING,
              "Unable to determine login name. Skipping user with ID {0}",
              user.getID());
          continue;
        }
        if (isDomainGroup) {
          map.put(
              (int) user.getID(),
              Acl.getExternalGroupPrincipal(getPrincipalName(userName, defaultNamespace)));
        } else {
          map.put(
              (int) user.getID(),
              Acl.getExternalUserPrincipal(getPrincipalName(userName, defaultNamespace)));
        }
      }
      mapping = new MemberIdMapping(map);
      log.exiting("SiteConnector", "retrieveSiteUserMapping", mapping);
      return mapping;
    }

    private SiteConnector getConnectorForUrl(String url) throws IOException {
      log.entering("SiteConnector", "getConnectorForUrl", url);
      Holder<String> site = new Holder<String>();
      Holder<String> web = new Holder<String>();
      long result = siteDataClient.getSiteAndWeb(url, site, web);

      if (result != 0) {
        log.exiting("SiteConnector", "getConnectorForUrl", null);
        return null;
      }
      SiteConnector siteConnector = getSiteConnector(site.value, web.value);
      log.exiting("SiteConnector", "getConnectorForUrl", siteConnector);
      return siteConnector;
    }

    public SiteDataClient getSiteDataClient() {
      return siteDataClient;
    }
  }

  @VisibleForTesting
  static class FileInfo {
    /** Non-null contents. */
    private final InputStream contents;
    /** Non-null headers. Alternates between header name and header value. */
    private final List<String> headers;

    private FileInfo(InputStream contents, List<String> headers) {
      this.contents = contents;
      this.headers = headers;
    }

    public InputStream getContents() {
      return contents;
    }

    public List<String> getHeaders() {
      return headers;
    }

    public int getHeaderCount() {
      return headers.size() / 2;
    }

    public String getHeaderName(int i) {
      return headers.get(2 * i);
    }

    public String getHeaderValue(int i) {
      return headers.get(2 * i + 1);
    }

    /**
     * Find the first header with {@code name}, ignoring case.
     */
    public String getFirstHeaderWithName(String name) {
      String nameLowerCase = name.toLowerCase(Locale.ENGLISH);
      for (int i = 0; i < getHeaderCount(); i++) {
        String headerNameLowerCase = getHeaderName(i).toLowerCase(Locale.ENGLISH);
        if (headerNameLowerCase.equals(nameLowerCase)) {
          return getHeaderValue(i);
        }
      }
      return null;
    }

    public static class Builder {
      private InputStream contents;
      private List<String> headers = Collections.emptyList();

      public Builder(InputStream contents) {
        setContents(contents);
      }

      public Builder setContents(InputStream contents) {
        if (contents == null) {
          throw new NullPointerException();
        }
        this.contents = contents;
        return this;
      }

      /**
       * Sets the headers recieved as a response. List must alternate between
       * header name and header value.
       */
      public Builder setHeaders(List<String> headers) {
        if (headers == null) {
          throw new NullPointerException();
        }
        if (headers.size() % 2 != 0) {
          throw new IllegalArgumentException(
              "headers must have an even number of elements");
        }
        this.headers = Collections.unmodifiableList(
            new ArrayList<String>(headers));
        return this;
      }

      public FileInfo build() {
        return new FileInfo(contents, headers);
      }
    }
  }

  @VisibleForTesting
  interface HttpClient {
    /**
     * The caller must call {@code fileInfo.getContents().close()} after use.
     *
     * @return {@code null} if not found, {@code FileInfo} instance otherwise
     */
    public FileInfo issueGetRequest(
        URL url,
        List<String> authenticationCookies,
        String sharepointUserAgent,
        int maxRedirectsToFollow,
        boolean performBrowserLeniency)
        throws IOException;

    public String getRedirectLocation(
        URL url, List<String> authenticationCookies, String sharepointUserAgent) throws IOException;

    public HttpURLConnection getHttpURLConnection(URL url) throws IOException;
  }

  static class HttpClientImpl implements HttpClient {
    @Override
    public FileInfo issueGetRequest(URL url, List<String> authenticationCookies,
        String sharepointUserAgent, int maxRedirectsToFollow,
        boolean performBrowserLeniency) throws IOException {
      int redirectAttempt = 0;
      final URL initialRequest = url;
      HttpURLConnection conn;
      do {
        log.log(Level.FINER, "Handling URL {0}", url);
        conn = getHttpURLConnection(url);
        if (authenticationCookies.isEmpty()) {
          conn.addRequestProperty("X-FORMS_BASED_AUTH_ACCEPTED", "f");
        } else {
          // Pass authentication cookies only if talking to same SharePoint
          // instance as initial request.
          if (initialRequest.getHost().equalsIgnoreCase(url.getHost())
              && initialRequest.getPort() == url.getPort()) {
            for (String cookie : authenticationCookies) {
              conn.addRequestProperty("Cookie", cookie);
            }
          } else {
            log.log(Level.FINER,
                "Not passing authentication cookies for {0}", url);
          }
        }
        if (!"".equals(sharepointUserAgent)) {
          conn.addRequestProperty("User-Agent", sharepointUserAgent);
        }
        conn.setDoInput(true);
        conn.setDoOutput(false);
        // Set follow redirects to true here if connector need not to handle
        // encoding of redirect URLs.
        conn.setInstanceFollowRedirects(!performBrowserLeniency);
        int responseCode = conn.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
          return null;
        }
        if (responseCode == HttpURLConnection.HTTP_OK) {
          break;
        }
        if (responseCode != HttpURLConnection.HTTP_MOVED_TEMP
            && responseCode != HttpURLConnection.HTTP_MOVED_PERM) {
          InputStream inputStream =
              responseCode >= HttpURLConnection.HTTP_BAD_REQUEST
              ? conn.getErrorStream() : conn.getInputStream();
          inputStream.close();
          throw new IOException(String.format("Got status code %d for URL %s", responseCode, url));
        }
        if (maxRedirectsToFollow < 0) {
          throw new AssertionError();
        }
        if ((maxRedirectsToFollow == 0)) {
          throw new IOException(
              String.format(
                  "Got status code %d for url %s "
                      + "but connector is configured to follow 0 redirects.",
                  responseCode, initialRequest));
        }
        redirectAttempt++;
        String redirectLocation = conn.getHeaderField("Location");
        // Close input stream
        InputStream inputStream = conn.getInputStream();
        inputStream.close();
        if (Strings.isNullOrEmpty(redirectLocation)) {
          throw new IOException(
              "No redirect location available for URL " + url);
        }
        log.log(Level.FINE, "Redirected to URL {0} from URL {1}",
            new Object[] {redirectLocation, url});
        url = encodeSharePointUrl(redirectLocation, performBrowserLeniency);
      } while (redirectAttempt <= maxRedirectsToFollow);
      if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
        throw new IOException(
            String.format(
                "Got status code %d for initial " + "request %s after %d redirect attempts.",
                conn.getResponseCode(), initialRequest, redirectAttempt));
      }
      String errorHeader = conn.getHeaderField("SharePointError");
      // SharePoint adds header SharePointError to response to indicate error
      // on SharePoint for requested URL.
      // errorHeader = 2 if SharePoint rejects current request because
      // of current processing load
      // errorHeader = 0 for other errors on SharePoint server

      if (errorHeader != null) {
        if ("2".equals(errorHeader)) {
          throw new IOException(
              "Got error 2 from SharePoint for URL ["
                  + url
                  + "]. Error Code 2 indicates SharePoint has rejected current "
                  + "request because of current processing load on SharePoint.");
        } else {
          throw new IOException(
              "Got error " + errorHeader + " from SharePoint for URL [" + url + "].");
        }
      }

      List<String> headers = new LinkedList<String>();
      // Start at 1 since index 0 is special.
      for (int i = 1;; i++) {
        String key = conn.getHeaderFieldKey(i);
        if (key == null) {
          break;
        }
        String value = conn.getHeaderField(i);
        headers.add(key);
        headers.add(value);
      }
      log.log(Level.FINER, "Response HTTP headers: {0}", headers);
      return new FileInfo.Builder(conn.getInputStream()).setHeaders(headers)
          .build();
    }

    @Override
    public String getRedirectLocation(
        URL url, List<String> authenticationCookies, String sharepointUserAgent)
        throws IOException {

      // Handle Unicode. Java does not properly encode the GET.
      try {
        url = new URL(url.toURI().toASCIIString());
      } catch (URISyntaxException ex) {
        throw new IOException(ex);
      }
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      try {
        if (authenticationCookies.isEmpty()) {
          conn.addRequestProperty("X-FORMS_BASED_AUTH_ACCEPTED", "f");
        } else {
          for (String cookie : authenticationCookies) {
            conn.addRequestProperty("Cookie", cookie);
          }
        }
        if (!"".equals(sharepointUserAgent)) {
          conn.addRequestProperty("User-Agent", sharepointUserAgent);
        }
        conn.setDoInput(true);
        conn.setDoOutput(false);
        conn.setInstanceFollowRedirects(false);
        if (conn.getResponseCode() != HttpURLConnection.HTTP_MOVED_TEMP) {
          log.log(Level.WARNING,
              "Received response code {0} instead of 302 for URL {1}",
              new Object[]{conn.getResponseCode(), url});
          return null;
        }
        return conn.getHeaderField("Location");
      } finally {
        InputStream inputStream = conn.getResponseCode() >= 400
            ? conn.getErrorStream() : conn.getInputStream();
        inputStream.close();
      }
    }

    @Override
    public HttpURLConnection getHttpURLConnection(URL url) throws IOException {
      return (HttpURLConnection)url.openConnection();
    }
  }

  @VisibleForTesting
  interface SoapFactory {
    /**
     * The {@code endpoint} string is a SharePoint URL, meaning that spaces are
     * not encoded.
     */
    public SiteDataSoap newSiteData(String endpoint);

    public UserGroupSoap newUserGroup(String endpoint);

    public PeopleSoap newPeople(String endpoint);
  }

  @VisibleForTesting
  static class SoapFactoryImpl implements SoapFactory {
    private final Service siteDataService;
    private final Service userGroupService;
    private final Service peopleService;

    public SoapFactoryImpl() {
      this.siteDataService = SiteDataClient.createSiteDataService();
      this.userGroupService =
          Service.create(
              UserGroupSoap.class.getResource("/UserGroup.wsdl"),
              new QName(XMLNS_DIRECTORY, "UserGroup"));
      this.peopleService =
          Service.create(PeopleSoap.class.getResource("/People.wsdl"), new QName(XMLNS, "People"));
    }

    private static String handleEncoding(String endpoint) {
      // Handle Unicode. Java does not properly encode the POST path.
      return URI.create(endpoint).toASCIIString();
    }

    @Override
    public SiteDataSoap newSiteData(String endpoint) {
      EndpointReference endpointRef = new W3CEndpointReferenceBuilder()
          .address(handleEncoding(endpoint)).build();
      return siteDataService.getPort(endpointRef, SiteDataSoap.class);
    }

    @Override
    public UserGroupSoap newUserGroup(String endpoint) {
      EndpointReference endpointRef = new W3CEndpointReferenceBuilder()
          .address(handleEncoding(endpoint)).build();
      return userGroupService.getPort(endpointRef, UserGroupSoap.class);
    }

    @Override
    public PeopleSoap newPeople(String endpoint) {
      EndpointReference endpointRef = new W3CEndpointReferenceBuilder()
          .address(handleEncoding(endpoint)).build();
      return peopleService.getPort(endpointRef, PeopleSoap.class);
    }
  }

  private static class NtlmAuthenticator extends Authenticator {
    private final String username;
    private final char[] password;
    private final Set<String> permittedHosts = new HashSet<String>();

    public NtlmAuthenticator(String username, String password) {
      this.username = username;
      this.password = password.toCharArray();
    }

    public void addPermitForHost(URL urlContainingHost) {
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

  /** As defined at http://msdn.microsoft.com/en-us/library/ee394878.aspx . */
  @SuppressWarnings("unused")
  private static class SPBasePermissions {
    public static final long EMPTYMASK= 0x0000000000000000;
    public static final long VIEWLISTITEMS= 0x0000000000000001;
    public static final long ADDLISTITEMS= 0x0000000000000002;
    public static final long EDITLISTITEMS= 0x0000000000000004;
    public static final long DELETELISTITEMS= 0x0000000000000008;
    public static final long APPROVEITEMS= 0x0000000000000010;
    public static final long OPENITEMS= 0x0000000000000020;
    public static final long VIEWVERSIONS= 0x0000000000000040;
    public static final long DELETEVERSIONS= 0x0000000000000080;
    public static final long CANCELCHECKOUT= 0x0000000000000100;
    public static final long MANAGEPERSONALVIEWS= 0x0000000000000200;
    public static final long MANAGELISTS= 0x0000000000000800;
    public static final long VIEWFORMPAGES= 0x0000000000001000;
    public static final long OPEN= 0x0000000000010000;
    public static final long VIEWPAGES= 0x0000000000020000;
    public static final long ADDANDCUSTOMIZEPAGES= 0x0000000000040000;
    public static final long APPLYTHEMEANDBORDER= 0x0000000000080000;
    public static final long APPLYSTYLESHEETS= 0x0000000000100000;
    public static final long VIEWUSAGEDATA= 0x0000000000200000;
    public static final long CREATESSCSITE= 0x0000000000400000;
    public static final long MANAGESUBWEBS= 0x0000000000800000;
    public static final long CREATEGROUPS= 0x0000000001000000;
    public static final long MANAGEPERMISSIONS= 0x0000000002000000;
    public static final long BROWSEDIRECTORIES= 0x0000000004000000;
    public static final long BROWSEUSERINFO= 0x0000000008000000;
    public static final long ADDDELPRIVATEWEBPARTS= 0x0000000010000000;
    public static final long UPDATEPERSONALWEBPARTS= 0x0000000020000000;
    public static final long MANAGEWEB= 0x0000000040000000;
    public static final long USECLIENTINTEGRATION= 0x0000001000000000L;
    public static final long USEREMOTEAPIS= 0x0000002000000000L;
    public static final long MANAGEALERTS= 0x0000004000000000L;
    public static final long CREATEALERTS= 0x0000008000000000L;
    public static final long EDITMYUSERINFO= 0x0000010000000000L;
    public static final long ENUMERATEPERMISSIONS= 0x4000000000000000L;
    public static final long FULLMASK= 0x7FFFFFFFFFFFFFFFL;
  }

  private class MemberIdMappingCallable implements Callable<MemberIdMapping> {
    private final String siteUrl;

    public MemberIdMappingCallable(String siteUrl) {
      if (siteUrl == null) {
        throw new NullPointerException();
      }
      this.siteUrl = siteUrl;
    }

    @Override
    public MemberIdMapping call() throws Exception {
      try {
        return memberIdsCache.get(siteUrl);
      } catch (ExecutionException ex) {
        Throwable cause = ex.getCause();
        if (cause instanceof Exception) {
          throw (Exception) cause;
        } else if (cause instanceof Error) {
          throw (Error) cause;
        } else {
          throw new AssertionError(cause);
        }
      }
    }
  }

  @VisibleForTesting
  class SiteUserIdMappingCallable implements Callable<MemberIdMapping> {
    private final String siteUrl;

    public SiteUserIdMappingCallable(String siteUrl) {
      if (siteUrl == null) {
        throw new NullPointerException();
      }
      this.siteUrl = siteUrl;
    }

    @Override
    public MemberIdMapping call() throws Exception {
      try {
        return siteUserCache.get(siteUrl);
      } catch (ExecutionException ex) {
        Throwable cause = ex.getCause();
        if (cause instanceof Exception) {
          throw (Exception) cause;
        } else if (cause instanceof Error) {
          throw (Error) cause;
        } else {
          throw new AssertionError(cause);
        }
      }
    }
  }

  private class MemberIdsCacheLoader extends AsyncCacheLoader<String, MemberIdMapping> {
    @Override
    protected Executor executor() {
      return executor;
    }

    @Override
    public MemberIdMapping load(String site) throws IOException {
      return getSiteConnector(site, site).retrieveMemberIdMapping();
    }
  }

  private class SiteUserCacheLoader extends AsyncCacheLoader<String, MemberIdMapping> {
    @Override
    protected Executor executor() {
      return executor;
    }

    @Override
    public MemberIdMapping load(String site) throws IOException {
      return getSiteConnector(site, site).retrieveSiteUserMapping();
    }
  }

  private static class CachedThreadPoolFactory implements Callable<ExecutorService> {
    @Override
    public ExecutorService call() {
      return Executors.newCachedThreadPool();
    }
  }

  @Override
  public void saveCheckpoint(boolean isShutdown) throws IOException, InterruptedException {
    // TODO(tvartak): Auto-generated method stub

  }
}
