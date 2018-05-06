package com.google.enterprise.cloud.search.sharepoint;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.FieldOrValue.withValue;

import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.util.DateTime;
import com.google.api.client.util.Strings;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemMetadata;
import com.google.api.services.cloudsearch.v1.model.Principal;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.enterprise.cloud.search.sharepoint.SharePointIncrementalCheckpoint.ChangeObjectType;
import com.google.enterprise.cloud.search.sharepoint.SharePointIncrementalCheckpoint.DiffKind;
import com.google.enterprise.cloud.search.sharepoint.SiteDataClient.CursorPaginator;
import com.google.enterprise.cloud.search.sharepoint.SiteDataClient.Paginator;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl.InheritanceType;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.ContentFormat;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ApiOperation;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ApiOperations;
import com.google.enterprise.cloudsearch.sdk.indexing.template.CheckpointClosableIterable;
import com.google.enterprise.cloudsearch.sdk.indexing.template.CheckpointClosableIterableImpl;
import com.google.enterprise.cloudsearch.sdk.indexing.template.PushItems;
import com.google.enterprise.cloudsearch.sdk.indexing.template.Repository;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryContext;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryDoc;
import com.microsoft.schemas.sharepoint.soap.ContentDatabase;
import com.microsoft.schemas.sharepoint.soap.ContentDatabases;
import com.microsoft.schemas.sharepoint.soap.ItemData;
import com.microsoft.schemas.sharepoint.soap.ListMetadata;
import com.microsoft.schemas.sharepoint.soap.Lists;
import com.microsoft.schemas.sharepoint.soap.SPContentDatabase;
import com.microsoft.schemas.sharepoint.soap.SPList;
import com.microsoft.schemas.sharepoint.soap.SPListItem;
import com.microsoft.schemas.sharepoint.soap.SPSite;
import com.microsoft.schemas.sharepoint.soap.SPSiteMetadata;
import com.microsoft.schemas.sharepoint.soap.SPWeb;
import com.microsoft.schemas.sharepoint.soap.Scopes;
import com.microsoft.schemas.sharepoint.soap.Site;
import com.microsoft.schemas.sharepoint.soap.Sites;
import com.microsoft.schemas.sharepoint.soap.VirtualServer;
import com.microsoft.schemas.sharepoint.soap.Web;
import com.microsoft.schemas.sharepoint.soap.WebMetadata;
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
import java.util.Optional;
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

  private static final String PUSH_TYPE_MODIFIED = "MODIFIED";
  private static final String PUSH_TYPE_NOT_MODIFIED = "NOT_MODIFIED";

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
  /** The time metadata or content was created. */
  private static final String OWS_CREATED_ATTRIBUTE = "ows_Created";
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
   * Row attribute that contains a hierarchical hex number that describes the type of object. See
   * http://msdn.microsoft.com/en-us/library/aa543822.aspx for more information about content type
   * IDs.
   */
  private static final String OWS_CONTENTTYPEID_ATTRIBUTE = "ows_ContentTypeId";
  /** As described at http://msdn.microsoft.com/en-us/library/aa543822.aspx . */
  private static final String CONTENTTYPEID_DOCUMENT_PREFIX = "0x0101";


  static final String VIRTUAL_SERVER_ID = "ROOT_NEW";
  static final String SITE_COLLECTION_ADMIN_FRAGMENT = "admin";

  private static final String DEFAULT_USER_NAME =
      System.getProperty("os.name").contains("Windows") ? "" : null;
  private static final String DEFAULT_PASSWORD =
      System.getProperty("os.name").contains("Windows") ? "" : null;

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
  private final ThreadLocal<DateFormat> createdDateFormat =
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
  // TODO(tvartak) : Add support for authentication handler implementations.
  private FormsAuthenticationHandler authenticationHandler = null;
  private boolean performBrowserLeniency;
  private HttpClient httpClient;

  private final HttpClientImpl.Builder httpClientBuilder;
  private final SiteConnectorFactoryImpl.Builder siteConnectorFactoryBuilder;
  private SharePointIncrementalCheckpoint initIncrementalCheckpoint;

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
  public void init(RepositoryContext repositoryContext) throws RepositoryException {
    checkState(Configuration.isInitialized(), "config should be initailized");
    String sharePointServer = Configuration.getString("sharepoint.server", null).get();
    performBrowserLeniency =
        Configuration.getBoolean("connector.lenientUrlRulesAndCustomRedirect", true).get();
    String username = Configuration.getString("sharepoint.username", DEFAULT_USER_NAME).get();
    String password = Configuration.getString("sharepoint.password", DEFAULT_PASSWORD).get();
    ntlmAuthenticator = new NtlmAuthenticator(username, password);
    try {
      SharePointUrl configuredUrl = buildSharePointUrl(sharePointServer);
      sharepointConfiguration =
          new SharePointConfiguration.Builder(configuredUrl)
              .setSharePointSiteCollectionOnly(
                  Configuration.getString("sharepoint.siteCollectionOnly", "").get())
              .build();
      ntlmAuthenticator.addPermitForHost(configuredUrl.toURL());
    } catch (Exception e) {
      throw new InvalidConfigurationException("Error validating SharePoint URL", e);
    }
    if (!"".equals(username) && !"".equals(password)) {
      // Unfortunately, this is a JVM-wide modification.
      Authenticator.setDefault(ntlmAuthenticator);
    }
    String sharepointUserAgent = Configuration.getString("sharepoint.userAgent", "").get().trim();
    int socketTimeoutMillis =
        Configuration.getInteger("sharepoint.webservices.socketTimeoutSecs", 30).get() * 1000;
    int readTimeOutMillis =
        Configuration.getInteger("sharepoint.webservices.readTimeOutSecs", 180).get() * 1000;

    boolean xmlValidation = Configuration.getBoolean("sharepoint.xmlValidation", false).get();
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
    initIncrementalCheckpoint = computeIncrementalCheckpoint();
  }

  @Override
  public CheckpointClosableIterable getIds(byte[] checkpoint) throws RepositoryException {
    log.entering("SharePointConnector", "traverse");
    Collection<ApiOperation> toReturn =
        sharepointConfiguration.isSiteCollectionUrl()
            ? getDocIdsSiteCollectionOnly()
            : getDocIdsVirtualServer();
    log.exiting("SharePointConnector", "traverse");
    return new CheckpointClosableIterableImpl.Builder(toReturn).build();
  }

  @Override
  public CheckpointClosableIterable getChanges(byte[] checkpoint) throws RepositoryException {
    SharePointIncrementalCheckpoint previousCheckpoint;
    try {
      Optional<SharePointIncrementalCheckpoint> parsedCheckpoint =
          Optional.ofNullable(SharePointIncrementalCheckpoint.parse(checkpoint));
      previousCheckpoint =
          parsedCheckpoint.filter(e -> e.isValid()).orElse(initIncrementalCheckpoint);
    } catch (IOException e) {
      log.log(
          Level.WARNING, "Error parsing checkpoint. Resetting to checkpoint computed at init.", e);
      previousCheckpoint = initIncrementalCheckpoint;
    }
    SharePointIncrementalCheckpoint currentCheckpoint = computeIncrementalCheckpoint();
    // Possible mismatch between saved checkpoint and current connector configuration if connector
    // switch from VirtualServer mode to siteCollectionOnly mode or vice-versa.
    boolean mismatchObjectType =
        previousCheckpoint.getObjectType() != currentCheckpoint.getObjectType();
    if (mismatchObjectType) {
      log.log(
          Level.INFO,
          "Mismatch between previous checkpoint object type {0} and "
              + "current checkpoint object type {1}. Resetting to checkpoint computed at init.",
          new Object[] {previousCheckpoint.getObjectType(), currentCheckpoint.getObjectType()});
      previousCheckpoint = initIncrementalCheckpoint;
    }
    if (sharepointConfiguration.isSiteCollectionUrl()) {
      checkState(
          currentCheckpoint.getObjectType() == ChangeObjectType.SITE_COLLECTION,
          "Mismatch between SharePoint Configuration and Checkpoint Type. "
              + "Expected SITE_COLLECTION. Actual %s",
          currentCheckpoint.getObjectType());
      try {
        return getChangesSiteCollectionOnlyMode(previousCheckpoint, currentCheckpoint);
      } catch (IOException e) {
        throw buildRepositoryExceptionFromIOException(
            "error processing changes SiteCollectionOnlyMode", e);
      }
    } else {
      checkState(
          currentCheckpoint.getObjectType() == ChangeObjectType.CONTENT_DB,
          "Mismatch between SharePoint Configuration and Checkpoint Type. "
              + "Expected CONTENT_DB. Actual %s",
          currentCheckpoint.getObjectType());
      try {
        return getChangesVirtualServerMode(previousCheckpoint, currentCheckpoint);
      } catch (IOException e) {
        throw buildRepositoryExceptionFromIOException(
            "error processing changes VirtualServerMode", e);
      }
    }
  }

  private CheckpointClosableIterable getChangesSiteCollectionOnlyMode(
      SharePointIncrementalCheckpoint previous, SharePointIncrementalCheckpoint current)
      throws IOException {
    Map<DiffKind, Set<String>> diff = previous.diff(current);
    Set<String> notModified = diff.get(DiffKind.NOT_MODIFIED);
    if (!notModified.isEmpty()) {
      checkState(
          notModified.size() == 1,
          "Unexpected number of Change ObjectIds %s for SiteCollectionOnlyMode",
          notModified);
      // No Changes since last checkpoint.
      return new CheckpointClosableIterableImpl.Builder(Collections.emptyList())
          .setCheckpoint(previous.encodePayload())
          .setHasMoreItems(false)
          .build();
    }

    Set<String> modified = diff.get(DiffKind.MODIFIED);
    if (!modified.isEmpty()) {
      // Process Changes since last checkpoint.
      SiteConnector scConnector = getSiteConnectorForSiteCollectionOnly();
      String siteCollectionGuid = Iterables.getOnlyElement(modified);
      String changeToken = previous.getTokens().get(siteCollectionGuid);
      CursorPaginator<SPSite, String> changes = scConnector
          .getSiteDataClient()
          .getChangesSPSite(siteCollectionGuid, changeToken);
      PushItems.Builder modifiedItems = new PushItems.Builder();
      SPSite change;
      while ((change = changes.next()) != null) {
        getModifiedDocIdsSite(scConnector, change, modifiedItems);
        changeToken = changes.getCursor();
      }
      SharePointIncrementalCheckpoint updatedCheckpoint =
          new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.SITE_COLLECTION)
              .addChangeToken(siteCollectionGuid, changeToken)
              .build();
      return new CheckpointClosableIterableImpl.Builder(
              Collections.singleton(modifiedItems.build()))
          .setCheckpoint(updatedCheckpoint.encodePayload())
          .setHasMoreItems(false)
          .build();
    }

    // This is a case where we try to handle change in configuration where connector is pointing to
    // different site collection.
    // Note : We rely on re-indexing of previously configured site collection to delete from index.
    // To support faster deletes we can either save previous site URL as part of checkpoint or
    // switch to SharePoint Object Id for item identifiers. For now we are ignoring DiffKind.REMOVE
    Set<String> added = diff.get(DiffKind.ADD);
    checkState(
        !added.isEmpty(),
        "In SiteCollectionOnlyMode current SiteCollection "
            + "should exist in MODIFIED or NOT_MODIFIED or ADD");
    SiteConnector scConnector = getSiteConnectorForSiteCollectionOnly();
    String siteCollectionGuid = Iterables.getOnlyElement(added);
    // Process Changes since initial checkpoint at start.
    String changeToken = initIncrementalCheckpoint.getTokens().get(siteCollectionGuid);
    CursorPaginator<SPSite, String> changes =
        scConnector.getSiteDataClient().getChangesSPSite(siteCollectionGuid, changeToken);
    PushItems.Builder modifiedItems = new PushItems.Builder();
    SPSite change;
    while ((change = changes.next()) != null) {
      getModifiedDocIdsSite(scConnector, change, modifiedItems);
      changeToken = changes.getCursor();
    }
    SharePointIncrementalCheckpoint updatedCheckpoint =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.SITE_COLLECTION)
            .addChangeToken(siteCollectionGuid, changeToken)
            .build();
    return new CheckpointClosableIterableImpl.Builder(Collections.singleton(modifiedItems.build()))
        .setCheckpoint(updatedCheckpoint.encodePayload())
        .setHasMoreItems(false)
        .build();
  }

  private void getModifiedDocIdsSite(
      SiteConnector scConnector, SPSite changes, PushItems.Builder pushItems) throws IOException {
    SPSite.Site site = changes.getSite();
    if (Objects.isNull(site)) {
      return;
    }
    SPSiteMetadata metadata = site.getMetadata();
    String encodedDocId = scConnector.encodeDocId(getCanonicalUrl(metadata.getURL()));
    SharePointObject siteCollection =
        new SharePointObject.Builder(SharePointObject.SITE_COLLECTION)
            .setUrl(encodedDocId)
            .setObjectId(metadata.getID())
            .setSiteId(metadata.getID())
            .setWebId(metadata.getID())
            .build();
    if (isModified(changes.getChange())) {
      pushItems.addPushItem(
          encodedDocId,
          new PushItem().encodePayload(siteCollection.encodePayload()).setType(PUSH_TYPE_MODIFIED));
    }
    List<SPWeb> changedWebs = changes.getSPWeb();
    if (changedWebs == null) {
      return;
    }
    for (SPWeb web : changedWebs) {
      getModifiedDocIdsWeb(siteCollection, web, pushItems);
    }
  }

  private void getModifiedDocIdsWeb(
      SharePointObject parentObject,
      SPWeb changes,
      PushItems.Builder pushItems)
      throws IOException {
    SPWeb.Web web = changes.getWeb();
    if (Objects.isNull(web)) {
      return;
    }
    WebMetadata metadata = web.getMetadata();
    String sharepointUrl = getCanonicalUrl(metadata.getURL());
    SiteConnector webConnector = getSiteConnector(parentObject.getUrl(), sharepointUrl);
    String encodedDocId = webConnector.encodeDocId(sharepointUrl);
    SharePointObject payload =
        new SharePointObject.Builder(SharePointObject.WEB)
            .setSiteId(parentObject.getSiteId())
            .setWebId(metadata.getID())
            .setUrl(encodedDocId)
            .setObjectId(metadata.getID())
            .build();
    if (isModified(changes.getChange())) {
      pushItems.addPushItem(
          encodedDocId,
          new PushItem().encodePayload(payload.encodePayload()).setType(PUSH_TYPE_MODIFIED));
    }

    List<Object> spObjects = changes.getSPFolderOrSPListOrSPFile();
    if (spObjects == null) {
      return;
    }
    for (Object choice : spObjects) {
      if (choice instanceof SPList) {
        getModifiedDocIdsList(webConnector, parentObject, (SPList) choice, pushItems);
      }
    }
  }

  private void getModifiedDocIdsList(
      SiteConnector webConnector,
      SharePointObject parentObject,
      SPList changes,
      PushItems.Builder pushItems)
      throws IOException {
    com.microsoft.schemas.sharepoint.soap.List list = changes.getList();
    if (Objects.isNull(list)) {
      return;
    }
    ListMetadata metadata = list.getMetadata();
    String encodedDocId = webConnector.encodeDocId(metadata.getDefaultViewUrl());
    SharePointObject payload =
        new SharePointObject.Builder(SharePointObject.LIST)
            .setSiteId(parentObject.getSiteId())
            .setWebId(parentObject.getWebId())
            .setUrl(encodedDocId)
            .setListId(metadata.getID())
            .setObjectId(metadata.getID())
            .build();
    if (isModified(changes.getChange())) {
      pushItems.addPushItem(
          encodedDocId,
          new PushItem().encodePayload(payload.encodePayload()).setType(PUSH_TYPE_MODIFIED));
    }
    List<Object> spObjects = changes.getSPViewOrSPListItem();
    if (spObjects == null) {
      return;
    }
    for (Object choice : spObjects) {
      // Ignore view change detection.

      if (choice instanceof SPListItem) {
        getModifiedDocIdsListItem(webConnector, payload, (SPListItem) choice, pushItems);
      }
    }
  }

  private void getModifiedDocIdsListItem(
      SiteConnector webConnector,
      SharePointObject parentObject,
      SPListItem changes,
      PushItems.Builder pushItems)
      throws IOException {
    if (isModified(changes.getChange())) {
      SPListItem.ListItem listItem = changes.getListItem();
      if (listItem == null) {
        return;
      }
      Object oData = listItem.getAny();
      if (!(oData instanceof Element)) {
        log.log(Level.WARNING, "Unexpected object type for data: {0}", oData.getClass());
      } else {
        Element data = (Element) oData;
        String serverUrl = data.getAttribute(OWS_SERVERURL_ATTRIBUTE);
        if (serverUrl == null) {
          log.log(
              Level.WARNING,
              "Could not find server url attribute for " + "list item {0}",
              changes.getId());
        } else {
          String encodedDocId = webConnector.encodeDocId(getCanonicalUrl(serverUrl));
          SharePointObject payload =
              new SharePointObject.Builder(SharePointObject.LIST_ITEM)
                  .setListId(parentObject.getListId())
                  .setSiteId(parentObject.getSiteId())
                  .setWebId(parentObject.getWebId())
                  .setUrl(encodedDocId)
                  .setObjectId("item")
                  .build();
          pushItems.addPushItem(
              encodedDocId,
              new PushItem().encodePayload(payload.encodePayload()).setType(PUSH_TYPE_MODIFIED));
        }
      }
    }
  }

  private boolean isModified(String change) {
    return !"Unchanged".equals(change) && !"Delete".equals(change);
  }

  private CheckpointClosableIterable getChangesVirtualServerMode(
      SharePointIncrementalCheckpoint previous, SharePointIncrementalCheckpoint current)
      throws IOException {
    SharePointIncrementalCheckpoint.Builder newCheckpoint =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.CONTENT_DB);
    Map<DiffKind, Set<String>> diff = previous.diff(current);
    Set<String> notModified = diff.get(DiffKind.NOT_MODIFIED);
    // Copy over not modified items
    for (String contentDbId : notModified) {
      newCheckpoint.addChangeToken(contentDbId, previous.getTokens().get(contentDbId));
    }

    // Process changes in previously known content DBs
    Set<String> modified = diff.get(DiffKind.MODIFIED);
    PushItems.Builder modifiedItems = new PushItems.Builder();
    SiteConnector vsSiteConnector = getSiteConnectorForVirtualServer();
    for (String contentDbId : modified) {
      newCheckpoint.addChangeToken(
          contentDbId,
          getModifiedDocIdsContentDb(
              vsSiteConnector, contentDbId, previous.getTokens().get(contentDbId), modifiedItems));
    }

    // Process newly discovered content DBs.
    // Note : Connector rely on reindexing to delete sites under deleted content databases.
    // Alternatively, if Content DB act as a container for site collection then we can simply delete
    // Content DB node.
    Set<String> added = diff.get(DiffKind.ADD);
    for (String contentDbId : added) {
      // Process newly added content DBs from init checkpoint if content DB was known during init
      // otherwise use values from current checkpoint.
      String changeToken =
          initIncrementalCheckpoint.getTokens().containsKey(contentDbId)
              ? initIncrementalCheckpoint.getTokens().get(contentDbId)
              : current.getTokens().get(contentDbId);
      newCheckpoint.addChangeToken(
          contentDbId,
          getModifiedDocIdsContentDb(vsSiteConnector, contentDbId, changeToken, modifiedItems));
    }

    return new CheckpointClosableIterableImpl.Builder(Collections.singleton(modifiedItems.build()))
        .setCheckpoint(newCheckpoint.build().encodePayload())
        .setHasMoreItems(false)
        .build();
  }

  private String getModifiedDocIdsContentDb(
      SiteConnector vsConnector,
      String contentDb,
      String lastChangeToken,
      PushItems.Builder modifiedItems)
      throws IOException {
    CursorPaginator<SPContentDatabase, String> changesContentDatabase =
        vsConnector.getSiteDataClient().getChangesContentDatabase(contentDb, lastChangeToken);
    String changeToken = lastChangeToken;
    boolean virtualServerAdded = false;
    SPContentDatabase change;
    while ((change = changesContentDatabase.next()) != null) {
      if (!virtualServerAdded && isModified(change.getChange())) {
        SharePointObject vsObject =
            new SharePointObject.Builder(SharePointObject.VIRTUAL_SERVER).build();
        PushItem pushItem =
            new PushItem().encodePayload(vsObject.encodePayload()).setType(PUSH_TYPE_MODIFIED);
        modifiedItems.addPushItem(VIRTUAL_SERVER_ID, pushItem);
        virtualServerAdded = true;
      }
      List<SPSite> changedSites = change.getSPSite();
      if (changedSites == null) {
        continue;
      }

      for (SPSite site : changedSites) {
        SPSite.Site changedSite = site.getSite();
        if (changedSite == null) {
          continue;
        }
        SPSiteMetadata metadata = changedSite.getMetadata();
        SiteConnector siteConnector;
        String canonicalUrl = getCanonicalUrl(metadata.getURL());
        try {
          siteConnector = getConnectorForDocId(canonicalUrl);
        } catch (URISyntaxException e) {
          throw new IOException(
              "Error creating SiteConnector for URL : " + canonicalUrl, e);
        }
        getModifiedDocIdsSite(siteConnector, site, modifiedItems);
      }
      changeToken = changesContentDatabase.getCursor();
    }
    return changeToken;
  }

  @Override
  public CheckpointClosableIterable getAllDocs(byte[] checkpoint) {
    return null;
  }

  @Override
  public ApiOperation getDoc(Item item) throws RepositoryException {
    checkNotNull(item);
    try {
      SharePointObject object = SharePointObject.parse(item.decodePayload());
      String objectType = object.getObjectType();
      if (!object.isValid()) {
        log.log(
            Level.WARNING,
            "Invalid SharePoint Objecct {0} on entry {1}",
            new Object[] {object, item});
        throw new RepositoryException.Builder().setErrorMessage("Invalid payload").build();
      }

      if (SharePointObject.NAMED_RESOURCE.equals(objectType)) {
        // Do not process named resource here.
        PushItem notModified =
            new PushItem().setType(PUSH_TYPE_NOT_MODIFIED).encodePayload(object.encodePayload());
        return new PushItems.Builder().addPushItem(item.getName(), notModified).build();
      }

      if (SharePointObject.VIRTUAL_SERVER.equals(objectType)) {
        return getVirtualServerDocContent();
      }

      SiteConnector siteConnector;
      try {
        siteConnector = getConnectorForDocId(item.getName());
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
      if (siteConnector == null) {
        return ApiOperations.deleteItem(item.getName());
      }

      if (SharePointObject.SITE_COLLECTION.equals(objectType)) {
        return getSiteCollectionDocContent(item, siteConnector, object);
      }
      if (SharePointObject.WEB.equals(objectType)) {
        return getWebDocContent(item, siteConnector, object);
      }
      if (SharePointObject.LIST.equals(objectType)) {
        return getListDocContent(item, siteConnector, object);
      }
      if (SharePointObject.LIST_ITEM.equals(objectType)) {
        return getListItemDocContent(item, siteConnector, object);
      }
      if (SharePointObject.ATTACHMENT.equals(objectType)) {
        return getAttachmentDocContent(item, siteConnector, object);
      }
      PushItem notModified =
          new PushItem().setType(PUSH_TYPE_NOT_MODIFIED).encodePayload(object.encodePayload());
      return new PushItems.Builder().addPushItem(item.getName(), notModified).build();
    } catch (IOException e) {
      throw buildRepositoryExceptionFromIOException(
          String.format("error processing item %s", item), e);
    }
  }

  @Override
  public boolean exists(Item item) {
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

  private SharePointIncrementalCheckpoint computeIncrementalCheckpoint()
      throws RepositoryException {
    return sharepointConfiguration.isSiteCollectionUrl()
        ? computeIncrementalCheckpointSiteCollection()
        : computeIncrementalCheckpointVirtualServer();
  }

  private SharePointIncrementalCheckpoint computeIncrementalCheckpointSiteCollection()
      throws RepositoryException {
    try {
      SiteConnector scConnector = getSiteConnectorForSiteCollectionOnly();
      Site site = scConnector.getSiteDataClient().getContentSite();
      return new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.SITE_COLLECTION)
          .addChangeToken(site.getMetadata().getID(), site.getMetadata().getChangeId())
          .build();
    } catch (IOException e) {
      throw buildRepositoryExceptionFromIOException(
          "error computing incremental checkpoint for SiteCollection", e);
    }
  }

  private SharePointIncrementalCheckpoint computeIncrementalCheckpointVirtualServer()
      throws RepositoryException {
    try {
      SiteConnector vsConnector = getSiteConnectorForVirtualServer();
      checkNotNull(vsConnector);
      VirtualServer vs = vsConnector.getSiteDataClient().getContentVirtualServer();
      SharePointIncrementalCheckpoint.Builder builder =
          new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.CONTENT_DB);
      for (ContentDatabases.ContentDatabase cdcd : vs.getContentDatabases().getContentDatabase()) {
        try {
          ContentDatabase cd =
              vsConnector.getSiteDataClient().getContentContentDatabase(cdcd.getID(), true);
          builder.addChangeToken(
              cd.getMetadata().getID(),
              cd.getMetadata().getChangeId());
        } catch (IOException ex) {
          log.log(Level.WARNING, "Failed to get content database: " + cdcd.getID(), ex);
          continue;
        }
      }
      return builder.build();
    } catch (IOException e) {
      throw buildRepositoryExceptionFromIOException(
          "error computing incremental checkpoint for virtualServer", e);
    }
  }

  private Collection<ApiOperation> getDocIdsVirtualServer() throws RepositoryException {
    try {
      List<ApiOperation> operations = new ArrayList<ApiOperation>();
      SharePointObject vsObject =
          new SharePointObject.Builder(SharePointObject.VIRTUAL_SERVER).build();
      PushItem pushItem = new PushItem().encodePayload(vsObject.encodePayload());
      operations.add(new PushItems.Builder().addPushItem(VIRTUAL_SERVER_ID, pushItem).build());
      SiteConnector vsConnector = getSiteConnectorForVirtualServer();
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
        }
      }
      return operations;
    } catch (IOException e) {
      throw buildRepositoryExceptionFromIOException("error listing Ids for VirtualServer", e);
    }
  }

  private SiteConnector getSiteConnectorForVirtualServer() throws IOException {
    return getSiteConnector(
        sharepointConfiguration.getVirtualServerUrl(),
        sharepointConfiguration.getVirtualServerUrl());
  }

  private Collection<ApiOperation> getDocIdsSiteCollectionOnly() throws RepositoryException {
    try {
      return Collections.singleton(getPushItemsForSiteCollectionOnly());
    } catch (IOException e) {
      throw buildRepositoryExceptionFromIOException("error listing Ids for SiteCollectionOnly", e);
    }
  }

  private PushItems getPushItemsForSiteCollectionOnly() throws IOException {
    SiteConnector scConnector = getSiteConnectorForSiteCollectionOnly();
    Site site = scConnector.getSiteDataClient().getContentSite();
    String siteCollectionUrl = getCanonicalUrl(site.getMetadata().getURL());
    SharePointObject siteCollection =
        new SharePointObject.Builder(SharePointObject.SITE_COLLECTION)
            .setUrl(siteCollectionUrl)
            .setObjectId(site.getMetadata().getID())
            .setSiteId(site.getMetadata().getID())
            .setWebId(site.getMetadata().getID())
            .build();
    PushItem pushEntry = new PushItem().encodePayload(siteCollection.encodePayload());
    return new PushItems.Builder().addPushItem(siteCollectionUrl, pushEntry).build();
  }

  private SiteConnector getSiteConnectorForSiteCollectionOnly() throws IOException {
    return getSiteConnector(
        sharepointConfiguration.getSharePointUrl().getUrl(),
        sharepointConfiguration.getSharePointUrl().getUrl());
  }

  private ApiOperation getVirtualServerDocContent() throws RepositoryException {
    try {
      SiteConnector vsConnector =
          getSiteConnector(
              sharepointConfiguration.getVirtualServerUrl(),
              sharepointConfiguration.getVirtualServerUrl());
      VirtualServer vs = vsConnector.getSiteDataClient().getContentVirtualServer();

      IndexingItemBuilder itemBuilder =
          new IndexingItemBuilder(VIRTUAL_SERVER_ID)
              .setAcl(vsConnector.getWebApplicationPolicyAcl(vs));
      RepositoryDoc.Builder docBuilder = new RepositoryDoc.Builder().setItem(itemBuilder.build());
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
              docBuilder.addChildId(
                  vsConnector.encodeDocId(siteUrl),
                  new PushItem().encodePayload(siteCollection.encodePayload()));
            }
          }
        } catch (IOException ex) {
          log.log(Level.WARNING, "Error retriving sites from content database " + cdcd.getID(), ex);
        }
      }
      return docBuilder.build();
    } catch (IOException e) {
      throw buildRepositoryExceptionFromIOException("error processing VirtualServerDoc", e);
    }
  }

  private static RepositoryException buildRepositoryExceptionFromIOException(
      String message, IOException e) {
    return new RepositoryException.Builder().setErrorMessage(message).setCause(e).build();
  }

  private ApiOperation getSiteCollectionDocContent(
      Item polledItem,
      SiteConnector scConnector,
      @SuppressWarnings("unused") SharePointObject siteCollection)
      throws IOException {
    List<ApiOperation> batchRequest = new ArrayList<ApiOperation>();
    Site site = scConnector.getSiteDataClient().getContentSite();
    Web rootWeb = scConnector.getSiteDataClient().getContentWeb();
    List<Principal> admins = scConnector.getSiteCollectionAdmins(rootWeb);
    Acl.Builder siteAdmins = new Acl.Builder().setReaders(admins);
    String siteAdminFragmentId =
        Acl.fragmentId(polledItem.getName(), SITE_COLLECTION_ADMIN_FRAGMENT);
    SharePointObject siteAdminObject =
        new SharePointObject.Builder(SharePointObject.NAMED_RESOURCE)
            .setSiteId(site.getMetadata().getID())
            .setObjectId(site.getMetadata().getID())
            .setUrl(siteAdminFragmentId)
            .build();
    if (!sharepointConfiguration.isSiteCollectionUrl()) {
      siteAdmins.setInheritFrom(VIRTUAL_SERVER_ID);
      siteAdmins.setInheritanceType(InheritanceType.PARENT_OVERRIDE);
    }
    Item adminFragmentItem =
        siteAdmins
            .build()
            .createFragmentItemOf(polledItem.getName(), SITE_COLLECTION_ADMIN_FRAGMENT)
            .encodePayload(siteAdminObject.encodePayload());
    RepositoryDoc adminFragment = new RepositoryDoc.Builder().setItem(adminFragmentItem).build();
    batchRequest.add(adminFragment);
    IndexingItemBuilder item = new IndexingItemBuilder(polledItem.getName());
    if (!sharepointConfiguration.isSiteCollectionUrl()) {
      item.setContainer(VIRTUAL_SERVER_ID);
    }
    Acl itemAcl =
        new Acl.Builder()
            .setReaders(scConnector.getWebAcls(rootWeb))
            .setInheritanceType(InheritanceType.PARENT_OVERRIDE)
            .setInheritFrom(siteAdminFragmentId)
            .build();
    item.setAcl(itemAcl);
    RepositoryDoc.Builder doc = new RepositoryDoc.Builder().setItem(item.build());
    addChildIdsToRepositoryDoc(
        doc, getChildWebEntries(scConnector, site.getMetadata().getID(), rootWeb));
    addChildIdsToRepositoryDoc(
        doc, getChildListEntries(scConnector, site.getMetadata().getID(), rootWeb));
    batchRequest.add(doc.build());
    return ApiOperations.batch(batchRequest.iterator());
  }

  private ApiOperation getWebDocContent(
      Item polledItem, SiteConnector scConnector, SharePointObject webObject) throws IOException {
    Web currentWeb = scConnector.getSiteDataClient().getContentWeb();
    String parentWebUrl = scConnector.getWebParentUrl();
    SiteConnector parentSiteConnector = getSiteConnector(scConnector.getSiteUrl(), parentWebUrl);
    Web parentWeb = parentSiteConnector.getSiteDataClient().getContentWeb();
    boolean inheritPermissions =
        Objects.equals(currentWeb.getMetadata().getScopeID(), parentWeb.getMetadata().getScopeID());
    IndexingItemBuilder item =
        new IndexingItemBuilder(polledItem.getName()).setContainer(parentWebUrl);
    Acl.Builder aclBuilder = new Acl.Builder().setInheritanceType(InheritanceType.PARENT_OVERRIDE);
    if (inheritPermissions) {
      aclBuilder.setInheritFrom(parentWebUrl);
    } else {
      aclBuilder.setReaders(scConnector.getWebAcls(currentWeb));
      aclBuilder.setInheritFrom(scConnector.getSiteUrl(), SITE_COLLECTION_ADMIN_FRAGMENT);
    }
    item.setAcl(aclBuilder.build());
    RepositoryDoc.Builder doc = new RepositoryDoc.Builder();
    addChildIdsToRepositoryDoc(
        doc, getChildWebEntries(scConnector, webObject.getSiteId(), currentWeb));
    addChildIdsToRepositoryDoc(
        doc, getChildListEntries(scConnector, webObject.getSiteId(), currentWeb));
    return doc.setItem(item.build()).build();
  }

  private ApiOperation getListDocContent(
      Item polledItem, SiteConnector scConnector, SharePointObject listObject) throws IOException {
    com.microsoft.schemas.sharepoint.soap.List l =
        scConnector.getSiteDataClient().getContentList(listObject.getListId());
    String rootFolder = l.getMetadata().getRootFolder();
    if (Strings.isNullOrEmpty(rootFolder)) {
      return ApiOperations.deleteItem(polledItem.getName());
    }

    String rootFolderDocId = scConnector.encodeDocId(rootFolder);
    SharePointObject rootFolderPayload =
        new SharePointObject.Builder(SharePointObject.NAMED_RESOURCE)
            .setSiteId(listObject.getSiteId())
            .setWebId(listObject.getWebId())
            .setUrl(rootFolderDocId)
            .setListId(listObject.getListId())
            .setObjectId(listObject.getListId())
            .build();
    Item rootFolderItem =
        new Item()
            .setName(rootFolderDocId)
            .setMetadata(new ItemMetadata().setContainerName(scConnector.getWebUrl()))
            .encodePayload(rootFolderPayload.encodePayload());

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
            .build();
    ItemMetadata metadata = new ItemMetadata();
    metadata.setContainerName(rootFolderDocId);
    Item listItem = new Item().setName(polledItem.getName());
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
    metadata.setSourceRepositoryUrl(displayUrl);
    String lastModified = l.getMetadata().getLastModified();
    try {
      metadata.setUpdateTime(
          new DateTime(listLastModifiedDateFormat.get().parse(lastModified)).toStringRfc3339());
    } catch (ParseException ex) {
      log.log(Level.INFO, "Could not parse LastModified: {0}", lastModified);
    }
    listItem.setMetadata(metadata);
    RepositoryDoc.Builder listDoc = new RepositoryDoc.Builder().setItem(listItem);
    addChildIdsToRepositoryDoc(
        listDoc, processFolder(scConnector, listObject.getListId(), "", listObject));
    List<ApiOperation> operations = Arrays.asList(listRootDoc, listDoc.build());
    return ApiOperations.batch(operations.iterator());
  }

  private ApiOperation getListItemDocContent(
      Item polledItem, SiteConnector scConnector, SharePointObject itemObject) throws IOException {
    Holder<String> listId = new Holder<String>();
    Holder<String> itemId = new Holder<String>();
    boolean result =
        scConnector.getSiteDataClient().getUrlSegments(polledItem.getName(), listId, itemId);
    if (!result || itemId.value == null || listId.value == null) {
      log.log(
          Level.WARNING,
          "Unable to identify itemId for Item {0}. Deleting item",
          polledItem.getName());
      return ApiOperations.deleteItem(polledItem.getName());
    }
    IndexingItemBuilder itemBuilder = new IndexingItemBuilder(polledItem.getName());
    ItemData i = scConnector.getSiteDataClient().getContentItem(listId.value, itemId.value);

    Xml xml = i.getXml();
    Element data = getFirstChildWithName(xml, DATA_ELEMENT);
    Element row = getChildrenWithName(data, ROW_ELEMENT).get(0);

    String modifiedString = row.getAttribute(OWS_MODIFIED_ATTRIBUTE);
    if (modifiedString == null) {
      log.log(Level.FINE, "No last modified information for list item");
    } else {
      try {
        itemBuilder.setLastModified(
            withValue(new DateTime(modifiedDateFormat.get().parse(modifiedString))));
      } catch (ParseException ex) {
        log.log(Level.INFO, "Could not parse ows_Modified: {0}", modifiedString);
      }
    }
    String createdString = row.getAttribute(OWS_CREATED_ATTRIBUTE);
    if (createdString == null) {
      log.log(Level.FINE, "No created time information for list item");
    } else {
      try {
        itemBuilder.setCreationTime(
            withValue(new DateTime(createdDateFormat.get().parse(createdString))));
      } catch (ParseException ex) {
        log.log(Level.INFO, "Could not parse ows_Created: {0}", createdString);
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
    itemBuilder.setContainer(folderDocId);
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
        throw new IOException("Unable to find permission scope for item: " + polledItem.getName());
      }
    }
    itemBuilder.setAcl(aclBuilder.build());
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
        itemBuilder.setUrl(withValue(displayUrl.toString()));
      } catch (URISyntaxException ex) {
        throw new IOException(ex);
      }
      RepositoryDoc.Builder doc = new RepositoryDoc.Builder();
      addChildIdsToRepositoryDoc(
          doc, processAttachments(scConnector, listId.value, itemId.value, row, itemObject));
      addChildIdsToRepositoryDoc(
          doc,
          processFolder(scConnector, listId.value, folder.substring(root.length()), itemObject));
      return doc.setItem(itemBuilder.build()).build();
    }
    String contentTypeId = row.getAttribute(OWS_CONTENTTYPEID_ATTRIBUTE);
    boolean isDocument =
        contentTypeId != null && contentTypeId.startsWith(CONTENTTYPEID_DOCUMENT_PREFIX);
    RepositoryDoc.Builder docBuilder = new RepositoryDoc.Builder();
    if (isDocument) {
      docBuilder.setContent(
          getFileContent(polledItem.getName(), itemBuilder, true), ContentFormat.RAW);
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
        itemBuilder.setUrl(withValue(viewItemUri.toString()));
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
      addChildIdsToRepositoryDoc(
          docBuilder, processAttachments(scConnector, listId.value, itemId.value, row, itemObject));
    }
    return docBuilder.setItem(itemBuilder.build()).build();
  }

  private static void addChildIdsToRepositoryDoc(
      RepositoryDoc.Builder docBuilder, Map<String, PushItem> entries) {
    entries.entrySet().stream().forEach(e -> docBuilder.addChildId(e.getKey(), e.getValue()));
  }

  private SharePointUrl buildSharePointUrl(String url) throws URISyntaxException {
    return new SharePointUrl.Builder(url)
        .setPerformBrowserLeniency(performBrowserLeniency)
        .build();
  }

  private ApiOperation getAttachmentDocContent(
      Item polledItem, SiteConnector scConnector, SharePointObject itemObject) throws IOException {
    Holder<String> listId = new Holder<String>();
    Holder<String> itemId = new Holder<String>();
    boolean result =
        scConnector.getSiteDataClient().getUrlSegments(itemObject.getItemId(), listId, itemId);
    if (!result || itemId.value == null || listId.value == null) {
      log.log(
          Level.WARNING,
          "Unable to identify itemId for Item {0}. Deleting item",
          polledItem.getName());
      return ApiOperations.deleteItem(polledItem.getName());
    }
    ItemData itemData = scConnector.getSiteDataClient().getContentItem(listId.value, itemId.value);
    Xml xml = itemData.getXml();
    Element data = getFirstChildWithName(xml, DATA_ELEMENT);
    assert data != null;
    String itemCount = data.getAttribute("ItemCount");
    if ("0".equals(itemCount)) {
      log.fine("Could not get parent list item as ItemCount is 0.");
      // Returning false here instead of returning 404 to avoid wrongly
      // identifying file documents as attachments when DocumentLibrary has
      // folder name Attachments. Returning false here would allow code
      // to see if this document was a regular file in DocumentLibrary.
      return ApiOperations.deleteItem(polledItem.getName());
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
      return ApiOperations.deleteItem(polledItem.getName());
    }
    IndexingItemBuilder itemBuilder = new IndexingItemBuilder(polledItem.getName());
    AbstractInputStreamContent content = getFileContent(polledItem.getName(), itemBuilder, false);
    Acl acl =
        new Acl.Builder()
            .setInheritanceType(InheritanceType.PARENT_OVERRIDE)
            .setInheritFrom(itemObject.getItemId())
            .build();
    itemBuilder.setAcl(acl);
    return new RepositoryDoc.Builder()
        .setItem(itemBuilder.build())
        .setContent(content, ContentFormat.RAW)
        .build();
  }

  private Map<String, PushItem> getChildListEntries(
      SiteConnector scConnector, String siteId, Web parentWeb) throws IOException {
    Map<String, PushItem> entries = new HashMap<>();
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
        entries.put(listUrl, new PushItem().encodePayload(payload.encodePayload()));
      }
    }
    return entries;
  }

  private Map<String, PushItem> getChildWebEntries(
      SiteConnector scConnector, String siteId, Web parentweb) throws IOException {
    Map<String, PushItem> entries = new HashMap<>();
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
        entries.put(childWebUrl, new PushItem().encodePayload(payload.encodePayload()));
      }
    }
    return entries;
  }

  private Map<String, PushItem> processFolder(
      SiteConnector scConnector, String listGuid, String folderPath, SharePointObject reference)
      throws IOException {
    Paginator<ItemData> folderPaginator =
        scConnector.getSiteDataClient().getContentFolderChildren(listGuid, folderPath);
    ItemData folder;
    Map<String, PushItem> entries = new HashMap<>();
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
        entries.put(itemId, new PushItem().encodePayload(payload.encodePayload()));
      }
    }
    return entries;
  }

  private Map<String, PushItem> processAttachments(
      SiteConnector scConnector,
      String listId,
      String itemId,
      Element row,
      SharePointObject reference)
      throws IOException {
    Map<String, PushItem> entries = new HashMap<>();
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
        entries.put(
            attachmentUrl, new PushItem().encodePayload(payloadBuilder.build().encodePayload()));
      }
    }
    return entries;
  }

  private AbstractInputStreamContent getFileContent(
      String fileUrl, IndexingItemBuilder item, boolean setLastModified) throws IOException {
    checkNotNull(item, "item can not be null");
    SharePointUrl sharepointFileUrl;
    try {
      sharepointFileUrl =
          buildSharePointUrl(fileUrl);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    item.setUrl(withValue(fileUrl));
    String filePath = sharepointFileUrl.getURI().getPath();
    String fileExtension = "";
    if (filePath.lastIndexOf('.') > 0) {
      fileExtension = filePath.substring(filePath.lastIndexOf('.')).toLowerCase(Locale.ENGLISH);
    }
    FileInfo fi = httpClient.issueGetRequest(sharepointFileUrl.toURL());
    String contentType;
    if (FILE_EXTENSION_TO_MIME_TYPE_MAPPING.containsKey(fileExtension)) {
      contentType = FILE_EXTENSION_TO_MIME_TYPE_MAPPING.get(fileExtension);
      log.log(
          Level.FINER,
          "Overriding content type as {0} for file extension {1}",
          new Object[] {contentType, fileExtension});
      item.setMimeType(contentType);
    } else {
      contentType = fi.getFirstHeaderWithName("Content-Type");
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
        item.setLastModified(
            withValue(new DateTime(dateFormatRfc1123.get().parse(lastModifiedString))));
      } catch (ParseException ex) {
        log.log(Level.INFO, "Could not parse Last-Modified: {0}", lastModifiedString);
      }
    }
    AbstractInputStreamContent content =
        new ByteArrayContent(contentType, ByteStreams.toByteArray(fi.getContents()));
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

  // Remove trailing slash from URLs as SharePoint doesn't like trailing slash
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
      // everything being in the clear).
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
