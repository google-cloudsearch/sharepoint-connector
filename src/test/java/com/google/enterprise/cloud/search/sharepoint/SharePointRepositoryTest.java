package com.google.enterprise.cloud.search.sharepoint;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.util.DateTime;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemMetadata;
import com.google.api.services.cloudsearch.v1.model.Principal;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.enterprise.cloud.search.sharepoint.SharePointIncrementalCheckpoint.ChangeObjectType;
import com.google.enterprise.cloud.search.sharepoint.SiteDataClient.CursorPaginator;
import com.google.enterprise.cloud.search.sharepoint.SiteDataClient.Paginator;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl.InheritanceType;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.FieldOrValue;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ApiOperation;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ApiOperations;
import com.google.enterprise.cloudsearch.sdk.indexing.template.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.indexing.template.CheckpointCloseableIterableImpl;
import com.google.enterprise.cloudsearch.sdk.indexing.template.CheckpointCloseableIterableImpl.CompareCheckpointCloseableIterableRule;
import com.google.enterprise.cloudsearch.sdk.indexing.template.PushItems;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryContext;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryDoc;
import com.microsoft.schemas.sharepoint.soap.ContentDatabase;
import com.microsoft.schemas.sharepoint.soap.ItemData;
import com.microsoft.schemas.sharepoint.soap.SPContentDatabase;
import com.microsoft.schemas.sharepoint.soap.SPSite;
import com.microsoft.schemas.sharepoint.soap.Site;
import com.microsoft.schemas.sharepoint.soap.VirtualServer;
import com.microsoft.schemas.sharepoint.soap.Web;
import com.microsoft.schemas.sharepoint.soap.directory.UserGroupSoap;
import com.microsoft.schemas.sharepoint.soap.people.PeopleSoap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.xml.ws.Holder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SharePointRepositoryTest {
  private static final byte[] NULL_CHECKPOINT = null;
  private static final String XMLNS = "http://schemas.microsoft.com/sharepoint/soap/";
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Rule
  public CompareCheckpointCloseableIterableRule checkpointIterableRule =
      CompareCheckpointCloseableIterableRule.getCompareRule();

  @Mock HttpClientImpl.Builder httpClientBuilder;
  @Mock SiteConnectorFactoryImpl.Builder siteConnectorFactoryBuilder;
  @Mock HttpClientImpl httpClient;
  @Mock SiteConnectorFactoryImpl siteConnectorFactory;
  @Mock SiteConnector siteConnector;
  @Mock SiteDataClient siteDataClient;
  @Mock PeopleSoap peopleSoap;
  @Mock UserGroupSoap userGroupSoap;
  @Mock RepositoryContext repoContext;
  @Mock CursorPaginator<SPSite, String> siteChangesPaginator;
  @Mock CursorPaginator<SPContentDatabase, String> cdChangesPaginator;

  @Before
  public void setup() {
    when(httpClientBuilder.setSharePointRequestContext(any())).thenReturn(httpClientBuilder);
    when(httpClientBuilder.setMaxRedirectsAllowed(20)).thenReturn(httpClientBuilder);
    when(httpClientBuilder.setPerformBrowserLeniency(true)).thenReturn(httpClientBuilder);
    when(httpClientBuilder.build()).thenReturn(httpClient);
    when(siteConnectorFactoryBuilder.setRequestContext(any()))
        .thenReturn(siteConnectorFactoryBuilder);
    when(siteConnectorFactoryBuilder.setXmlValidation(false))
        .thenReturn(siteConnectorFactoryBuilder);
    when(siteConnectorFactoryBuilder.build()).thenReturn(siteConnectorFactory);
  }

  @Test
  public void testConstructor() {
    new SharePointRepository();
  }

  @Test
  public void testInitInvalidSharePointUrl() throws RepositoryException {
    SharePointRepository repo =
        new SharePointRepository(httpClientBuilder, siteConnectorFactoryBuilder);

    Properties baseConfig = getBaseConfig();
    baseConfig.put("sharepoint.server", "abc");
    overrideConfig(baseConfig);
    // config.freeze is calling init.
    thrown.expect(InvalidConfigurationException.class);
    repo.init(repoContext);
    verifyNoMoreInteractions(httpClientBuilder, siteConnectorFactoryBuilder);
  }

  @Test
  public void testInit() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
    SharePointRequestContext requestContext =
        new SharePointRequestContext.Builder()
            .setAuthenticationHandler(null)
            .setReadTimeoutMillis(180000)
            .setSocketTimeoutMillis(30000)
            .setUserAgent("")
            .build();
    repo.init(repoContext);
    InOrder inOrder = inOrder(httpClientBuilder, siteConnectorFactoryBuilder);
    inOrder.verify(httpClientBuilder).setSharePointRequestContext(requestContext);
    inOrder.verify(httpClientBuilder).setMaxRedirectsAllowed(20);
    inOrder.verify(httpClientBuilder).setPerformBrowserLeniency(true);
    inOrder.verify(httpClientBuilder).build();
    inOrder.verify(siteConnectorFactoryBuilder).setRequestContext(requestContext);
    inOrder.verify(siteConnectorFactoryBuilder).setXmlValidation(false);
    inOrder.verify(siteConnectorFactoryBuilder).build();
    verifyNoMoreInteractions(httpClientBuilder, siteConnectorFactoryBuilder);
  }

  private void setupVirtualServerForInit() throws IOException {
    SiteConnector scRoot =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1"))
        .thenReturn(scRoot);
    setupVirualServer();
    setupContentDB("{4fb7dea1-2912-4927-9eda-1ea2f0977cf8}");
  }

  @Test
  public void testInitNonDefaultValues() throws IOException {
    SharePointRepository repo =
        new SharePointRepository(httpClientBuilder, siteConnectorFactoryBuilder);

    Properties properties = getBaseConfig();
    properties.put("connector.lenientUrlRulesAndCustomRedirect", "false");
    when(httpClientBuilder.setPerformBrowserLeniency(false)).thenReturn(httpClientBuilder);
    properties.put("sharepoint.xmlValidation", "true");
    when(siteConnectorFactoryBuilder.setXmlValidation(true))
        .thenReturn(siteConnectorFactoryBuilder);
    properties.put("sharepoint.userAgent", "custom-user-agent");
    overrideConfig(properties);
    setupVirtualServerForInit();
    repo.init(repoContext);
    SharePointRequestContext requestContext =
        new SharePointRequestContext.Builder()
            .setAuthenticationHandler(null)
            .setReadTimeoutMillis(180000)
            .setSocketTimeoutMillis(30000)
            .setUserAgent("custom-user-agent")
            .build();
    InOrder inOrder = inOrder(httpClientBuilder, siteConnectorFactoryBuilder);
    inOrder.verify(httpClientBuilder).setSharePointRequestContext(requestContext);
    inOrder.verify(httpClientBuilder).setMaxRedirectsAllowed(20);
    inOrder.verify(httpClientBuilder).setPerformBrowserLeniency(false);
    inOrder.verify(httpClientBuilder).build();
    inOrder.verify(siteConnectorFactoryBuilder).setRequestContext(requestContext);
    inOrder.verify(siteConnectorFactoryBuilder).setXmlValidation(true);
    inOrder.verify(siteConnectorFactoryBuilder).build();
    verifyNoMoreInteractions(httpClientBuilder, siteConnectorFactoryBuilder);
  }

  @Test
  public void testGetDocIdsVirtualServer() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
    repo.init(repoContext);
    String rootSite =
        SharePointResponseHelper.getSiteCollectionResponse()
            .replaceAll("/sites/SiteCollection", "");
    setupSite(rootSite);
    List<ApiOperation> operations = new ArrayList<ApiOperation>();
    SharePointObject rootServerPayload =
        new SharePointObject.Builder(SharePointObject.VIRTUAL_SERVER).build();
    PushItems rootEntry =
        new PushItems.Builder()
            .addPushItem(
                SharePointRepository.VIRTUAL_SERVER_ID,
                new PushItem().encodePayload(rootServerPayload.encodePayload()))
            .build();
    operations.add(rootEntry);
    Iterator<ApiOperation> actual = repo.getIds(null).iterator();
    compareIterartor(operations.iterator(), actual);
  }

  @Test
  public void testGetDocIdsSiteCollectionOnly() throws IOException {
    SharePointRepository repo =
        new SharePointRepository(httpClientBuilder, siteConnectorFactoryBuilder);
    Properties properties = getBaseConfig();
    properties.put("sharepoint.siteCollectionOnly", "true");
    overrideConfig(properties);
    SiteConnector scRoot =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1"))
        .thenReturn(scRoot);
    String rootSite =
        SharePointResponseHelper.getSiteCollectionResponse()
            .replaceAll("/sites/SiteCollection", "");
    setupSite(rootSite);
    repo.init(repoContext);
    List<ApiOperation> operations = new ArrayList<ApiOperation>();
    SharePointObject siteCollectionPayload =
        new SharePointObject.Builder(SharePointObject.SITE_COLLECTION)
            .setUrl("http://localhost:1")
            .setObjectId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .build();
    PushItems rootEntry =
        new PushItems.Builder()
            .addPushItem(
                "http://localhost:1",
                new PushItem().encodePayload(siteCollectionPayload.encodePayload()))
            .build();
    operations.add(rootEntry);
    Iterator<ApiOperation> actual = repo.getIds(NULL_CHECKPOINT).iterator();
    compareIterartor(operations.iterator(), actual);
  }

  @Test
  public void testGetVirtualServerDocContent() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
    repo.init(repoContext);
    SiteConnector scRoot = Mockito.mock(SiteConnector.class);
    when(scRoot.getSiteDataClient()).thenReturn(siteDataClient);
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1"))
        .thenReturn(scRoot);
    VirtualServer vs =
        SiteDataClient.jaxbParse(
            SharePointResponseHelper.loadTestResponse("vs.xml"), VirtualServer.class, false);
    when(siteDataClient.getContentVirtualServer()).thenReturn(vs);
    Acl policyAcl =
        new Acl.Builder()
            .setReaders(Collections.singletonList(Acl.getUserPrincipal("adminUser")))
            .build();
    when(scRoot.getWebApplicationPolicyAcl(vs)).thenReturn(policyAcl);
    when(scRoot.encodeDocId("http://localhost:1")).thenReturn("http://localhost:1");
    when(scRoot.encodeDocId("http://localhost:1/sites/SiteCollection"))
        .thenReturn("http://localhost:1/sites/SiteCollection");
    setupContentDB("{4fb7dea1-2912-4927-9eda-1ea2f0977cf8}");
    SharePointObject rootServerPayload =
        new SharePointObject.Builder(SharePointObject.VIRTUAL_SERVER).build();
    Item entry =
        new Item()
            .setName(SharePointRepository.VIRTUAL_SERVER_ID)
            .encodePayload(rootServerPayload.encodePayload());
    Item rootItem =
        new IndexingItemBuilder(SharePointRepository.VIRTUAL_SERVER_ID).setAcl(policyAcl).build();
    SharePointObject siteCollectionPayload =
        new SharePointObject.Builder(SharePointObject.SITE_COLLECTION)
            .setUrl("http://localhost:1")
            .setObjectId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .build();
    Map<String, PushItem> entries = new HashMap<>();
    entries.put(
        "http://localhost:1", new PushItem().encodePayload(siteCollectionPayload.encodePayload()));

    SharePointObject siteCollectionPayloadManagdPath =
        new SharePointObject.Builder(SharePointObject.SITE_COLLECTION)
            .setUrl("http://localhost:1/sites/SiteCollection")
            .setObjectId("{5cbcd3b1-fca9-48b2-92db-3b5de26f837d}")
            .setSiteId("{5cbcd3b1-fca9-48b2-92db-3b5de26f837d}")
            .setWebId("{5cbcd3b1-fca9-48b2-92db-3b5de26f837d}")
            .build();
    entries.put(
        "http://localhost:1/sites/SiteCollection",
        new PushItem().encodePayload(siteCollectionPayloadManagdPath.encodePayload()));
    RepositoryDoc.Builder expected = new RepositoryDoc.Builder().setItem(rootItem);
    entries.entrySet().stream().forEach(e -> expected.addChildId(e.getKey(), e.getValue()));
    ApiOperation actual = repo.getDoc(entry);
    assertEquals(expected.build(), actual);
    verify(scRoot, times(2)).getSiteDataClient();
    verify(scRoot).getWebApplicationPolicyAcl(vs);
    verify(scRoot).encodeDocId("http://localhost:1");
    verify(scRoot).encodeDocId("http://localhost:1/sites/SiteCollection");
    verifyNoMoreInteractions(scRoot);
  }

  @Test
  public void testGetSiteCollectionDocContent() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
    repo.init(repoContext);
    SiteConnector scRoot =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1"))
        .thenReturn(scRoot);
    setupGetSiteAndWeb("http://localhost:1", "http://localhost:1", "http://localhost:1", 0);
    String rootSite =
        SharePointResponseHelper.getSiteCollectionResponse()
            .replaceAll("/sites/SiteCollection", "");
    setupSite(rootSite);
    String rootWeb =
        SharePointResponseHelper.getWebResponse().replaceAll("/sites/SiteCollection", "");
    setupWeb(rootWeb);
    SharePointObject siteCollectionPayload =
        new SharePointObject.Builder(SharePointObject.SITE_COLLECTION)
            .setUrl("http://localhost:1")
            .setObjectId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .build();
    Item entry =
        new Item()
            .setName("http://localhost:1")
            .encodePayload(siteCollectionPayload.encodePayload());
    String siteAdminFragmentId =
        Acl.fragmentId("http://localhost:1", SharePointRepository.SITE_COLLECTION_ADMIN_FRAGMENT);
    SharePointObject siteAdminObject =
        new SharePointObject.Builder(SharePointObject.NAMED_RESOURCE)
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setObjectId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setUrl(siteAdminFragmentId)
            .build();
    List<Principal> admins =
        Arrays.asList(
            Acl.getUserPrincipal(Acl.getPrincipalName("GDC-PSL\\administrator", "default")),
            Acl.getUserPrincipal(Acl.getPrincipalName("GDC-PSL\\spuser1", "default")));
    Acl adminAcl =
        new Acl.Builder()
            .setReaders(admins)
            .setInheritanceType(InheritanceType.PARENT_OVERRIDE)
            .setInheritFrom(SharePointRepository.VIRTUAL_SERVER_ID)
            .build();
    Item siteAdminFragment =
        adminAcl
            .createFragmentItemOf(
                "http://localhost:1", SharePointRepository.SITE_COLLECTION_ADMIN_FRAGMENT)
            .encodePayload(siteAdminObject.encodePayload());
    List<ApiOperation> operations = new ArrayList<ApiOperation>();
    operations.add(new RepositoryDoc.Builder().setItem(siteAdminFragment).build());
    Map<String, PushItem> childEntries = getChildEntriesForWeb("http://localhost:1");

    RepositoryDoc.Builder expectedDoc =
        new RepositoryDoc.Builder()
            .setItem(
                getWebItem(
                    "http://localhost:1",
                    SharePointRepository.VIRTUAL_SERVER_ID,
                    siteAdminFragmentId,
                    false));
    childEntries.entrySet().stream().forEach(e -> expectedDoc.addChildId(e.getKey(), e.getValue()));
    operations.add(expectedDoc.build());
    ApiOperation expected = ApiOperations.batch(operations.iterator());
    ApiOperation actual = repo.getDoc(entry);
    assertEquals(expected, actual);
  }

  @Test
  public void testGetWebDocContent() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
    repo.init(repoContext);
    SiteConnector scRoot =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1"))
        .thenReturn(scRoot);
    setupGetSiteAndWeb(
        "http://localhost:1/subsite", "http://localhost:1", "http://localhost:1/subsite", 0);
    SiteDataClient subSiteDataClient = Mockito.mock(SiteDataClient.class);
    SiteConnector scSubSite =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1/subsite")
            .setSiteDataClient(subSiteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1/subsite"))
        .thenReturn(scSubSite);

    String rootSite =
        SharePointResponseHelper.getSiteCollectionResponse()
            .replaceAll("/sites/SiteCollection", "");
    setupSite(rootSite);
    String rootWeb =
        SharePointResponseHelper.getWebResponse().replaceAll("/sites/SiteCollection", "");
    setupWeb(rootWeb);
    String currentWeb =
        SharePointResponseHelper.getWebResponse().replaceAll("/sites/SiteCollection", "/subsite");
    Web web = SiteDataClient.jaxbParse(currentWeb, Web.class, false);
    when(subSiteDataClient.getContentWeb()).thenReturn(web);
    SharePointObject webPayload =
        new SharePointObject.Builder(SharePointObject.WEB)
            .setUrl("http://localhost:1/subsite")
            .setObjectId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .build();
    Item entry =
        new Item().setName("http://localhost:1/subsite").encodePayload(webPayload.encodePayload());
    Map<String, PushItem> childEntries = getChildEntriesForWeb("http://localhost:1/subsite");
    RepositoryDoc.Builder expectedDoc =
        new RepositoryDoc.Builder()
            .setItem(
                getWebItem(
                    "http://localhost:1/subsite",
                    "http://localhost:1",
                    "http://localhost:1",
                    true));
    childEntries.entrySet().stream().forEach(e -> expectedDoc.addChildId(e.getKey(), e.getValue()));
    ApiOperation actual = repo.getDoc(entry);
    assertEquals(expectedDoc.build(), actual);
  }

  @Test
  public void testGetListDocContent() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
    repo.init(repoContext);
    SiteConnector scRoot =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1"))
        .thenReturn(scRoot);
    setupGetSiteAndWeb(
        "http://localhost:1/Lists/Custom List/AllItems.aspx",
        "http://localhost:1",
        "http://localhost:1",
        0);
    String rootSite =
        SharePointResponseHelper.getSiteCollectionResponse()
            .replaceAll("/sites/SiteCollection", "");
    setupSite(rootSite);
    String rootWeb =
        SharePointResponseHelper.getWebResponse().replaceAll("/sites/SiteCollection", "");
    setupWeb(rootWeb);
    String listResponse =
        SharePointResponseHelper.getListResponse()
            .replaceAll("/sites/SiteCollection", "")
            .replace(
                "ScopeID=\"{f9cb02b3-7f29-4cac-804f-ba6e14f1eb39}\"",
                "ScopeID=\"{01abac8c-66c8-4fed-829c-8dd02bbf40dd}\"");
    setupList(listResponse, "{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}");
    String listRootFolderResponse =
        SharePointResponseHelper.getListRootFolderContentResponse()
            .replaceAll("/sites/SiteCollection", "");
    setupFolder(listRootFolderResponse, "{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}", "");
    SharePointObject listRootPayload =
        new SharePointObject.Builder(SharePointObject.NAMED_RESOURCE)
            .setUrl("http://localhost:1/Lists/Custom List")
            .setObjectId("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setListId("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .build();
    SharePointObject listPayload =
        new SharePointObject.Builder(SharePointObject.LIST)
            .setUrl("http://localhost:1/Lists/Custom List/AllItems.aspx")
            .setObjectId("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setListId("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .build();

    Item entry =
        new Item()
            .setName("http://localhost:1/Lists/Custom List/AllItems.aspx")
            .encodePayload(listPayload.encodePayload());
    ItemMetadata metadata =
        new ItemMetadata()
            .setContainerName("http://localhost:1/Lists/Custom List")
            .setSourceRepositoryUrl("http://localhost:1/Lists/Custom List/AllItems.aspx")
            .setUpdateTime(null);
    entry.setMetadata(metadata);
    List<ApiOperation> expected = new ArrayList<>();

    Item rootItem =
        getWebItem(
                "http://localhost:1/Lists/Custom List",
                "http://localhost:1",
                "http://localhost:1",
                true)
            .encodePayload(listRootPayload.encodePayload());
    RepositoryDoc expectedListRootDoc =
        new RepositoryDoc.Builder()
            .setItem(rootItem)
            .build();
    expected.add(expectedListRootDoc);

    IndexingItemBuilder itemBuilder =
        new IndexingItemBuilder("http://localhost:1/Lists/Custom List/AllItems.aspx")
            .setAcl(
                new Acl.Builder()
                    .setInheritanceType(InheritanceType.PARENT_OVERRIDE)
                    .setInheritFrom("http://localhost:1/Lists/Custom List")
                    .build())
            .setUrl(FieldOrValue.withValue("http://localhost:1/Lists/Custom List/AllItems.aspx"))
            .setContainer("http://localhost:1/Lists/Custom List")
            .setLastModified(FieldOrValue.withValue(new DateTime("2012-05-04T14:24:32.000-07:00")));

    RepositoryDoc.Builder expectedDoc = new RepositoryDoc.Builder().setItem(itemBuilder.build());
    getChildEntriesForList("http://localhost:1/Lists/Custom List")
        .entrySet()
        .stream()
        .forEach(e -> expectedDoc.addChildId(e.getKey(), e.getValue()));
    expected.add(expectedDoc.build());
    ApiOperation actual = repo.getDoc(entry);
    assertEquals(ApiOperations.batch(expected.iterator()), actual);
  }

  @Test
  public void testGetListItemDocContent() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
    repo.init(repoContext);
    SiteConnector scRoot =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1"))
        .thenReturn(scRoot);
    setupGetSiteAndWeb(
        "http://localhost:1/Lists/Custom List/2_.000",
        "http://localhost:1",
        "http://localhost:1",
        0);
    String rootSite =
        SharePointResponseHelper.getSiteCollectionResponse()
            .replaceAll("/sites/SiteCollection", "");
    setupSite(rootSite);
    String rootWeb =
        SharePointResponseHelper.getWebResponse().replaceAll("/sites/SiteCollection", "");
    setupWeb(rootWeb);
    String listResponse =
        SharePointResponseHelper.getListResponse()
            .replaceAll("/sites/SiteCollection", "")
            .replace(
                "ScopeID=\"{f9cb02b3-7f29-4cac-804f-ba6e14f1eb39}\"",
                "ScopeID=\"{2e29615c-59e7-493b-b08a-3642949cc069}\"");
    setupList(listResponse, "{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}");
    SharePointObject payloadItem =
        new SharePointObject.Builder(SharePointObject.LIST_ITEM)
            .setListId("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setUrl("http://localhost:1/Lists/Custom List/2_.000")
            .setObjectId("item")
            .build();
    setupUrlSegments(
        "http://localhost:1/Lists/Custom List/2_.000",
        "{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}",
        "2");
    String listItemResponse = SharePointResponseHelper.getListItemResponse();
    listItemResponse =
        listItemResponse
            .replaceAll("/Test Folder", "")
            .replaceAll("/Test%20Folder", "")
            .replaceAll("/sites/SiteCollection", "")
            .replaceAll("sites/SiteCollection/", "")
            .replaceAll("ows_Attachments='1'", "ows_Attachments='0'");
    setupListItem(listItemResponse, "{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}", "2");

    Item entry =
        new Item()
            .setName("http://localhost:1/Lists/Custom List/2_.000")
            .encodePayload(payloadItem.encodePayload());
    IndexingItemBuilder itemBuilder =
        new IndexingItemBuilder("http://localhost:1/Lists/Custom List/2_.000")
            .setAcl(
                new Acl.Builder()
                    .setInheritanceType(InheritanceType.PARENT_OVERRIDE)
                    .setInheritFrom("http://localhost:1/Lists/Custom List")
                    .build())
            .setUrl(
                FieldOrValue.withValue("http://localhost:1/Lists/Custom%20List/DispForm.aspx?ID=2"))
            .setContainer("http://localhost:1/Lists/Custom List")
            .setLastModified(FieldOrValue.withValue(new DateTime("2012-05-04T14:24:32.000-07:00")))
            .setCreationTime(FieldOrValue.withValue(new DateTime("2012-05-01T15:14:06.000-07:00")));

    RepositoryDoc.Builder expectedDoc = new RepositoryDoc.Builder().setItem(itemBuilder.build());
    ApiOperation actual = repo.getDoc(entry);
    assertEquals(expectedDoc.build(), actual);
  }

  @Test
  public void testGetChangesNullCheckpointNoChangesSinceInitSiteCollectionOnly()
      throws IOException {
    SharePointRepository repo =
        new SharePointRepository(httpClientBuilder, siteConnectorFactoryBuilder);
    Properties properties = getBaseConfig();
    properties.put("sharepoint.siteCollectionOnly", "true");
    overrideConfig(properties);
    SiteConnector scRoot =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1"))
        .thenReturn(scRoot);
    String rootSite =
        SharePointResponseHelper.getSiteCollectionResponse()
            .replaceAll("/sites/SiteCollection", "");
    setupSite(rootSite);
    repo.init(repoContext);
    SharePointIncrementalCheckpoint checkpoint =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.SITE_COLLECTION)
            .addChangeToken(
                "{bb3bb2dd-6ea7-471b-a361-6fb67988755c}",
                "1;1;bb3bb2dd-6ea7-471b-a361-6fb67988755c;634762601982930000;726")
            .build();
    CheckpointCloseableIterableImpl expected =
        new CheckpointCloseableIterableImpl.Builder(Collections.emptyList())
            .setCheckpoint(checkpoint.encodePayload())
            .setHasMoreItems(false)
            .build();

    CheckpointCloseableIterable changes = repo.getChanges(null);
    assertTrue(checkpointIterableRule.compare(expected, changes));
  }

  @Test
  public void testGetChangesWithCheckpointNoChangesSinceInitSiteCollectionOnly()
      throws IOException {
    SharePointRepository repo =
        new SharePointRepository(httpClientBuilder, siteConnectorFactoryBuilder);
    Properties properties = getBaseConfig();
    properties.put("sharepoint.siteCollectionOnly", "true");
    overrideConfig(properties);
    SiteConnector scRoot =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1"))
        .thenReturn(scRoot);
    String rootSite =
        SharePointResponseHelper.getSiteCollectionResponse()
            .replaceAll("/sites/SiteCollection", "");
    setupSite(rootSite);
    repo.init(repoContext);
    SharePointIncrementalCheckpoint checkpoint =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.SITE_COLLECTION)
            .addChangeToken(
                "{bb3bb2dd-6ea7-471b-a361-6fb67988755c}",
                "1;1;bb3bb2dd-6ea7-471b-a361-6fb67988755c;634762601982930000;726")
            .build();
    CheckpointCloseableIterableImpl expected =
        new CheckpointCloseableIterableImpl.Builder(Collections.emptyList())
            .setCheckpoint(checkpoint.encodePayload())
            .build();

    CheckpointCloseableIterable changes = repo.getChanges(checkpoint.encodePayload());
    assertTrue(checkpointIterableRule.compare(expected, changes));
  }

  @Test
  public void testGetChangesInvalidCheckpointNoChangesSinceInitSiteCollectionOnly()
      throws IOException {
    SharePointRepository repo =
        new SharePointRepository(httpClientBuilder, siteConnectorFactoryBuilder);
    Properties properties = getBaseConfig();
    properties.put("sharepoint.siteCollectionOnly", "true");
    overrideConfig(properties);
    SiteConnector scRoot =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1"))
        .thenReturn(scRoot);
    String rootSite =
        SharePointResponseHelper.getSiteCollectionResponse()
            .replaceAll("/sites/SiteCollection", "");
    setupSite(rootSite);
    repo.init(repoContext);
    SharePointIncrementalCheckpoint checkpoint =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.SITE_COLLECTION)
            .addChangeToken(
                "{bb3bb2dd-6ea7-471b-a361-6fb67988755c}",
                "1;1;bb3bb2dd-6ea7-471b-a361-6fb67988755c;634762601982930000;726")
            .build();
    CheckpointCloseableIterableImpl expected =
        new CheckpointCloseableIterableImpl.Builder(Collections.emptyList())
            .setCheckpoint(checkpoint.encodePayload())
            .setHasMoreItems(false)
            .build();

    CheckpointCloseableIterable changes = repo.getChanges("invalid".getBytes());
    assertTrue(checkpointIterableRule.compare(expected, changes));
  }

  @Test
  public void testGetChangesSwitchToSiteCollectionOnlyModeNoChangesSinceInitSiteCollectionOnly()
      throws IOException {
    SharePointRepository repo =
        new SharePointRepository(httpClientBuilder, siteConnectorFactoryBuilder);
    Properties properties = getBaseConfig();
    properties.put("sharepoint.siteCollectionOnly", "true");
    overrideConfig(properties);
    SiteConnector scRoot =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1"))
        .thenReturn(scRoot);
    String rootSite =
        SharePointResponseHelper.getSiteCollectionResponse()
            .replaceAll("/sites/SiteCollection", "");
    setupSite(rootSite);
    repo.init(repoContext);
    SharePointIncrementalCheckpoint checkpointOld =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.CONTENT_DB)
            .addChangeToken(
                "{bb3bb2dd-6ea7-471b-a361-old}",
                "1;1;bb3bb2dd-6ea7-471b-a361-6fb67988755c;something")
            .build();
    SharePointIncrementalCheckpoint checkpoint =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.SITE_COLLECTION)
            .addChangeToken(
                "{bb3bb2dd-6ea7-471b-a361-6fb67988755c}",
                "1;1;bb3bb2dd-6ea7-471b-a361-6fb67988755c;634762601982930000;726")
            .build();
    CheckpointCloseableIterableImpl expected =
        new CheckpointCloseableIterableImpl.Builder(Collections.emptyList())
            .setCheckpoint(checkpoint.encodePayload())
            .build();

    CheckpointCloseableIterable changes = repo.getChanges(checkpointOld.encodePayload());
    assertTrue(checkpointIterableRule.compare(expected, changes));
  }

  @Test
  public void testGetChangesSwitchToAnotherSiteCollectionChangesSinceInitSiteCollectionOnly()
      throws IOException {
    SharePointRepository repo =
        new SharePointRepository(httpClientBuilder, siteConnectorFactoryBuilder);
    Properties properties = getBaseConfig();
    properties.put("sharepoint.siteCollectionOnly", "true");
    overrideConfig(properties);
    SiteConnector scRoot =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1"))
        .thenReturn(scRoot);
    String rootSite =
        SharePointResponseHelper.getSiteCollectionResponse()
            .replaceAll("/sites/SiteCollection", "");
    setupSite(rootSite);
    String changes726 =
        SharePointResponseHelper.getChangesForSiteCollection()
            .replace("<SPSite ", "<SPSite xmlns='" + XMLNS + "' ")
            .replaceAll("/sites/SiteCollection", "");
    when(siteChangesPaginator.next())
        .thenReturn(SiteDataClient.jaxbParse(changes726, SPSite.class, false))
        .thenReturn(null);
    when(siteChangesPaginator.getCursor())
        .thenReturn("1;1;bb3bb2dd-6ea7-471b-a361-6fb67988755c;634762601982930000;728");
    when(siteDataClient.getChangesSPSite(
            "{bb3bb2dd-6ea7-471b-a361-6fb67988755c}",
            "1;1;bb3bb2dd-6ea7-471b-a361-6fb67988755c;634762601982930000;726"))
        .thenReturn(siteChangesPaginator);
    repo.init(repoContext);
    SharePointIncrementalCheckpoint checkpointOld =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.SITE_COLLECTION)
            .addChangeToken(
                "{bb3bb2dd-6ea7-471b-a361-old}",
                "1;1;bb3bb2dd-6ea7-471b-a361-6fb67988755c;634762601982930000;726")
            .build();
    SharePointIncrementalCheckpoint checkpoint =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.SITE_COLLECTION)
            .addChangeToken(
                "{bb3bb2dd-6ea7-471b-a361-6fb67988755c}",
                "1;1;bb3bb2dd-6ea7-471b-a361-6fb67988755c;634762601982930000;728")
            .build();
    SharePointObject listItemObject =
        new SharePointObject.Builder(SharePointObject.LIST_ITEM)
            .setListId("{133fcb96-7e9b-46c9-b5f3-09770a35ad8a}")
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setUrl("http://localhost:1/Lists/Announcements/2_.000")
            .setObjectId("item")
            .build();
    CheckpointCloseableIterableImpl expected =
        new CheckpointCloseableIterableImpl.Builder(
                Collections.<ApiOperation>singleton(
                    new PushItems.Builder()
                        .addPushItem(
                            "http://localhost:1/Lists/Announcements/2_.000",
                            new PushItem()
                                .setType("MODIFIED")
                                .encodePayload(listItemObject.encodePayload()))
                        .build()))
            .setCheckpoint(checkpoint.encodePayload())
            .build();

    CheckpointCloseableIterable changes = repo.getChanges(checkpointOld.encodePayload());
    assertTrue(checkpointIterableRule.compare(expected, changes));
  }

  @Test
  public void testGetChangesSinceCheckpointSiteCollectionOnly() throws IOException {
    SharePointRepository repo =
        new SharePointRepository(httpClientBuilder, siteConnectorFactoryBuilder);
    Properties properties = getBaseConfig();
    properties.put("sharepoint.siteCollectionOnly", "true");
    overrideConfig(properties);
    SiteConnector scRoot =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1"))
        .thenReturn(scRoot);
    String rootSite =
        SharePointResponseHelper.getSiteCollectionResponse()
            .replaceAll("/sites/SiteCollection", "");
    setupSite(rootSite);
    String changes726 =
        SharePointResponseHelper.getChangesForSiteCollection()
            .replace("<SPSite ", "<SPSite xmlns='" + XMLNS + "' ")
            .replaceAll("/sites/SiteCollection", "");
    when(siteChangesPaginator.next())
        .thenReturn(SiteDataClient.jaxbParse(changes726, SPSite.class, false))
        .thenReturn(null);
    when(siteChangesPaginator.getCursor())
        .thenReturn("1;1;bb3bb2dd-6ea7-471b-a361-6fb67988755c;634762601982930000;728");
    when(siteDataClient.getChangesSPSite(
            "{bb3bb2dd-6ea7-471b-a361-6fb67988755c}",
            "1;1;bb3bb2dd-6ea7-471b-a361-6fb67988755c;634762601982930000;724"))
        .thenReturn(siteChangesPaginator);
    repo.init(repoContext);
    SharePointIncrementalCheckpoint checkpointOld =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.SITE_COLLECTION)
            .addChangeToken(
                "{bb3bb2dd-6ea7-471b-a361-6fb67988755c}",
                "1;1;bb3bb2dd-6ea7-471b-a361-6fb67988755c;634762601982930000;724")
            .build();
    SharePointIncrementalCheckpoint checkpoint =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.SITE_COLLECTION)
            .addChangeToken(
                "{bb3bb2dd-6ea7-471b-a361-6fb67988755c}",
                "1;1;bb3bb2dd-6ea7-471b-a361-6fb67988755c;634762601982930000;728")
            .build();
    SharePointObject listItemObject =
        new SharePointObject.Builder(SharePointObject.LIST_ITEM)
            .setListId("{133fcb96-7e9b-46c9-b5f3-09770a35ad8a}")
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setUrl("http://localhost:1/Lists/Announcements/2_.000")
            .setObjectId("item")
            .build();
    CheckpointCloseableIterableImpl expected =
        new CheckpointCloseableIterableImpl.Builder(
                Collections.<ApiOperation>singleton(
                    new PushItems.Builder()
                        .addPushItem(
                            "http://localhost:1/Lists/Announcements/2_.000",
                            new PushItem()
                                .setType("MODIFIED")
                                .encodePayload(listItemObject.encodePayload()))
                        .build()))
            .setCheckpoint(checkpoint.encodePayload())
            .build();

    CheckpointCloseableIterable changes = repo.getChanges(checkpointOld.encodePayload());
    assertTrue(checkpointIterableRule.compare(expected, changes));
  }

  @Test
  public void testGetChangesNullCheckpointNoChangesSinceInitVirtualServer() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
    SiteConnector scRoot =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1"))
        .thenReturn(scRoot);
    String rootSite =
        SharePointResponseHelper.getSiteCollectionResponse()
            .replaceAll("/sites/SiteCollection", "");
    setupSite(rootSite);
    repo.init(repoContext);
    SharePointIncrementalCheckpoint checkpoint =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.CONTENT_DB)
            .addChangeToken(
                "{4fb7dea1-2912-4927-9eda-1ea2f0977cf8}",
                "1;0;4fb7dea1-2912-4927-9eda-1ea2f0977cf8;634727056594000000;603")
            .build();
    CheckpointCloseableIterableImpl expected =
        new CheckpointCloseableIterableImpl.Builder(
                Collections.singleton(new PushItems.Builder().build()))
            .setCheckpoint(checkpoint.encodePayload())
            .build();

    CheckpointCloseableIterable changes = repo.getChanges(null);
    assertTrue(checkpointIterableRule.compare(expected, changes));
  }

  @Test
  public void testGetChangesCheckpointNoChangesSinceInitVirtualServer() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
    SiteConnector scRoot =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1"))
        .thenReturn(scRoot);
    String rootSite =
        SharePointResponseHelper.getSiteCollectionResponse()
            .replaceAll("/sites/SiteCollection", "");
    setupSite(rootSite);
    repo.init(repoContext);
    SharePointIncrementalCheckpoint checkpoint =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.CONTENT_DB)
            .addChangeToken(
                "{4fb7dea1-2912-4927-9eda-1ea2f0977cf8}",
                "1;0;4fb7dea1-2912-4927-9eda-1ea2f0977cf8;634727056594000000;603")
            .build();
    CheckpointCloseableIterableImpl expected =
        new CheckpointCloseableIterableImpl.Builder(
                Collections.<ApiOperation>singleton(new PushItems.Builder().build()))
            .setCheckpoint(checkpoint.encodePayload())
            .setHasMoreItems(false)
            .build();

    CheckpointCloseableIterable changes = repo.getChanges(checkpoint.encodePayload());
    assertTrue(checkpointIterableRule.compare(expected, changes));
  }

  @Test
  public void testGetChangesWithChangesCheckpointVirtualServer() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
    SiteConnector scRoot =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1"))
        .thenReturn(scRoot);
    String rootSite =
        SharePointResponseHelper.getSiteCollectionResponse()
            .replaceAll("/sites/SiteCollection", "");
    setupSite(rootSite);
    setupGetSiteAndWeb("http://localhost:1", "http://localhost:1", "http://localhost:1", 0);
    String changes726 =
        SharePointResponseHelper.getChangesForcontentDB()
            .replace("<SPContentDatabase ", "<SPContentDatabase xmlns='" + XMLNS + "' ")
            .replaceAll("/sites/SiteCollection", "");
    when(cdChangesPaginator.next())
        .thenReturn(SiteDataClient.jaxbParse(changes726, SPContentDatabase.class, false))
        .thenReturn(null);
    when(cdChangesPaginator.getCursor())
        .thenReturn("1;0;4fb7dea1-2912-4927-9eda-1ea2f0977cf8;634727056594000000;603");
    when(siteDataClient.getChangesContentDatabase(
            "{4fb7dea1-2912-4927-9eda-1ea2f0977cf8}",
            "1;0;4fb7dea1-2912-4927-9eda-1ea2f0977cf8;634727056594000000;600"))
        .thenReturn(cdChangesPaginator);

    repo.init(repoContext);
    SharePointIncrementalCheckpoint checkpointOld =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.CONTENT_DB)
            .addChangeToken(
                "{4fb7dea1-2912-4927-9eda-1ea2f0977cf8}",
                "1;0;4fb7dea1-2912-4927-9eda-1ea2f0977cf8;634727056594000000;600")
            .build();
    SharePointIncrementalCheckpoint checkpoint =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.CONTENT_DB)
            .addChangeToken(
                "{4fb7dea1-2912-4927-9eda-1ea2f0977cf8}",
                "1;0;4fb7dea1-2912-4927-9eda-1ea2f0977cf8;634727056594000000;603")
            .build();
    SharePointObject listItemObject =
        new SharePointObject.Builder(SharePointObject.LIST_ITEM)
            .setListId("{133fcb96-7e9b-46c9-b5f3-09770a35ad8a}")
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setUrl("http://localhost:1/Lists/Announcements/2_.000")
            .setObjectId("item")
            .build();
    CheckpointCloseableIterableImpl expected =
        new CheckpointCloseableIterableImpl.Builder(
                Collections.<ApiOperation>singleton(
                    new PushItems.Builder()
                        .addPushItem(
                            "http://localhost:1/Lists/Announcements/2_.000",
                            new PushItem()
                                .setType("MODIFIED")
                                .encodePayload(listItemObject.encodePayload()))
                        .build()))
            .setCheckpoint(checkpoint.encodePayload())
            .build();

    CheckpointCloseableIterable changes = repo.getChanges(checkpointOld.encodePayload());
    assertTrue(checkpointIterableRule.compare(expected, changes));
  }

  @Test
  public void testGetChangesRemovedContentDBCheckpointVirtualServer() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
    SiteConnector scRoot =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1"))
        .thenReturn(scRoot);
    String rootSite =
        SharePointResponseHelper.getSiteCollectionResponse()
            .replaceAll("/sites/SiteCollection", "");
    setupSite(rootSite);
    setupGetSiteAndWeb("http://localhost:1", "http://localhost:1", "http://localhost:1", 0);
    String changes726 =
        SharePointResponseHelper.getChangesForcontentDB()
            .replace("<SPContentDatabase ", "<SPContentDatabase xmlns='" + XMLNS + "' ")
            .replaceAll("/sites/SiteCollection", "");
    when(cdChangesPaginator.next())
        .thenReturn(SiteDataClient.jaxbParse(changes726, SPContentDatabase.class, false))
        .thenReturn(null);
    when(cdChangesPaginator.getCursor())
        .thenReturn("1;0;4fb7dea1-2912-4927-9eda-1ea2f0977cf8;634727056594000000;603");
    when(siteDataClient.getChangesContentDatabase(
            "{4fb7dea1-2912-4927-9eda-1ea2f0977cf8}",
            "1;0;4fb7dea1-2912-4927-9eda-1ea2f0977cf8;634727056594000000;600"))
        .thenReturn(cdChangesPaginator);

    repo.init(repoContext);
    SharePointIncrementalCheckpoint checkpointOld =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.CONTENT_DB)
            .addChangeToken(
                "{4fb7dea1-2912-4927-9eda-1ea2f0977cf8}",
                "1;0;4fb7dea1-2912-4927-9eda-1ea2f0977cf8;634727056594000000;600")
            .addChangeToken(
                "{4fb7dea1-2912-4927-9eda-1ea2f0977cf8-removed}",
                "1;0;4fb7dea1-2912-4927-9eda-1ea2f0977cf8;634727056594000000;removed")
            .build();
    SharePointIncrementalCheckpoint checkpoint =
        new SharePointIncrementalCheckpoint.Builder(ChangeObjectType.CONTENT_DB)
            .addChangeToken(
                "{4fb7dea1-2912-4927-9eda-1ea2f0977cf8}",
                "1;0;4fb7dea1-2912-4927-9eda-1ea2f0977cf8;634727056594000000;603")
            .build();
    SharePointObject listItemObject =
        new SharePointObject.Builder(SharePointObject.LIST_ITEM)
            .setListId("{133fcb96-7e9b-46c9-b5f3-09770a35ad8a}")
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setUrl("http://localhost:1/Lists/Announcements/2_.000")
            .setObjectId("item")
            .build();
    CheckpointCloseableIterableImpl expected =
        new CheckpointCloseableIterableImpl.Builder(
                Collections.<ApiOperation>singleton(
                    new PushItems.Builder()
                        .addPushItem(
                            "http://localhost:1/Lists/Announcements/2_.000",
                            new PushItem()
                                .setType("MODIFIED")
                                .encodePayload(listItemObject.encodePayload()))
                        .build()))
            .setCheckpoint(checkpoint.encodePayload())
            .build();

    CheckpointCloseableIterable changes = repo.getChanges(checkpointOld.encodePayload());
    assertTrue(checkpointIterableRule.compare(expected, changes));
  }

  private void setupGetSiteAndWeb(String url, String outputSite, String outputWeb, long result)
      throws IOException {
    doAnswer(
            invocation -> {
              Holder<String> site = invocation.getArgument(1);
              site.value = outputSite;
              Holder<String> web = invocation.getArgument(2);
              web.value = outputWeb;
              return result;
            })
        .when(siteDataClient)
        .getSiteAndWeb(eq(url), any(), any());
  }

  private SharePointRepository setUpDefaultRepository() throws IOException {
    SharePointRepository repo =
        new SharePointRepository(httpClientBuilder, siteConnectorFactoryBuilder);
    overrideConfig(getBaseConfig());
    setupVirtualServerForInit();
    return repo;
  }

  private Properties getBaseConfig() {
    Properties properties = new Properties();
    properties.put("sharepoint.server", "http://localhost:1");
    properties.put("sharepoint.username", "user");
    properties.put("sharepoint.password", "password");
    return properties;
  }

  private void overrideConfig(Properties properties) {
    setupConfig.initConfig(properties);
  }

  private void setupVirualServer() throws IOException {
    VirtualServer vs =
        SiteDataClient.jaxbParse(
            SharePointResponseHelper.loadTestResponse("vs.xml"), VirtualServer.class, false);
    when(siteDataClient.getContentVirtualServer()).thenReturn(vs);
  }

  private void setupContentDB(String id) throws IOException {
    ContentDatabase cd =
        SiteDataClient.jaxbParse(
            SharePointResponseHelper.loadTestResponse("cd.xml"), ContentDatabase.class, false);
    when(siteDataClient.getContentContentDatabase(id, true)).thenReturn(cd);
  }

  private void setupSite(String xml) throws IOException {
    Site site = SiteDataClient.jaxbParse(xml, Site.class, false);
    when(siteDataClient.getContentSite()).thenReturn(site);
  }

  private void setupWeb(String xml) throws IOException {
    Web site = SiteDataClient.jaxbParse(xml, Web.class, false);
    when(siteDataClient.getContentWeb()).thenReturn(site);
  }

  private void setupList(String xml, String listId) throws IOException {
    com.microsoft.schemas.sharepoint.soap.List list =
        SiteDataClient.jaxbParse(xml, com.microsoft.schemas.sharepoint.soap.List.class, false);
    when(siteDataClient.getContentList(listId)).thenReturn(list);
  }

  private void setupListItem(String xml, String listId, String itemId) throws IOException {
    xml = xml.replace("<Item>", "<ItemData xmlns='" + XMLNS + "'>");
    xml = xml.replace("</Item>", "</ItemData>");
    com.microsoft.schemas.sharepoint.soap.ItemData item =
        SiteDataClient.jaxbParse(xml, com.microsoft.schemas.sharepoint.soap.ItemData.class, false);
    when(siteDataClient.getContentItem(listId, itemId)).thenReturn(item);
  }

  @SuppressWarnings("unchecked")
  private void setupUrlSegments(String itemUrl, String listId, String itemId) throws IOException {
    doAnswer(
            invocation -> {
              ((Holder<String>) invocation.getArgument(1)).value = listId;
              ((Holder<String>) invocation.getArgument(2)).value = itemId;
              return true;
            })
        .when(siteDataClient)
        .getUrlSegments(eq(itemUrl), any(), any());
  }

  private void setupFolder(String xml, String listId, String folderUrl) {
    final AtomicBoolean executed = new AtomicBoolean();
    Paginator<ItemData> result =
        () -> {
      if (executed.get()) {
        return null;
      }
      try {
        return SiteDataClient.jaxbParse(xml, ItemData.class, false);
      } finally {
        executed.set(true);
      }
    };
    when(siteDataClient.getContentFolderChildren(listId, folderUrl)).thenReturn(result);
  }

  private Map<String, PushItem> getChildEntriesForWeb(String webUrl) throws IOException {
    Map<String, PushItem> entries = new HashMap<>();
    String childWebUrl = webUrl + "/somesite";
    SharePointObject payloadWeb =
        new SharePointObject.Builder(SharePointObject.WEB)
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{ee63e7d0-da23-4553-9f14-359f1cc1bf1c}")
            .setUrl(childWebUrl)
            .setObjectId("{ee63e7d0-da23-4553-9f14-359f1cc1bf1c}")
            .build();
    entries.put(childWebUrl, new PushItem().encodePayload(payloadWeb.encodePayload()));
    String annoucementUrl = webUrl + "/Lists/Announcements/AllItems.aspx";
    SharePointObject payloadAnnoucement =
        new SharePointObject.Builder(SharePointObject.LIST)
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{b2ea1067-3a54-4ab7-a459-c8ec864b97eb}")
            .setListId("{133fcb96-7e9b-46c9-b5f3-09770a35ad8a}")
            .setUrl(annoucementUrl)
            .setObjectId("{133fcb96-7e9b-46c9-b5f3-09770a35ad8a}")
            .build();
    entries.put(annoucementUrl, new PushItem().encodePayload(payloadAnnoucement.encodePayload()));
    String sharedDocsUrl = webUrl + "/Shared Documents/Forms/AllItems.aspx";
    SharePointObject payloadSharedDoc =
        new SharePointObject.Builder(SharePointObject.LIST)
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{b2ea1067-3a54-4ab7-a459-c8ec864b97eb}")
            .setListId("{648f6636-3d90-4565-86b9-2dd7611fc855}")
            .setUrl(sharedDocsUrl)
            .setObjectId("{648f6636-3d90-4565-86b9-2dd7611fc855}")
            .build();
    entries.put(sharedDocsUrl, new PushItem().encodePayload(payloadSharedDoc.encodePayload()));
    return entries;
  }

  private Map<String, PushItem> getChildEntriesForList(String listUrl) throws IOException {
    Map<String, PushItem> entries = new HashMap<>();
    SharePointObject payloadFolder =
        new SharePointObject.Builder(SharePointObject.LIST_ITEM)
            .setListId("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setUrl(listUrl + "/Test Folder")
            .setObjectId("item")
            .build();
    entries.put(
        listUrl + "/Test Folder", new PushItem().encodePayload(payloadFolder.encodePayload()));
    SharePointObject payloadItem =
        new SharePointObject.Builder(SharePointObject.LIST_ITEM)
            .setListId("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setUrl(listUrl + "/3_.000")
            .setObjectId("item")
            .build();
    entries.put(listUrl + "/3_.000", new PushItem().encodePayload(payloadItem.encodePayload()));
    return entries;
  }

  private Item getWebItem(String url, String parent, String aclParent, boolean inherit) {
    Item item = new Item().setName(url).setMetadata(new ItemMetadata().setContainerName(parent));
    if (inherit) {
      new Acl.Builder()
          .setInheritanceType(InheritanceType.PARENT_OVERRIDE)
          .setInheritFrom(aclParent)
          .build()
          .applyTo(item);
    } else {
      new Acl.Builder()
          .setInheritanceType(InheritanceType.PARENT_OVERRIDE)
          .setInheritFrom(aclParent)
          .setReaders(
              Arrays.asList(
                  Acl.getUserPrincipal(Acl.getPrincipalName("GDC-PSL\\spuser1", "default")),
                  Acl.getGroupPrincipal(
                      Acl.getPrincipalName("TeamSite Owners", "http://localhost:1")),
                  Acl.getGroupPrincipal(
                      Acl.getPrincipalName("TeamSite Visitors", "http://localhost:1")),
                  Acl.getGroupPrincipal(
                      Acl.getPrincipalName("TeamSite Members", "http://localhost:1"))))
          .build()
          .applyTo(item);
    }
    return item;
  }

  private void compareIterartor(Iterator<ApiOperation> expected, Iterator<ApiOperation> actual) {
    checkNotNull(expected);
    checkNotNull(actual);
    while (expected.hasNext() && actual.hasNext()) {
      ApiOperation expectedOp = expected.next();
      ApiOperation actualOp = actual.next();
      assertEquals(expectedOp, actualOp);
    }
    assertFalse("More elements expected", expected.hasNext());
    assertFalse("More elements present", actual.hasNext());
  }
}
