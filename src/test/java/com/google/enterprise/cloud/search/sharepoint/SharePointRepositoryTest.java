package com.google.enterprise.cloud.search.sharepoint;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.springboardindex.model.ExternalGroup;
import com.google.api.services.springboardindex.model.Item;
import com.google.api.services.springboardindex.model.Principal;
import com.google.api.services.springboardindex.model.PushEntry;
import com.google.api.services.springboardindex.model.QueueEntry;
import com.google.common.collect.ImmutableList;
import com.google.enterprise.adaptor.sharepoint.SiteDataClient;
import com.google.enterprise.adaptor.sharepoint.SiteDataClient.Paginator;
import com.google.enterprise.springboard.sdk.Acl;
import com.google.enterprise.springboard.sdk.Acl.InheritanceType;
import com.google.enterprise.springboard.sdk.Config;
import com.google.enterprise.springboard.sdk.InvalidConfigurationException;
import com.google.enterprise.springboard.sdk.template.ApiOperation;
import com.google.enterprise.springboard.sdk.template.ApiOperations;
import com.google.enterprise.springboard.sdk.template.RepositoryDoc;
import com.microsoft.schemas.sharepoint.soap.ContentDatabase;
import com.microsoft.schemas.sharepoint.soap.ItemData;
import com.microsoft.schemas.sharepoint.soap.ObjectType;
import com.microsoft.schemas.sharepoint.soap.Site;
import com.microsoft.schemas.sharepoint.soap.VirtualServer;
import com.microsoft.schemas.sharepoint.soap.Web;
import com.microsoft.schemas.sharepoint.soap.directory.UserGroupSoap;
import com.microsoft.schemas.sharepoint.soap.people.PeopleSoap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class SharePointRepositoryTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Mock HttpClientImpl.Builder httpClientBuilder;
  @Mock SiteConnectorFactoryImpl.Builder siteConnectorFactoryBuilder;
  @Mock HttpClientImpl httpClient;
  @Mock SiteConnectorFactoryImpl siteConnectorFactory;
  @Mock SiteConnector siteConnector;
  @Mock SiteDataClient siteDataClient;
  @Mock PeopleSoap peopleSoap;
  @Mock UserGroupSoap userGroupSoap;
  
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
  public void testInitInvalidSharePointUrl() {
    SharePointRepository repo =
        new SharePointRepository(httpClientBuilder, siteConnectorFactoryBuilder);
    Config config = new Config();
    config.register(repo);
    setMandatoryConfig(config);
    config.overrideKey("sharepoint.server", "abc");
    // config.freeze is calling init.
    thrown.expect(InvalidConfigurationException.class);
    config.freeze();
    verifyNoMoreInteractions(httpClientBuilder, siteConnectorFactoryBuilder);
  }

  @Test
  public void testInit() {
    setUpDefaultRepository();
    SharePointRequestContext requestContext =
        new SharePointRequestContext.Builder()
            .setAuthenticationHandler(null)
            .setReadTimeoutMillis(180000)
            .setSocketTimeoutMillis(30000)
            .setUserAgent("")
            .build();
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

  @Test
  public void testInitNonDefaultValues() {
    SharePointRepository repo =
        new SharePointRepository(httpClientBuilder, siteConnectorFactoryBuilder);
    Config config = new Config();
    config.register(repo);
    setMandatoryConfig(config);
    config.overrideKey("connector.lenientUrlRulesAndCustomRedirect", "false");
    when(httpClientBuilder.setPerformBrowserLeniency(false)).thenReturn(httpClientBuilder);
    config.overrideKey("sharepoint.xmlValidation", "true");
    when(siteConnectorFactoryBuilder.setXmlValidation(true))
        .thenReturn(siteConnectorFactoryBuilder);
    config.overrideKey("sharepoint.userAgent", "custom-user-agent");
    // config.freeze is calling init.
    config.freeze();
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
    SiteConnector scRoot =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    SiteDataClient siteCollectionDataClient = Mockito.mock(SiteDataClient.class);
    SiteConnector scManagedPath =
        new SiteConnector.Builder(
                "http://localhost:1/sites/SiteCollection",
                "http://localhost:1/sites/SiteCollection")
            .setSiteDataClient(siteCollectionDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1"))
        .thenReturn(scRoot);
    when(siteConnectorFactory.getInstance(
            "http://localhost:1/sites/SiteCollection", "http://localhost:1/sites/SiteCollection"))
        .thenReturn(scManagedPath);
    setupVirualServer();
    setupContentDB("{4fb7dea1-2912-4927-9eda-1ea2f0977cf8}");
    String rootSite =
        SharePointResponseHelper.getSiteCollectionResponse()
            .replaceAll("/sites/SiteCollection", "");
    setupSite(rootSite);
    String siteCollectionResponse = SharePointResponseHelper.getSiteCollectionResponse();
    Site siteCollectionSite = SiteDataClient.jaxbParse(siteCollectionResponse, Site.class, false);
    when(siteCollectionDataClient.getContentSite()).thenReturn(siteCollectionSite);
    List<ApiOperation> operations = new ArrayList<ApiOperation>();
    SharePointObject rootServerPayload =
        new SharePointObject.Builder(SharePointObject.VIRTUAL_SERVER).build();
    PushEntry rootEntry =
        new PushEntry()
            .setId(SharePointRepository.VIRTUAL_SERVER_ID)
            .encodePayload(rootServerPayload.encodePayload());
    operations.add(ApiOperations.pushEntries(Collections.singletonList(rootEntry)));
    operations.add(getTeamOwnersGroupOperation("http://localhost:1"));
    operations.add(getTeamMembersGroupOperation("http://localhost:1"));
    operations.add(getTeamVisitorsGroupOperation("http://localhost:1"));
    operations.add(getTeamOwnersGroupOperation("http://localhost:1/sites/SiteCollection"));
    operations.add(getTeamMembersGroupOperation("http://localhost:1/sites/SiteCollection"));
    operations.add(getTeamVisitorsGroupOperation("http://localhost:1/sites/SiteCollection"));
    Iterator<ApiOperation> actual = repo.getIds().iterator();
    compareIterartor(operations.iterator(), actual);
  }
  
  @Test
  public void testGetDocIdsSiteCollectionOnly() throws IOException {
    SharePointRepository repo =
        new SharePointRepository(httpClientBuilder, siteConnectorFactoryBuilder);
    Config config = new Config();
    config.register(repo);
    setMandatoryConfig(config);
    config.overrideKey("sharepoint.siteCollectionOnly", "true");
    config.freeze();
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
    List<ApiOperation> operations = new ArrayList<ApiOperation>();
    SharePointObject siteCollectionPayload =
        new SharePointObject.Builder(SharePointObject.SITE_COLLECTION)
            .setUrl("http://localhost:1")
            .setObjectId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .build();
    PushEntry rootEntry =
        new PushEntry()
            .setId("http://localhost:1")
            .encodePayload(siteCollectionPayload.encodePayload());
    operations.add(ApiOperations.pushEntries(Collections.singletonList(rootEntry)));
    operations.add(getTeamOwnersGroupOperation("http://localhost:1"));
    operations.add(getTeamMembersGroupOperation("http://localhost:1"));
    operations.add(getTeamVisitorsGroupOperation("http://localhost:1"));
    Iterator<ApiOperation> actual = repo.getIds().iterator();
    compareIterartor(operations.iterator(), actual);
  }
  
  @Test
  public void testGetVirtualServerDocContent() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
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
            .setReaders(Collections.singletonList(Acl.getExternalUserPrincipal("adminUser")))
            .build();
    when(scRoot.getWebApplicationPolicyAcl(vs)).thenReturn(policyAcl);
    when(scRoot.encodeDocId("http://localhost:1")).thenReturn("http://localhost:1");
    when(scRoot.encodeDocId("http://localhost:1/sites/SiteCollection"))
        .thenReturn("http://localhost:1/sites/SiteCollection");
    setupContentDB("{4fb7dea1-2912-4927-9eda-1ea2f0977cf8}");
    SharePointObject rootServerPayload =
        new SharePointObject.Builder(SharePointObject.VIRTUAL_SERVER).build();
    QueueEntry entry =
        new QueueEntry()
            .setId(SharePointRepository.VIRTUAL_SERVER_ID)
            .encodePayload(rootServerPayload.encodePayload());
    Item rootItem = new Item().setId(SharePointRepository.VIRTUAL_SERVER_ID);
    policyAcl.applyTo(rootItem);
    SharePointObject siteCollectionPayload =
        new SharePointObject.Builder(SharePointObject.SITE_COLLECTION)
            .setUrl("http://localhost:1")
            .setObjectId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .build();
    PushEntry rootEntry =
        new PushEntry()
            .setId("http://localhost:1")
            .encodePayload(siteCollectionPayload.encodePayload());
    SharePointObject siteCollectionPayloadManagdPath =
        new SharePointObject.Builder(SharePointObject.SITE_COLLECTION)
            .setUrl("http://localhost:1/sites/SiteCollection")
            .setObjectId("{5cbcd3b1-fca9-48b2-92db-3b5de26f837d}")
            .setSiteId("{5cbcd3b1-fca9-48b2-92db-3b5de26f837d}")
            .setWebId("{5cbcd3b1-fca9-48b2-92db-3b5de26f837d}")
            .build();
    PushEntry managePathEntry =
        new PushEntry()
            .setId("http://localhost:1/sites/SiteCollection")
            .encodePayload(siteCollectionPayloadManagdPath.encodePayload());
    RepositoryDoc expected =
        new RepositoryDoc.Builder()
            .setItem(rootItem)
            .setChildIds(Arrays.asList(rootEntry, managePathEntry))
            .build();
    ApiOperation actual = repo.getDoc(entry);
    assertEquals(expected, actual);
    verify(scRoot, times(2)).getSiteDataClient();
    verify(scRoot).getWebApplicationPolicyAcl(vs);
    verify(scRoot).encodeDocId("http://localhost:1");
    verify(scRoot).encodeDocId("http://localhost:1/sites/SiteCollection");
    verifyNoMoreInteractions(scRoot);
  }
  
  @Test
  public void testGetSiteCollectionDocContent() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
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
    QueueEntry entry =
        new QueueEntry()
            .setId("http://localhost:1")
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
            Acl.getExternalUserPrincipal(Acl.getPrincipalName("GDC-PSL\\administrator", "default")),
            Acl.getExternalUserPrincipal(Acl.getPrincipalName("GDC-PSL\\spuser1", "default")));
    Acl adminAcl =
        new Acl.Builder()
            .setReaders(admins)
            .setInheritanceType(InheritanceType.PARENT_OVERRIDE)
            .setInheritFrom(SharePointRepository.VIRTUAL_SERVER_ID)
            .build();
    List<PushEntry> childEntries = new ArrayList<PushEntry>();
    childEntries.add(
        new PushEntry().setId(siteAdminFragmentId).encodePayload(siteAdminObject.encodePayload()));
    childEntries.addAll(getChildEntriesForWeb("http://localhost:1"));
    RepositoryDoc expectedDoc =
        new RepositoryDoc.Builder()
            .setChildIds(childEntries)
            .setAclFragments(
                Collections.singletonMap(
                    SharePointRepository.SITE_COLLECTION_ADMIN_FRAGMENT, adminAcl))
            .setItem(
                getWebItem(
                    "http://localhost:1",
                    SharePointRepository.VIRTUAL_SERVER_ID,
                    siteAdminFragmentId,
                    false))
            .build();
    List<ApiOperation> operations = new ArrayList<ApiOperation>();
    operations.add(getTeamOwnersGroupOperation("http://localhost:1"));
    operations.add(getTeamMembersGroupOperation("http://localhost:1"));
    operations.add(getTeamVisitorsGroupOperation("http://localhost:1"));
    operations.add(expectedDoc);
    ApiOperation expected = ApiOperations.batch(operations.iterator());
    ApiOperation actual = repo.getDoc(entry);
    assertEquals(expected, actual);
  }
  
  @Test
  public void testGetWebDocContent() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
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
    QueueEntry entry =
        new QueueEntry()
            .setId("http://localhost:1/subsite")
            .encodePayload(webPayload.encodePayload());
    List<PushEntry> childEntries = new ArrayList<PushEntry>();
    childEntries.addAll(getChildEntriesForWeb("http://localhost:1/subsite"));
    RepositoryDoc expectedDoc =
        new RepositoryDoc.Builder()
            .setChildIds(childEntries)
            .setItem(
                getWebItem(
                    "http://localhost:1/subsite", "http://localhost:1", "http://localhost:1", true))
            .build();
    List<ApiOperation> operations = new ArrayList<ApiOperation>();
    operations.add(expectedDoc);
    ApiOperation actual = repo.getDoc(entry);
    assertEquals(expectedDoc, actual);
  }
  
  @Test
  public void testGetListDocContent() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
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
    SharePointObject listPayload =
        new SharePointObject.Builder(SharePointObject.LIST)
            .setUrl("http://localhost:1/Lists/Custom List/AllItems.aspx")
            .setObjectId("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setListId("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .build();
    QueueEntry entry =
        new QueueEntry()
            .setId("http://localhost:1/Lists/Custom List/AllItems.aspx")
            .encodePayload(listPayload.encodePayload());
    List<PushEntry> childEntries = new ArrayList<PushEntry>();
    childEntries.addAll(getChildEntriesForWeb("http://localhost:1/subsite"));
    RepositoryDoc expectedDoc =
        new RepositoryDoc.Builder()
            .setChildIds(childEntries)
            .setItem(
                getWebItem(
                    "http://localhost:1/subsite", "http://localhost:1", "http://localhost:1", true))
            .build();
    List<ApiOperation> operations = new ArrayList<ApiOperation>();
    operations.add(expectedDoc);
    repo.getDoc(entry);
  }

  private void setupGetSiteAndWeb(String url, String outputSite, String outputWeb, long result)
      throws IOException {
    doAnswer(
            new Answer<Long>() {

              @Override
              public Long answer(InvocationOnMock invocation) throws Throwable {
                Holder<String> site = invocation.getArgument(1);
                site.value = outputSite;
                Holder<String> web = invocation.getArgument(2);
                web.value = outputWeb;
                return result;
              }
            })
        .when(siteDataClient)
        .getSiteAndWeb(eq(url), any(), any());
  }

  private SharePointRepository setUpDefaultRepository() {
    SharePointRepository repo =
        new SharePointRepository(httpClientBuilder, siteConnectorFactoryBuilder);
    Config config = new Config();
    config.register(repo);
    setMandatoryConfig(config);
    config.freeze();
    return repo;
  }

  private void setMandatoryConfig(Config config) {
    config.overrideKey("sharepoint.server", "http://localhost:1");
    config.overrideKey("sharepoint.username", "user");
    config.overrideKey("sharepoint.password", "password");
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
  
  private void setupFolder(String xml, String listId, String folderUrl) {
    final AtomicBoolean executed = new AtomicBoolean();
    Paginator<ItemData> result =
        new Paginator<ItemData>() {
          @Override
          public ItemData next() throws IOException {
            if (executed.get()) {
              return null;
            }
            try {
              return SiteDataClient.jaxbParse(xml, ItemData.class, false);
            } finally {
              executed.set(true);
            }
          }
        };
    when(siteDataClient.getContentFolderChildren(listId, folderUrl)).thenReturn(result);
  }
  
  private ApiOperation getTeamOwnersGroupOperation(String siteUrl) {
    ExternalGroup group = getTeamOwnersGroup(siteUrl);
    return getLocalGroup(group);
  }
  
  private List<PushEntry> getChildEntriesForWeb(String webUrl) throws IOException {
    List<PushEntry> entries = new ArrayList<>();
    String childWebUrl = webUrl + "/somesite";
    SharePointObject payloadWeb =
        new SharePointObject.Builder(SharePointObject.WEB)
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{ee63e7d0-da23-4553-9f14-359f1cc1bf1c}")
            .setUrl(childWebUrl)
            .setObjectId("{ee63e7d0-da23-4553-9f14-359f1cc1bf1c}")
            .build();
    entries.add(new PushEntry().setId(childWebUrl).encodePayload(payloadWeb.encodePayload()));
    String annoucementUrl = webUrl + "/Lists/Announcements/AllItems.aspx";
    SharePointObject payloadAnnoucement =
        new SharePointObject.Builder(SharePointObject.LIST)
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{b2ea1067-3a54-4ab7-a459-c8ec864b97eb}")
            .setListId("{133fcb96-7e9b-46c9-b5f3-09770a35ad8a}")
            .setUrl(annoucementUrl)
            .setObjectId("{133fcb96-7e9b-46c9-b5f3-09770a35ad8a}")
            .build();
    entries.add(
        new PushEntry().setId(annoucementUrl).encodePayload(payloadAnnoucement.encodePayload()));
    String sharedDocsUrl = webUrl + "/Shared Documents/Forms/AllItems.aspx";
    SharePointObject payloadSharedDoc =
        new SharePointObject.Builder(SharePointObject.LIST)
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{b2ea1067-3a54-4ab7-a459-c8ec864b97eb}")
            .setListId("{648f6636-3d90-4565-86b9-2dd7611fc855}")
            .setUrl(sharedDocsUrl)
            .setObjectId("{648f6636-3d90-4565-86b9-2dd7611fc855}")
            .build();
    entries.add(
        new PushEntry().setId(sharedDocsUrl).encodePayload(payloadSharedDoc.encodePayload()));
    return entries;
  }
  
  private Item getWebItem(String url, String parent, String aclParent, boolean inherit) {
    Item item = new Item().setId(url).setContainer(parent);
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
                  Acl.getExternalUserPrincipal(Acl.getPrincipalName("GDC-PSL\\spuser1", "default")),
                  Acl.getExternalGroupPrincipal(
                      Acl.getPrincipalName("TeamSite Owners", "http://localhost:1")),
                  Acl.getExternalGroupPrincipal(
                      Acl.getPrincipalName("TeamSite Visitors", "http://localhost:1")),
                  Acl.getExternalGroupPrincipal(
                      Acl.getPrincipalName("TeamSite Members", "http://localhost:1"))))
          .build()
          .applyTo(item);
    }
    return item;
  }

  private ExternalGroup getTeamOwnersGroup(String siteUrl) {
    List<Principal> members =
        Collections.singletonList(
            Acl.getExternalUserPrincipal(
                Acl.getPrincipalName("GDC-PSL\\administrator", "default")));
    ExternalGroup group =
        new ExternalGroup()
            .setId(Acl.getPrincipalName("TeamSite Owners", siteUrl))
            .setMembers(members);
    return group;
  }

  private ApiOperation getTeamMembersGroupOperation(String siteUrl) {
    ExternalGroup group = getTeamMembersGroup(siteUrl);
    return getLocalGroup(group);
  }

  private ExternalGroup getTeamMembersGroup(String siteUrl) {
    List<Principal> members =
        new ImmutableList.Builder<Principal>()
            .add(Acl.getExternalUserPrincipal(Acl.getPrincipalName("GDC-PSL\\spuser2", "default")))
            .add(Acl.getExternalGroupPrincipal(Acl.getPrincipalName("BUILTIN\\users", "default")))
            .add(Acl.getExternalUserPrincipal(Acl.getPrincipalName("GDC-PSL\\spuser4", "default")))
            .build();
    ExternalGroup group =
        new ExternalGroup()
            .setId(Acl.getPrincipalName("TeamSite Members", siteUrl))
            .setMembers(members);
    return group;
  }
  
  private ExternalGroup getTeamVisitorsGroup(String siteUrl) {
    ExternalGroup group =
        new ExternalGroup()
            .setId(Acl.getPrincipalName("TeamSite Visitors", siteUrl))
            .setMembers(Collections.emptyList());
    return group;
  }
  
  private ApiOperation getTeamVisitorsGroupOperation(String siteUrl) {
    return getLocalGroup(getTeamVisitorsGroup(siteUrl));
  }
  
  private ApiOperation getLocalGroup(ExternalGroup group) {
    return ApiOperations.updateExternalGroup(group);
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
