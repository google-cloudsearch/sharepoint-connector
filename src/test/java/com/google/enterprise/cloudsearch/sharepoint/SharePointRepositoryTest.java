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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.json.GenericJson;
import com.google.api.client.util.DateTime;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemMetadata;
import com.google.api.services.cloudsearch.v1.model.NamedProperty;
import com.google.api.services.cloudsearch.v1.model.ObjectDefinition;
import com.google.api.services.cloudsearch.v1.model.Principal;
import com.google.api.services.cloudsearch.v1.model.PropertyDefinition;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.api.services.cloudsearch.v1.model.RepositoryError;
import com.google.api.services.cloudsearch.v1.model.Schema;
import com.google.api.services.cloudsearch.v1.model.TextPropertyOptions;
import com.google.api.services.cloudsearch.v1.model.TextValues;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.io.ByteStreams;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterableImpl;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterableImpl.CompareCheckpointCloseableIterableRule;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.identity.IdentitySourceConfiguration;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl.InheritanceType;
import com.google.enterprise.cloudsearch.sdk.indexing.ContentTemplate;
import com.google.enterprise.cloudsearch.sdk.indexing.ContentTemplate.UnmappedColumnsMode;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.FieldOrValue;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.ContentFormat;
import com.google.enterprise.cloudsearch.sdk.indexing.StructuredData;
import com.google.enterprise.cloudsearch.sdk.indexing.StructuredData.ResetStructuredDataRule;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ApiOperation;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ApiOperations;
import com.google.enterprise.cloudsearch.sdk.indexing.template.PushItems;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryContext;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryDoc;
import com.google.enterprise.cloudsearch.sharepoint.SharePointConfiguration.SharePointDeploymentType;
import com.google.enterprise.cloudsearch.sharepoint.SharePointIncrementalCheckpoint.ChangeObjectType;
import com.google.enterprise.cloudsearch.sharepoint.SiteDataClient.CursorPaginator;
import com.google.enterprise.cloudsearch.sharepoint.SiteDataClient.Paginator;
import com.microsoft.schemas.sharepoint.soap.ContentDatabase;
import com.microsoft.schemas.sharepoint.soap.ItemData;
import com.microsoft.schemas.sharepoint.soap.SPContentDatabase;
import com.microsoft.schemas.sharepoint.soap.SPSite;
import com.microsoft.schemas.sharepoint.soap.Site;
import com.microsoft.schemas.sharepoint.soap.VirtualServer;
import com.microsoft.schemas.sharepoint.soap.Web;
import com.microsoft.schemas.sharepoint.soap.directory.UserGroupSoap;
import com.microsoft.schemas.sharepoint.soap.people.PeopleSoap;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;
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

/** Unit tests for {@link SharePointRepository} */
@RunWith(MockitoJUnitRunner.class)
public class SharePointRepositoryTest {
  private static final byte[] NULL_CHECKPOINT = null;
  private static final String XMLNS = "http://schemas.microsoft.com/sharepoint/soap/";
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();
  @Rule public ResetStructuredDataRule resetStructuredData = new ResetStructuredDataRule();

  @Rule
  public CompareCheckpointCloseableIterableRule<ApiOperation> checkpointIterableRule =
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
    when(siteConnectorFactoryBuilder.setActiveDirectoryClient(any()))
        .thenReturn(siteConnectorFactoryBuilder);
    when(siteConnectorFactoryBuilder.build()).thenReturn(siteConnectorFactory);
    when(siteConnectorFactoryBuilder.setReferenceIdentitySourceConfiguration(any()))
        .thenReturn(siteConnectorFactoryBuilder);
    when(siteConnectorFactoryBuilder.setStripDomainInUserPrincipals(false))
        .thenReturn(siteConnectorFactoryBuilder);
    when(siteConnectorFactoryBuilder.setSharePointDeploymentType(any()))
        .thenReturn(siteConnectorFactoryBuilder);
    PropertyDefinition author =
        new PropertyDefinition()
            .setName("CreatedBy")
            .setIsRepeatable(false)
            .setTextPropertyOptions(new TextPropertyOptions());
    PropertyDefinition multiValue =
        new PropertyDefinition()
            .setName("MultiValue")
            .setIsRepeatable(true)
            .setTextPropertyOptions(new TextPropertyOptions());
    ObjectDefinition objectDefinition =
        new ObjectDefinition()
            .setName("Item")
            .setPropertyDefinitions(ImmutableList.of(author, multiValue));
    ObjectDefinition objectDefinitionAnother =
        new ObjectDefinition()
            .setName("AnotherContentType")
            .setPropertyDefinitions(ImmutableList.of(author, multiValue));
    StructuredData.init(
        new Schema()
            .setObjectDefinitions(ImmutableList.of(objectDefinition, objectDefinitionAnother)));
  }

  @Test
  public void testConstructor() {
    new SharePointRepository();
  }

  @Test
  public void testInitInvalidSharePointUrl() throws RepositoryException {
    SharePointRepository repo = getSharePointRepository();

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
    inOrder.verify(siteConnectorFactoryBuilder).setActiveDirectoryClient(Optional.empty());
    inOrder.verify(siteConnectorFactoryBuilder).setReferenceIdentitySourceConfiguration(any());
    inOrder.verify(siteConnectorFactoryBuilder).setStripDomainInUserPrincipals(false);
    inOrder
        .verify(siteConnectorFactoryBuilder)
        .setSharePointDeploymentType(SharePointDeploymentType.ON_PREMISES);
    inOrder.verify(siteConnectorFactoryBuilder).build();
    verifyNoMoreInteractions(httpClientBuilder, siteConnectorFactoryBuilder);
  }

  @Test
  public void testInitAdfs() throws IOException {
    AuthenticationClientFactory actual = new AuthenticationClientFactoryImpl();
    AuthenticationClientFactory spyAuthenticationFactory = spy(actual);
    FormsAuthenticationHandler mockFormsAuthenticationHandler =
        mock(FormsAuthenticationHandler.class);
    doAnswer(
            invocation -> {
              // This is forcing configuration validation
              FormsAuthenticationHandler actualHandler =
                  actual.getFormsAuthenticationHandler(
                      invocation.getArgument(0),
                      invocation.getArgument(1),
                      invocation.getArgument(2),
                      invocation.getArgument(3));
              assertTrue(actualHandler instanceof SamlAuthenticationHandler);
              return mockFormsAuthenticationHandler;
            })
        .when(spyAuthenticationFactory)
        .getFormsAuthenticationHandler(eq("http://localhost:1"), eq("user"), eq("password"), any());
    SharePointRepository repo =
        new SharePointRepository(
            httpClientBuilder, siteConnectorFactoryBuilder, spyAuthenticationFactory);
    Properties baseConfig = getBaseConfig();
    baseConfig.put("sharepoint.formsAuthenticationMode", "ADFS");
    baseConfig.put("sharepoint.sts.endpoint", "https://stsendpoint");
    baseConfig.put("sharepoint.sts.realm", "upn");
    overrideConfig(baseConfig);
    setupVirtualServerForInit();

    SharePointRequestContext requestContext =
        new SharePointRequestContext.Builder()
            .setAuthenticationHandler(mockFormsAuthenticationHandler)
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
    inOrder.verify(siteConnectorFactoryBuilder).setActiveDirectoryClient(Optional.empty());
    inOrder.verify(siteConnectorFactoryBuilder).setReferenceIdentitySourceConfiguration(any());
    inOrder.verify(siteConnectorFactoryBuilder).setStripDomainInUserPrincipals(false);
    inOrder
        .verify(siteConnectorFactoryBuilder)
        .setSharePointDeploymentType(SharePointDeploymentType.ON_PREMISES);
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
    SharePointRepository repo = getSharePointRepository();

    Properties properties = getBaseConfig();
    properties.put("connector.lenientUrlRulesAndCustomRedirect", "false");
    when(httpClientBuilder.setPerformBrowserLeniency(false)).thenReturn(httpClientBuilder);
    properties.put("sharepoint.xmlValidation", "true");
    when(siteConnectorFactoryBuilder.setXmlValidation(true))
        .thenReturn(siteConnectorFactoryBuilder);
    properties.put("sharepoint.stripDomainInUserPrincipals", "true");
    when(siteConnectorFactoryBuilder.setStripDomainInUserPrincipals(true))
        .thenReturn(siteConnectorFactoryBuilder);
    properties.put("sharepoint.userAgent", "custom-user-agent");
    properties.put("sharepoint.deploymentType", "ONLINE");
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
    inOrder.verify(siteConnectorFactoryBuilder).setActiveDirectoryClient(Optional.empty());
    inOrder.verify(siteConnectorFactoryBuilder).setReferenceIdentitySourceConfiguration(any());
    inOrder.verify(siteConnectorFactoryBuilder).setStripDomainInUserPrincipals(true);
    inOrder
        .verify(siteConnectorFactoryBuilder)
        .setSharePointDeploymentType(SharePointDeploymentType.ONLINE);
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
    SharePointRepository repo = getSharePointRepository();
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
        new IndexingItemBuilder(SharePointRepository.VIRTUAL_SERVER_ID)
            .setAcl(policyAcl)
            .setItemType(ItemType.VIRTUAL_CONTAINER_ITEM)
            .setPayload(rootServerPayload.encodePayload())
            .build();
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
            .setReferenceIdentitySourceConfiguration(
                ImmutableMap.of(
                    "GDC-PSL", new IdentitySourceConfiguration.Builder("idSourceGdcPsl").build()))
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
            Acl.getUserPrincipal("GDC-PSL\\administrator", "idSourceGdcPsl"),
            Acl.getUserPrincipal("GDC-PSL\\spuser1", "idSourceGdcPsl"));
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

    Item webItem = getWebItem(
            "http://localhost:1",
            SharePointRepository.VIRTUAL_SERVER_ID,
            siteAdminFragmentId,
            false);
    webItem.getMetadata().setTitle("chinese1");
    webItem.encodePayload(siteCollectionPayload.encodePayload());
    RepositoryDoc.Builder expectedDoc = new RepositoryDoc.Builder().setItem(webItem);
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
    Item webItem = getWebItem(
            "http://localhost:1/subsite",
            "http://localhost:1",
            "http://localhost:1",
            true);
    webItem.getMetadata().setTitle("chinese1");
    webItem.encodePayload(webPayload.encodePayload());
    RepositoryDoc.Builder expectedDoc = new RepositoryDoc.Builder().setItem(webItem);
    childEntries.entrySet().stream().forEach(e -> expectedDoc.addChildId(e.getKey(), e.getValue()));
    ApiOperation actual = repo.getDoc(entry);
    assertEquals(expectedDoc.build(), actual);
  }

  @Test
  public void testGetWebDocContentNoIndex() throws IOException {
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
        SharePointResponseHelper.getWebResponse()
            .replaceAll("/sites/SiteCollection", "/subsite")
            .replace("NoIndex=\"False\"", "NoIndex=\"True\"");
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
    assertEquals(ApiOperations.deleteItem("http://localhost:1/subsite"), repo.getDoc(entry));
  }

  @Test
  public void testGetListDocContent() throws Exception {
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
            .setName("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .encodePayload(listPayload.encodePayload());

    IndexingItemBuilder itemBuilder =
        new IndexingItemBuilder("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .setAcl(
                new Acl.Builder()
                    .setInheritanceType(InheritanceType.PARENT_OVERRIDE)
                    .setInheritFrom("http://localhost:1")
                    .build())
            .setSourceRepositoryUrl(
                FieldOrValue.withValue("http://localhost:1/Lists/Custom List/AllItems.aspx"))
            .setContainerName("http://localhost:1")
            .setUpdateTime(
                FieldOrValue.withValue(
                    getParsedDateTime(
                        SharePointRepository.MODIFIED_DATE_LIST_FORMAT, "2012-05-04 21:24:32Z")))
            .setItemType(ItemType.CONTAINER_ITEM)
            .setTitle(FieldOrValue.withValue("Custom List"))
            .setPayload(listPayload.encodePayload());

    RepositoryDoc.Builder expectedDoc = new RepositoryDoc.Builder().setItem(itemBuilder.build());
    getChildEntriesForList("http://localhost:1/Lists/Custom List")
        .entrySet()
        .stream()
        .forEach(e -> expectedDoc.addChildId(e.getKey(), e.getValue()));
    RepositoryDoc actual = (RepositoryDoc) repo.getDoc(entry);
    assertEquals(expectedDoc.build(), actual);
  }

  @Test
  public void testGetListDocContentNoIndex() throws IOException {
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
            .replace("NoIndex=\"False\"", "NoIndex=\"True\"")
            .replace(
                "ScopeID=\"{f9cb02b3-7f29-4cac-804f-ba6e14f1eb39}\"",
                "ScopeID=\"{01abac8c-66c8-4fed-829c-8dd02bbf40dd}\"");
    setupList(listResponse, "{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}");
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
            .setName("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .encodePayload(listPayload.encodePayload());
    assertEquals(
        ApiOperations.deleteItem("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}"), repo.getDoc(entry));
  }

  @Test
  public void testGetListItemDocContent() throws Exception {
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
            .setName("{E7156244-AC2F-4402-AA74-7A365726CD02}")
            .encodePayload(payloadItem.encodePayload());
    IndexingItemBuilder itemBuilder =
        new IndexingItemBuilder("{E7156244-AC2F-4402-AA74-7A365726CD02}")
            .setAcl(
                new Acl.Builder()
                    .setInheritanceType(InheritanceType.PARENT_OVERRIDE)
                    .setInheritFrom("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
                    .build())
            .setSourceRepositoryUrl(
                FieldOrValue.withValue("http://localhost:1/Lists/Custom%20List/DispForm.aspx?ID=2"))
            .setContainerName("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .setUpdateTime(
                FieldOrValue.withValue(
                    getParsedDateTime(
                        SharePointRepository.MODIFIED_DATE_LIST_ITEM_FORMAT,
                        "2012-05-04T21:24:32Z")))
            .setCreateTime(
                FieldOrValue.withValue(
                    getParsedDateTime(
                        SharePointRepository.CREATED_DATE_LIST_ITEM_FORMAT,
                        "2012-05-01T22:14:06Z")))
            .setPayload(payloadItem.encodePayload())
            .setObjectType("Item")
            .setTitle(FieldOrValue.withValue("Inside Folder"))
            .setItemType(ItemType.CONTAINER_ITEM);

    Multimap<String, Object> values = ArrayListMultimap.create();
    values.put("Title", "Inside Folder");
    values.put("ContentType", "Item");
    values.put("Modified", "2012-05-04T21:24:32Z");
    values.put("Created", "2012-05-01T22:14:06Z");
    values.put("CreatedBy", "System Account");
    values.put("ModifiedBy", "System Account");
    values.putAll("MultiValue", Arrays.asList("alpha", "beta"));
    itemBuilder.setValues(values);

    RepositoryDoc.Builder expectedDoc = new RepositoryDoc.Builder().setItem(itemBuilder.build());
    ContentTemplate listItemContentTemplate =
        new ContentTemplate.Builder()
            .setTitle("Title")
            .setLowContent(
                Arrays.asList("Created", "CreatedBy", "ModifiedBy", "ContentType", "MultiValue"))
            .setUnmappedColumnMode(UnmappedColumnsMode.IGNORE)
            .build();

    String expectedContent = listItemContentTemplate.apply(values);
    expectedDoc.setContent(
        ByteArrayContent.fromString(null, expectedContent),
        ContentFormat.HTML);
    RepositoryDoc expected = expectedDoc.build();
    ApiOperation actual = repo.getDoc(entry);
    RepositoryDoc returnedDoc = (RepositoryDoc) actual;
    try (InputStream inputStream = returnedDoc.getContent().getInputStream()) {
      String actualContent = new String(ByteStreams.toByteArray(inputStream), UTF_8);
      assertEquals(expectedContent, actualContent);
    }
    assertEquals(expected.getItem(), returnedDoc.getItem());
    assertEquals(expected.getContentFormat(), returnedDoc.getContentFormat());
    assertEquals(expected.getRequestMode(), returnedDoc.getRequestMode());
    // Explicitly validating that values set with structured data are populated via
    // IndexingItemBuilder.
    assertThat(
        returnedDoc.getItem().getStructuredData().getObject().getProperties(),
        hasItem(
            new NamedProperty()
                .setName("CreatedBy")
                .setTextValues(new TextValues().setValues(ImmutableList.of("System Account")))));
    assertThat(
        returnedDoc.getItem().getStructuredData().getObject().getProperties(),
        hasItem(
            new NamedProperty()
                .setName("MultiValue")
                .setTextValues(new TextValues().setValues(ImmutableList.of("alpha", "beta")))));
  }

  @Test
  public void testGetListItemDocContentFolder() throws Exception {
    Properties properties = getBaseConfig();
    properties.put(
        "contentTemplate.sharepointItem.quality.low",
        "Created,CreatedBy,ModifiedBy,ContentType,MultiValue,ItemType");

    SharePointRepository repo = getSharePointRepository();
    overrideConfig(properties);
    setupVirtualServerForInit();

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
        "http://localhost:1/Lists/Custom List/Sub=Folder",
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
            .setUrl("http://localhost:1/Lists/Custom List/Sub=Folder")
            .setObjectId("item")
            .build();
    setupUrlSegments(
        "http://localhost:1/Lists/Custom List/Sub=Folder",
        "{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}",
        "2");
    String listItemResponse = SharePointResponseHelper.getListItemResponse();
    listItemResponse =
        listItemResponse
            .replaceAll("/Test Folder", "")
            .replaceAll("/Test%20Folder", "")
            .replaceAll("/sites/SiteCollection", "")
            .replaceAll("sites/SiteCollection/", "")
            .replaceAll("ows_Attachments='1'", "ows_Attachments='0'")
            .replaceAll("ows_FSObjType='2;#0'", "ows_FSObjType='2;#1'")
            .replaceAll("/2_.000", "/Sub=Folder");
    setupListItem(listItemResponse, "{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}", "2");
    String listRootFolderResponse =
        SharePointResponseHelper.getListRootFolderContentResponse()
            .replaceAll("/sites/SiteCollection", "");
    setupFolder(listRootFolderResponse, "{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}", "Sub=Folder");

    Item entry =
        new Item()
            .setName("{E7156244-AC2F-4402-AA74-7A365726CD02}")
            .encodePayload(payloadItem.encodePayload());
    IndexingItemBuilder itemBuilder =
        new IndexingItemBuilder("{E7156244-AC2F-4402-AA74-7A365726CD02}")
            .setAcl(
                new Acl.Builder()
                    .setInheritanceType(InheritanceType.PARENT_OVERRIDE)
                    .setInheritFrom("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
                    .build())
            .setSourceRepositoryUrl(
                FieldOrValue.withValue(
                    "http://localhost:1/Lists/Custom%20List/AllItems.aspx?"
                        + "RootFolder=/Lists/Custom%20List/Sub=Folder"))
            .setContainerName("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .setUpdateTime(
                FieldOrValue.withValue(
                    getParsedDateTime(
                        SharePointRepository.MODIFIED_DATE_LIST_ITEM_FORMAT,
                        "2012-05-04T21:24:32Z")))
            .setCreateTime(
                FieldOrValue.withValue(
                    getParsedDateTime(
                        SharePointRepository.CREATED_DATE_LIST_ITEM_FORMAT,
                        "2012-05-01T22:14:06Z")))
            .setPayload(payloadItem.encodePayload())
            .setObjectType("Item")
            .setTitle(FieldOrValue.withValue("Inside Folder"))
            .setItemType(ItemType.CONTAINER_ITEM);

    Multimap<String, Object> values = ArrayListMultimap.create();
    values.put("Title", "Inside Folder");
    values.put("ContentType", "Item");
    values.put("Modified", "2012-05-04T21:24:32Z");
    values.put("Created", "2012-05-01T22:14:06Z");
    values.put("CreatedBy", "System Account");
    values.put("ModifiedBy", "System Account");
    values.put("ItemType", "1"); // FSObjType maps to display name ItemType
    values.putAll("MultiValue", Arrays.asList("alpha", "beta"));
    itemBuilder.setValues(values);

    RepositoryDoc.Builder expectedDoc = new RepositoryDoc.Builder().setItem(itemBuilder.build());
    ContentTemplate listItemContentTemplate =
        new ContentTemplate.Builder()
            .setTitle("Title")
            .setLowContent(
                Arrays.asList(
                    "Created", "CreatedBy", "ModifiedBy", "ContentType", "MultiValue", "ItemType"))
            .setUnmappedColumnMode(UnmappedColumnsMode.IGNORE)
            .build();

    String expectedContent = listItemContentTemplate.apply(values);
    expectedDoc.setContent(ByteArrayContent.fromString(null, expectedContent), ContentFormat.HTML);
    RepositoryDoc expected = expectedDoc.build();
    ApiOperation actual = repo.getDoc(entry);
    RepositoryDoc returnedDoc = (RepositoryDoc) actual;
    try (InputStream inputStream = returnedDoc.getContent().getInputStream()) {
      String actualContent = new String(ByteStreams.toByteArray(inputStream), UTF_8);
      assertEquals(expectedContent, actualContent);
    }
    assertEquals(expected.getItem(), returnedDoc.getItem());
    assertEquals(expected.getContentFormat(), returnedDoc.getContentFormat());
    assertEquals(expected.getRequestMode(), returnedDoc.getRequestMode());
    // Explicitly validating that values set with structured data are populated via
    // IndexingItemBuilder.
    assertThat(
        returnedDoc.getItem().getStructuredData().getObject().getProperties(),
        hasItem(
            new NamedProperty()
                .setName("CreatedBy")
                .setTextValues(new TextValues().setValues(ImmutableList.of("System Account")))));
    assertThat(
        returnedDoc.getItem().getStructuredData().getObject().getProperties(),
        hasItem(
            new NamedProperty()
                .setName("MultiValue")
                .setTextValues(new TextValues().setValues(ImmutableList.of("alpha", "beta")))));
  }


  @Test
  public void testGetListItemDocContentEmptyTitle() throws Exception {
    Properties baseConfig = getBaseConfig();
    baseConfig.put("itemMetadata.title.field", "Name");
    overrideConfig(baseConfig);
    SharePointRepository repo = getSharePointRepository();
    setupVirtualServerForInit();
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
            .replaceAll("ows_Attachments='1'", "ows_Attachments='0'")
            .replaceAll("Inside Folder", ""); // clear out Title value
    setupListItem(listItemResponse, "{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}", "2");

    Item entry =
        new Item()
            .setName("{E7156244-AC2F-4402-AA74-7A365726CD02}")
            .encodePayload(payloadItem.encodePayload());
    IndexingItemBuilder itemBuilder =
        new IndexingItemBuilder("{E7156244-AC2F-4402-AA74-7A365726CD02}")
            .setAcl(
                new Acl.Builder()
                    .setInheritanceType(InheritanceType.PARENT_OVERRIDE)
                    .setInheritFrom("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
                    .build())
            .setSourceRepositoryUrl(
                FieldOrValue.withValue("http://localhost:1/Lists/Custom%20List/DispForm.aspx?ID=2"))
            .setContainerName("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .setUpdateTime(
                FieldOrValue.withValue(
                    getParsedDateTime(
                        SharePointRepository.MODIFIED_DATE_LIST_ITEM_FORMAT,
                        "2012-05-04T21:24:32Z")))
            .setCreateTime(
                FieldOrValue.withValue(
                    getParsedDateTime(
                        SharePointRepository.CREATED_DATE_LIST_ITEM_FORMAT,
                        "2012-05-01T22:14:06Z")))
            .setPayload(payloadItem.encodePayload())
            .setObjectType("Item")
            .setTitle(FieldOrValue.withValue("2_.000")) // ows_FileLeafRef has display name "Name"
            .setItemType(ItemType.CONTAINER_ITEM);

    Multimap<String, Object> values = ArrayListMultimap.create();
    values.put("Title", "");
    values.put("ContentType", "Item");
    values.put("Modified", "2012-05-04T21:24:32Z");
    values.put("Created", "2012-05-01T22:14:06Z");
    values.put("CreatedBy", "System Account");
    values.put("ModifiedBy", "System Account");
    values.putAll("MultiValue", Arrays.asList("alpha", "beta"));
    itemBuilder.setValues(values);

    RepositoryDoc.Builder expectedDoc = new RepositoryDoc.Builder().setItem(itemBuilder.build());
    ContentTemplate listItemContentTemplate =
        new ContentTemplate.Builder()
            .setTitle("Title")
            .setLowContent(
                Arrays.asList("Created", "CreatedBy", "ModifiedBy", "ContentType", "MultiValue"))
            .setUnmappedColumnMode(UnmappedColumnsMode.IGNORE)
            .build();

    String expectedContent = listItemContentTemplate.apply(values);
    expectedDoc.setContent(ByteArrayContent.fromString(null, expectedContent), ContentFormat.HTML);
    RepositoryDoc expected = expectedDoc.build();
    ApiOperation actual = repo.getDoc(entry);
    RepositoryDoc returnedDoc = (RepositoryDoc) actual;
    try (InputStream inputStream = returnedDoc.getContent().getInputStream()) {
      String actualContent = new String(ByteStreams.toByteArray(inputStream), UTF_8);
      assertEquals(expectedContent, actualContent);
    }
    assertEquals(expected.getItem(), returnedDoc.getItem());
    assertEquals(expected.getContentFormat(), returnedDoc.getContentFormat());
    assertEquals(expected.getRequestMode(), returnedDoc.getRequestMode());
    // Explicitly validating that values set with structured data are populated via
    // IndexingItemBuilder.
    assertThat(
        returnedDoc.getItem().getStructuredData().getObject().getProperties(),
        hasItem(
            new NamedProperty()
                .setName("CreatedBy")
                .setTextValues(new TextValues().setValues(ImmutableList.of("System Account")))));
    assertThat(
        returnedDoc.getItem().getStructuredData().getObject().getProperties(),
        hasItem(
            new NamedProperty()
                .setName("MultiValue")
                .setTextValues(new TextValues().setValues(ImmutableList.of("alpha", "beta")))));
  }

  @Test
  public void testGetListItemDocContentNoIndex() throws IOException {
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
    String listResponse =
        SharePointResponseHelper.getListResponse()
            .replaceAll("/sites/SiteCollection", "")
            .replace("NoIndex=\"False\"", "NoIndex=\"True\"")
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

    Item entry =
        new Item()
            .setName("{E7156244-AC2F-4402-AA74-7A365726CD02}")
            .encodePayload(payloadItem.encodePayload());

    assertEquals(
        ApiOperations.deleteItem("{E7156244-AC2F-4402-AA74-7A365726CD02}"), repo.getDoc(entry));
  }

  @Test
  public void testGetAttachmentDocContent() throws IOException {
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
        "http://localhost:1/Lists/Custom List/Attachments/2/attach.pdf",
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
        new SharePointObject.Builder(SharePointObject.ATTACHMENT)
            .setListId("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setItemId("http://localhost:1/Lists/Custom List/2_.000")
            .setObjectId("http://localhost:1/Lists/Custom List/Attachments/2/attach.pdf")
            .setUrl("http://localhost:1/Lists/Custom List/Attachments/2/attach.pdf")
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
            .replaceAll("sites/SiteCollection/", "");
    setupListItem(listItemResponse, "{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}", "2");
    InputStream attachmentContent = new ByteArrayInputStream("attachment".getBytes());
    when(httpClient.issueGetRequest(
            new URL("http://localhost:1/Lists/Custom%20List/Attachments/2/attach.pdf")))
        .thenReturn(new FileInfo.Builder(attachmentContent).build());

    Item entry =
        new Item()
            .setName("http://localhost:1/Lists/Custom List/Attachments/2/attach.pdf")
            .encodePayload(payloadItem.encodePayload());
    IndexingItemBuilder itemBuilder =
        new IndexingItemBuilder("http://localhost:1/Lists/Custom List/Attachments/2/attach.pdf")
            .setAcl(
                new Acl.Builder()
                    .setInheritanceType(InheritanceType.PARENT_OVERRIDE)
                    .setInheritFrom("{E7156244-AC2F-4402-AA74-7A365726CD02}")
                    .build())
            .setSourceRepositoryUrl(
                FieldOrValue.withValue(
                    "http://localhost:1/Lists/Custom List/Attachments/2/attach.pdf"))
            .setContainerName("{E7156244-AC2F-4402-AA74-7A365726CD02}")
            .setPayload(payloadItem.encodePayload())
            .setItemType(ItemType.CONTENT_ITEM);

    RepositoryDoc.Builder expectedDoc = new RepositoryDoc.Builder().setItem(itemBuilder.build());
    expectedDoc.setContent(ByteArrayContent.fromString(null, "attachment"), ContentFormat.RAW);
    RepositoryDoc expected = expectedDoc.build();
    ApiOperation actual = repo.getDoc(entry);
    RepositoryDoc returnedDoc = (RepositoryDoc) actual;
    try (InputStream inputStream = returnedDoc.getContent().getInputStream()) {
      String actualContent = new String(ByteStreams.toByteArray(inputStream), UTF_8);
      assertEquals("attachment", actualContent);
    }
    assertEquals(expected.getItem(), returnedDoc.getItem());
    assertEquals(expected.getContentFormat(), returnedDoc.getContentFormat());
    assertEquals(expected.getRequestMode(), returnedDoc.getRequestMode());
  }

  @Test
  public void testGetListItemDocContentNormalizedContentType() throws Exception {
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
            .replaceAll("ows_Attachments='1'", "ows_Attachments='0'")
            .replaceAll("ows_ContentType='Item'", "ows_ContentType='Another Content Type'");
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
                    .setInheritFrom("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
                    .build())
            .setSourceRepositoryUrl(
                FieldOrValue.withValue("http://localhost:1/Lists/Custom%20List/DispForm.aspx?ID=2"))
            .setContainerName("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .setUpdateTime(
                FieldOrValue.withValue(
                    getParsedDateTime(
                        SharePointRepository.MODIFIED_DATE_LIST_ITEM_FORMAT,
                        "2012-05-04T21:24:32Z")))
            .setCreateTime(
                FieldOrValue.withValue(
                    getParsedDateTime(
                        SharePointRepository.CREATED_DATE_LIST_ITEM_FORMAT,
                        "2012-05-01T22:14:06Z")))
            .setPayload(payloadItem.encodePayload())
            .setObjectType("AnotherContentType")
            .setTitle(FieldOrValue.withValue("Inside Folder"))
            .setItemType(ItemType.CONTAINER_ITEM);

    Multimap<String, Object> values = ArrayListMultimap.create();
    values.put("Title", "Inside Folder");
    values.put("ContentType", "Another Content Type");
    values.put("Modified", "2012-05-04T21:24:32Z");
    values.put("Created", "2012-05-01T22:14:06Z");
    values.put("CreatedBy", "System Account");
    values.put("ModifiedBy", "System Account");
    values.putAll("MultiValue", Arrays.asList("alpha", "beta"));
    itemBuilder.setValues(values);

    RepositoryDoc.Builder expectedDoc = new RepositoryDoc.Builder().setItem(itemBuilder.build());
    RepositoryDoc expected = expectedDoc.build();
    ApiOperation actual = repo.getDoc(entry);
    RepositoryDoc returnedDoc = (RepositoryDoc) actual;
    assertEquals(expected.getItem(), returnedDoc.getItem());
    // Explicitly validating that values set with structured data are populated via
    // IndexingItemBuilder.
    assertThat(
        returnedDoc.getItem().getStructuredData().getObject().getProperties(),
        hasItem(
            new NamedProperty()
                .setName("CreatedBy")
                .setTextValues(new TextValues().setValues(ImmutableList.of("System Account")))));
    assertThat(
        returnedDoc.getItem().getStructuredData().getObject().getProperties(),
        hasItem(
            new NamedProperty()
                .setName("MultiValue")
                .setTextValues(new TextValues().setValues(ImmutableList.of("alpha", "beta")))));
  }

  @Test
  public void testGetListItemDocContentObjectTypeNotAvailable() throws Exception {
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
            .replaceAll("ows_Attachments='1'", "ows_Attachments='0'")
            .replaceAll("ows_ContentType='Item'", "ows_ContentType='Something Else'");
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
                    .setInheritFrom("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
                    .build())
            .setSourceRepositoryUrl(
                FieldOrValue.withValue("http://localhost:1/Lists/Custom%20List/DispForm.aspx?ID=2"))
            .setContainerName("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .setUpdateTime(
                FieldOrValue.withValue(
                    getParsedDateTime(
                        SharePointRepository.MODIFIED_DATE_LIST_ITEM_FORMAT,
                        "2012-05-04T21:24:32Z")))
            .setCreateTime(
                FieldOrValue.withValue(
                    getParsedDateTime(
                        SharePointRepository.CREATED_DATE_LIST_ITEM_FORMAT,
                        "2012-05-01T22:14:06Z")))
            .setPayload(payloadItem.encodePayload())
            .setTitle(FieldOrValue.withValue("Inside Folder"))
            .setItemType(ItemType.CONTAINER_ITEM);

    Multimap<String, Object> values = ArrayListMultimap.create();
    values.put("Title", "Inside Folder");
    values.put("ContentType", "Something Else");
    values.put("Modified", "2012-05-04T21:24:32Z");
    values.put("Created", "2012-05-01T22:14:06Z");
    values.put("Author", "System Account");
    values.put("Editor", "System Account");
    values.putAll("MultiValue", Arrays.asList("alpha", "beta"));
    itemBuilder.setValues(values);

    RepositoryDoc.Builder expectedDoc = new RepositoryDoc.Builder().setItem(itemBuilder.build());
    RepositoryDoc expected = expectedDoc.build();
    ApiOperation actual = repo.getDoc(entry);
    RepositoryDoc returnedDoc = (RepositoryDoc) actual;
    assertEquals(expected.getItem(), returnedDoc.getItem());
    assertNull(returnedDoc.getItem().getStructuredData());
  }

  @Test
  public void testGetListItemDocContentTypeNull() throws Exception {
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
            .replaceAll("ows_Attachments='1'", "ows_Attachments='0'")
            .replaceAll("ows_ContentType='Item'", "");
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
                    .setInheritFrom("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
                    .build())
            .setSourceRepositoryUrl(
                FieldOrValue.withValue("http://localhost:1/Lists/Custom%20List/DispForm.aspx?ID=2"))
            .setContainerName("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .setUpdateTime(
                FieldOrValue.withValue(
                    getParsedDateTime(
                        SharePointRepository.MODIFIED_DATE_LIST_ITEM_FORMAT,
                        "2012-05-04T21:24:32Z")))
            .setCreateTime(
                FieldOrValue.withValue(
                    getParsedDateTime(
                        SharePointRepository.CREATED_DATE_LIST_ITEM_FORMAT,
                        "2012-05-01T22:14:06Z")))
            .setPayload(payloadItem.encodePayload())
            .setTitle(FieldOrValue.withValue("Inside Folder"))
            .setItemType(ItemType.CONTAINER_ITEM);

    Multimap<String, Object> values = ArrayListMultimap.create();
    values.put("Title", "Inside Folder");
    values.put("Modified", "2012-05-04T21:24:32Z");
    values.put("Created", "2012-05-01T22:14:06Z");
    values.put("Author", "System Account");
    values.put("Editor", "System Account");
    values.putAll("MultiValue", Arrays.asList("alpha", "beta"));
    itemBuilder.setValues(values);

    RepositoryDoc.Builder expectedDoc = new RepositoryDoc.Builder().setItem(itemBuilder.build());
    RepositoryDoc expected = expectedDoc.build();
    ApiOperation actual = repo.getDoc(entry);
    RepositoryDoc returnedDoc = (RepositoryDoc) actual;
    assertEquals(expected.getItem(), returnedDoc.getItem());
    assertNull(returnedDoc.getItem().getStructuredData());
  }

  @Test
  public void testGetDocInvalidPayload() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
    repo.init(repoContext);
    Item entry =
        new Item().setName("item-invalid-payload").encodePayload("invalid payload".getBytes(UTF_8));

    ApiOperation actual = repo.getDoc(entry);
    assertEquals(ApiOperations.deleteItem("item-invalid-payload"), actual);
  }

  @Test
  public void testGetDocEmptyPayload() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
    repo.init(repoContext);
    Item entry = new Item().setName("item-empty-payload");

    ApiOperation actual = repo.getDoc(entry);
    assertEquals(ApiOperations.deleteItem("item-empty-payload"), actual);
  }

  @Test
  public void testGetDocEmptyPayloadGuid() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
    repo.init(repoContext);
    Item entry = new Item().setName("{E7156244-AC2F-4402-AA74-7A365726CD02}");

    ApiOperation actual = repo.getDoc(entry);
    PushItems expected =
        new PushItems.Builder()
            .addPushItem(
                "{E7156244-AC2F-4402-AA74-7A365726CD02}",
                new PushItem()
                    .setQueue("undefined")
                    .setType("REPOSITORY_ERROR")
                    .setRepositoryError(new RepositoryError().setErrorMessage("Empty Payload")))
            .build();
    assertEquals(expected, actual);
  }

  @Test
  public void testGetDocEmptyPayloadUrl() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
    repo.init(repoContext);
    Item entry = new Item().setName("http://localhost:1/subsite");

    ApiOperation actual = repo.getDoc(entry);
    PushItems expected =
        new PushItems.Builder()
            .addPushItem(
                "http://localhost:1/subsite",
                new PushItem()
                    .setQueue("undefined")
                    .setType("REPOSITORY_ERROR")
                    .setRepositoryError(new RepositoryError().setErrorMessage("Empty Payload")))
            .build();
    assertEquals(expected, actual);
  }

  @Test
  public void testGetDocInvalidJsonPayload() throws IOException {
    SharePointRepository repo = setUpDefaultRepository();
    repo.init(repoContext);
    Item entry =
        new Item()
            .setName("item-invalid-payload")
            .encodePayload(new GenericJson().toString().getBytes(UTF_8));

    ApiOperation actual = repo.getDoc(entry);
    assertEquals(ApiOperations.deleteItem("item-invalid-payload"), actual);
  }

  @Test
  public void testGetChangesNullCheckpointNoChangesSinceInitSiteCollectionOnly()
      throws IOException {
    SharePointRepository repo = getSharePointRepository();
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
    CheckpointCloseableIterable<ApiOperation> expected =
        new CheckpointCloseableIterableImpl.Builder<ApiOperation>(Collections.emptyList())
            .setCheckpoint(checkpoint.encodePayload())
            .setHasMore(false)
            .build();

    CheckpointCloseableIterable<ApiOperation> changes = repo.getChanges(null);
    assertTrue(checkpointIterableRule.compare(expected, changes));
  }

  @Test
  public void testGetChangesWithCheckpointNoChangesSinceInitSiteCollectionOnly()
      throws IOException {
    SharePointRepository repo = getSharePointRepository();
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
    CheckpointCloseableIterable<ApiOperation> expected =
        new CheckpointCloseableIterableImpl.Builder<ApiOperation>(Collections.emptyList())
            .setCheckpoint(checkpoint.encodePayload())
            .build();

    CheckpointCloseableIterable<ApiOperation> changes = repo.getChanges(checkpoint.encodePayload());
    assertTrue(checkpointIterableRule.compare(expected, changes));
  }

  @Test
  public void testGetChangesInvalidCheckpointNoChangesSinceInitSiteCollectionOnly()
      throws IOException {
    SharePointRepository repo = getSharePointRepository();
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
    CheckpointCloseableIterable<ApiOperation> expected =
        new CheckpointCloseableIterableImpl.Builder<ApiOperation>(Collections.emptyList())
            .setCheckpoint(checkpoint.encodePayload())
            .setHasMore(false)
            .build();

    CheckpointCloseableIterable<ApiOperation> changes = repo.getChanges("invalid".getBytes());
    assertTrue(checkpointIterableRule.compare(expected, changes));
  }

  @Test
  public void testGetChangesSwitchToSiteCollectionOnlyModeNoChangesSinceInitSiteCollectionOnly()
      throws IOException {
    SharePointRepository repo = getSharePointRepository();
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
    CheckpointCloseableIterable<ApiOperation> expected =
        new CheckpointCloseableIterableImpl.Builder<ApiOperation>(Collections.emptyList())
            .setCheckpoint(checkpoint.encodePayload())
            .build();

    CheckpointCloseableIterable<ApiOperation> changes =
        repo.getChanges(checkpointOld.encodePayload());
    assertTrue(checkpointIterableRule.compare(expected, changes));
  }

  @Test
  public void testGetChangesSwitchToAnotherSiteCollectionChangesSinceInitSiteCollectionOnly()
      throws IOException {
    SharePointRepository repo = getSharePointRepository();
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
            .setWebId("{b2ea1067-3a54-4ab7-a459-c8ec864b97eb}")
            .setUrl("http://localhost:1/Lists/Announcements/2_.000")
            .setObjectId("item")
            .build();
    CheckpointCloseableIterable<ApiOperation> expected =
        new CheckpointCloseableIterableImpl.Builder<>(
                Collections.<ApiOperation>singleton(
                    new PushItems.Builder()
                        .addPushItem(
                            "{5085BE94-B5C1-45C8-A047-D0F03344FE31}",
                            new PushItem()
                                .setType("MODIFIED")
                                .encodePayload(listItemObject.encodePayload()))
                        .build()))
            .setCheckpoint(checkpoint.encodePayload())
            .build();

    CheckpointCloseableIterable<ApiOperation> changes =
        repo.getChanges(checkpointOld.encodePayload());
    assertTrue(checkpointIterableRule.compare(expected, changes));
  }

  @Test
  public void testGetChangesSinceCheckpointSiteCollectionOnly() throws IOException {
    SharePointRepository repo = getSharePointRepository();
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
            .setWebId("{b2ea1067-3a54-4ab7-a459-c8ec864b97eb}")
            .setUrl("http://localhost:1/Lists/Announcements/2_.000")
            .setObjectId("item")
            .build();
    CheckpointCloseableIterable<ApiOperation> expected =
        new CheckpointCloseableIterableImpl.Builder<>(
                Collections.<ApiOperation>singleton(
                    new PushItems.Builder()
                        .addPushItem(
                            "{5085BE94-B5C1-45C8-A047-D0F03344FE31}",
                            new PushItem()
                                .setType("MODIFIED")
                                .encodePayload(listItemObject.encodePayload()))
                        .build()))
            .setCheckpoint(checkpoint.encodePayload())
            .build();

    CheckpointCloseableIterable<ApiOperation> changes =
        repo.getChanges(checkpointOld.encodePayload());
    assertTrue(checkpointIterableRule.compare(expected, changes));
  }

  @Test
  public void testGetChangesSitePermissions() throws IOException {
    SharePointRepository repo = getSharePointRepository();
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
    setupGetSiteAndWeb("http://localhost:1", "http://localhost:1", "http://localhost:1", 0);
    String changes726 =
        SharePointResponseHelper.getChangesSitePermissionsChange()
            .replace("<SPSite ", "<SPSite xmlns='" + XMLNS + "' ");
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
        new SharePointObject.Builder(SharePointObject.SITE_COLLECTION)
            .setSiteId("{8a5b00a0-efcb-45d0-b282-f2d4dc746eeb}")
            .setWebId("{eb1869b6-0258-4713-8298-f5412677da26}")
            .setObjectId("{eb1869b6-0258-4713-8298-f5412677da26}")
            .setUrl("http://localhost:1")
            .build();
    CheckpointCloseableIterable<ApiOperation> expected =
        new CheckpointCloseableIterableImpl.Builder<>(
                Collections.<ApiOperation>singleton(
                    new PushItems.Builder()
                        .addPushItem(
                            "http://localhost:1",
                            new PushItem()
                                .setType("MODIFIED")
                                .encodePayload(listItemObject.encodePayload()))
                        .build()))
            .setCheckpoint(checkpoint.encodePayload())
            .build();

    CheckpointCloseableIterable<ApiOperation> changes =
        repo.getChanges(checkpointOld.encodePayload());
    assertTrue(checkpointIterableRule.compare(expected, changes));
  }

  @Test
  public void testGetChangesForSubSite() throws IOException {
    SharePointRepository repo = getSharePointRepository();
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
    SiteConnector scSubSite =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1/subsite")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1/subsite"))
        .thenReturn(scSubSite);
    setupGetSiteAndWeb(
        "http://localhost:1/subsite", "http://localhost:1", "http://localhost:1/subsite", 0);
    String changes726 =
        SharePointResponseHelper.getChangesSitePermissionsChange()
            .replace("<SPSite ", "<SPSite xmlns='" + XMLNS + "' ")
            .replace("DisplayUrl=\"\"", "DisplayUrl=\"/subsite\"");
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
        new SharePointObject.Builder(SharePointObject.WEB)
            .setSiteId("{8a5b00a0-efcb-45d0-b282-f2d4dc746eeb}")
            .setWebId("{eb1869b6-0258-4713-8298-f5412677da26}")
            .setObjectId("{eb1869b6-0258-4713-8298-f5412677da26}")
            .setUrl("http://localhost:1/subsite")
            .build();
    CheckpointCloseableIterable<ApiOperation> expected =
        new CheckpointCloseableIterableImpl.Builder<>(
                Collections.<ApiOperation>singleton(
                    new PushItems.Builder()
                        .addPushItem(
                            "http://localhost:1/subsite",
                            new PushItem()
                                .setType("MODIFIED")
                                .encodePayload(listItemObject.encodePayload()))
                        .build()))
            .setCheckpoint(checkpoint.encodePayload())
            .build();

    CheckpointCloseableIterable<ApiOperation> changes =
        repo.getChanges(checkpointOld.encodePayload());
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
    CheckpointCloseableIterable<ApiOperation> expected =
        new CheckpointCloseableIterableImpl.Builder<ApiOperation>(
                Collections.singleton(new PushItems.Builder().build()))
            .setCheckpoint(checkpoint.encodePayload())
            .build();

    CheckpointCloseableIterable<ApiOperation> changes = repo.getChanges(null);
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
    CheckpointCloseableIterable<ApiOperation> expected =
        new CheckpointCloseableIterableImpl.Builder<>(
                Collections.<ApiOperation>singleton(new PushItems.Builder().build()))
            .setCheckpoint(checkpoint.encodePayload())
            .setHasMore(false)
            .build();

    CheckpointCloseableIterable<ApiOperation> changes = repo.getChanges(checkpoint.encodePayload());
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
            .setWebId("{b2ea1067-3a54-4ab7-a459-c8ec864b97eb}")
            .setUrl("http://localhost:1/Lists/Announcements/2_.000")
            .setObjectId("item")
            .build();
    CheckpointCloseableIterable<ApiOperation> expected =
        new CheckpointCloseableIterableImpl.Builder<>(
                Collections.<ApiOperation>singleton(
                    new PushItems.Builder()
                        .addPushItem(
                            "{5085BE94-B5C1-45C8-A047-D0F03344FE31}",
                            new PushItem()
                                .setType("MODIFIED")
                                .encodePayload(listItemObject.encodePayload()))
                        .build()))
            .setCheckpoint(checkpoint.encodePayload())
            .build();

    CheckpointCloseableIterable<ApiOperation> changes =
        repo.getChanges(checkpointOld.encodePayload());
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
            .setWebId("{b2ea1067-3a54-4ab7-a459-c8ec864b97eb}")
            .setUrl("http://localhost:1/Lists/Announcements/2_.000")
            .setObjectId("item")
            .build();
    CheckpointCloseableIterable<ApiOperation> expected =
        new CheckpointCloseableIterableImpl.Builder<>(
                Collections.<ApiOperation>singleton(
                    new PushItems.Builder()
                        .addPushItem(
                            "{5085BE94-B5C1-45C8-A047-D0F03344FE31}",
                            new PushItem()
                                .setType("MODIFIED")
                                .encodePayload(listItemObject.encodePayload()))
                        .build()))
            .setCheckpoint(checkpoint.encodePayload())
            .build();

    CheckpointCloseableIterable<ApiOperation> changes =
        repo.getChanges(checkpointOld.encodePayload());
    assertTrue(checkpointIterableRule.compare(expected, changes));
  }

  @Test
  public void testIsHtmlContent() {
    assertFalse(SharePointRepository.isHtmlContent(null));
    assertFalse(SharePointRepository.isHtmlContent(""));
    assertFalse(SharePointRepository.isHtmlContent("invalid"));
    assertFalse(SharePointRepository.isHtmlContent("text/plain"));
    assertFalse(SharePointRepository.isHtmlContent("text/xhtml"));

    assertTrue(SharePointRepository.isHtmlContent("text/html"));
    assertTrue(SharePointRepository.isHtmlContent("text/html;"));
    assertTrue(SharePointRepository.isHtmlContent("text/html; charset=utf-8"));
    assertTrue(SharePointRepository.isHtmlContent("text/html; charset=utf-8;"));
    assertTrue(SharePointRepository.isHtmlContent("text/html; something"));
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
    SharePointRepository repo = getSharePointRepository();
    overrideConfig(getBaseConfig());
    setupVirtualServerForInit();
    return repo;
  }

  private SharePointRepository getSharePointRepository() {
    return new SharePointRepository(
        httpClientBuilder, siteConnectorFactoryBuilder, new AuthenticationClientFactoryImpl());
  }

  private Properties getBaseConfig() {
    Properties properties = new Properties();
    properties.put("sharepoint.server", "http://localhost:1");
    properties.put("sharepoint.username", "user");
    properties.put("sharepoint.password", "password");
    properties.put("contentTemplate.sharepointItem.title", "Title");
    properties.put(
        "contentTemplate.sharepointItem.quality.low",
        "Created,CreatedBy,ModifiedBy,ContentType,MultiValue");
    properties.put("contentTemplate.sharepointItem.unmappedColumnsMode", "IGNORE");
    properties.put("api.referenceIdentitySources", "GDC-PSL");
    properties.put("api.referenceIdentitySource.GDC-PSL.id", "idSourceGdcPsl");
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
    entries.put(
        "{133fcb96-7e9b-46c9-b5f3-09770a35ad8a}",
        new PushItem().encodePayload(payloadAnnoucement.encodePayload()));
    String sharedDocsUrl = webUrl + "/Shared Documents/Forms/AllItems.aspx";
    SharePointObject payloadSharedDoc =
        new SharePointObject.Builder(SharePointObject.LIST)
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{b2ea1067-3a54-4ab7-a459-c8ec864b97eb}")
            .setListId("{648f6636-3d90-4565-86b9-2dd7611fc855}")
            .setUrl(sharedDocsUrl)
            .setObjectId("{648f6636-3d90-4565-86b9-2dd7611fc855}")
            .build();
    entries.put(
        "{648f6636-3d90-4565-86b9-2dd7611fc855}",
        new PushItem().encodePayload(payloadSharedDoc.encodePayload()));
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
        "{CE33B6B7-9F5E-4224-8D77-9C42E6290FE6}",
        new PushItem().encodePayload(payloadFolder.encodePayload()));
    SharePointObject payloadItem =
        new SharePointObject.Builder(SharePointObject.LIST_ITEM)
            .setListId("{6f33949a-b3ff-4b0c-ba99-93cb518ac2c0}")
            .setSiteId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setWebId("{bb3bb2dd-6ea7-471b-a361-6fb67988755c}")
            .setUrl(listUrl + "/3_.000")
            .setObjectId("item")
            .build();
    entries.put(
        "{FD87F56D-DBE1-4EB1-8379-0B83082615E0}",
        new PushItem().encodePayload(payloadItem.encodePayload()));
    return entries;
  }

  private Item getWebItem(String url, String parent, String aclParent, boolean inherit) {
    Item item =
        new Item()
            .setName(url)
            .setMetadata(new ItemMetadata().setContainerName(parent).setSourceRepositoryUrl(url))
            .setItemType(ItemType.CONTAINER_ITEM.name());
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
                  Acl.getUserPrincipal("GDC-PSL\\spuser1", "idSourceGdcPsl"),
                  Acl.getGroupPrincipal(
                      SiteConnector.encodeSharePointLocalGroupName(
                          "http://localhost:1", "TeamSite Owners")),
                  Acl.getGroupPrincipal(
                      SiteConnector.encodeSharePointLocalGroupName(
                          "http://localhost:1", "TeamSite Visitors")),
                  Acl.getGroupPrincipal(
                      SiteConnector.encodeSharePointLocalGroupName(
                          "http://localhost:1", "TeamSite Members"))))
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

  private static DateTime getParsedDateTime(String pattern, String value) throws Exception {
    SimpleDateFormat dateTimeFormat = new SimpleDateFormat(pattern);
    dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    return new DateTime(dateTimeFormat.parse(value));
  }
}
