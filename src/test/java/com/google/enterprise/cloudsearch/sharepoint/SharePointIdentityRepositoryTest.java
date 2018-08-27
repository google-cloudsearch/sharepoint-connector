package com.google.enterprise.cloudsearch.sharepoint;

import static org.hamcrest.CoreMatchers.anything;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.cloudidentity.v1beta1.model.EntityKey;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.identity.IdentityGroup;
import com.google.enterprise.cloudsearch.sdk.identity.RepositoryContext;
import com.microsoft.schemas.sharepoint.soap.ContentDatabase;
import com.microsoft.schemas.sharepoint.soap.Site;
import com.microsoft.schemas.sharepoint.soap.VirtualServer;
import com.microsoft.schemas.sharepoint.soap.directory.UserGroupSoap;
import com.microsoft.schemas.sharepoint.soap.people.PeopleSoap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SharePointIdentityRepositoryTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Mock SiteConnectorFactoryImpl.Builder siteConnectorFactoryBuilder;
  @Mock SiteConnectorFactoryImpl siteConnectorFactory;
  @Mock SiteDataClient siteDataClient;
  @Mock PeopleSoap peopleSoap;
  @Mock UserGroupSoap userGroupSoap;
  @Mock ActiveDirectoryClient activeDirectoryClient;
  RepositoryContext repoContext;

  @Before
  public void setup() {
    when(siteConnectorFactoryBuilder.setRequestContext(any()))
        .thenReturn(siteConnectorFactoryBuilder);
    when(siteConnectorFactoryBuilder.setXmlValidation(false))
        .thenReturn(siteConnectorFactoryBuilder);
    when(siteConnectorFactoryBuilder.setActiveDirectoryClient(any()))
        .thenReturn(siteConnectorFactoryBuilder);
    when(siteConnectorFactoryBuilder.build()).thenReturn(siteConnectorFactory);
    when(siteConnectorFactoryBuilder.setReferenceIdentitySourceConfiguration(any()))
        .thenReturn(siteConnectorFactoryBuilder);
  }

  @Test
  public void testInitInvalidSharePointUrl() throws IOException {
    SharePointIdentityRepository repo =
        new SharePointIdentityRepository(siteConnectorFactoryBuilder);

    Properties baseConfig = getBaseConfig();
    baseConfig.put("sharepoint.server", "abc");
    overrideConfig(baseConfig);
    thrown.expect(InvalidConfigurationException.class);
    repo.init(repoContext);
  }

  @Test
  public void testInit() throws IOException {
    SharePointIdentityRepository repo =
        new SharePointIdentityRepository(siteConnectorFactoryBuilder);
    Properties baseConfig = getBaseConfig();
    overrideConfig(baseConfig);
    repo.init(repoContext);
  }

  @Test
  public void testListUsersEmpty() throws IOException {
    SharePointIdentityRepository repo =
        new SharePointIdentityRepository(siteConnectorFactoryBuilder);
    Properties baseConfig = getBaseConfig();
    overrideConfig(baseConfig);
    repo.init(repoContext);
    assertThat(repo.listUsers(null /* Checkpoint */), not(hasItem(anything())));
  }

  @Test
  public void testListGroupsVirtualServer() throws IOException {
    SharePointIdentityRepository repo =
        new SharePointIdentityRepository(siteConnectorFactoryBuilder);
    Properties baseConfig = getBaseConfig();
    overrideConfig(baseConfig);
    repo.init(repoContext);
    setupVirtualServerForGroups();
    CheckpointCloseableIterable<IdentityGroup> groups = repo.listGroups(null /* Checkpoint */);
    IdentityGroup teamOwners =
        repoContext.buildIdentityGroup(
            SiteConnector.encodeSharePointLocalGroupName("http://localhost:1", "TeamSite Owners"),
            () -> ImmutableSet.of());
    IdentityGroup teamMembers =
        repoContext.buildIdentityGroup(
            SiteConnector.encodeSharePointLocalGroupName("http://localhost:1", "TeamSite Members"),
            () -> ImmutableSet.of());
    IdentityGroup teamVisitors =
        repoContext.buildIdentityGroup(
            SiteConnector.encodeSharePointLocalGroupName("http://localhost:1", "TeamSite Visitors"),
            () -> ImmutableSet.of());
    IdentityGroup teamOwnersSC =
        repoContext.buildIdentityGroup(
            SiteConnector.encodeSharePointLocalGroupName(
                "http://localhost:1/sites/SiteCollection", "TeamSite Owners"),
            () -> ImmutableSet.of());
    IdentityGroup teamMembersSC =
        repoContext.buildIdentityGroup(
            SiteConnector.encodeSharePointLocalGroupName(
                "http://localhost:1/sites/SiteCollection", "TeamSite Members"),
            () -> ImmutableSet.of());
    IdentityGroup teamVisitorsSC =
        repoContext.buildIdentityGroup(
            SiteConnector.encodeSharePointLocalGroupName(
                "http://localhost:1/sites/SiteCollection", "TeamSite Visitors"),
            () -> ImmutableSet.of());
    Set<EntityKey> actual =
        Streams.stream(groups).map(g -> g.getGroupKey()).collect(Collectors.toSet());
    Set<EntityKey> expected =
        Arrays.asList(
                teamOwners, teamMembers, teamVisitors, teamOwnersSC, teamMembersSC, teamVisitorsSC)
            .stream()
            .map(g -> g.getGroupKey())
            .collect(Collectors.toSet());
    assertEquals(expected, actual);
  }

  @Test
  public void testListGroupsSiteCollectionOnlyExplicit() throws IOException {
    SharePointIdentityRepository repo =
        new SharePointIdentityRepository(siteConnectorFactoryBuilder);
    Properties baseConfig = getBaseConfig();
    baseConfig.put("sharepoint.siteCollectionOnly", "true");
    overrideConfig(baseConfig);
    repo.init(repoContext);
    String xml = SharePointResponseHelper.getSiteCollectionResponse();
        setupSiteCollectionForGroups(
            "http://localhost:1", xml.replaceAll("/sites/SiteCollection", ""));
    CheckpointCloseableIterable<IdentityGroup> groups = repo.listGroups(null /* Checkpoint */);
    IdentityGroup teamOwners =
        repoContext.buildIdentityGroup(
            SiteConnector.encodeSharePointLocalGroupName("http://localhost:1", "TeamSite Owners"),
            () -> ImmutableSet.of());
    IdentityGroup teamMembers =
        repoContext.buildIdentityGroup(
            SiteConnector.encodeSharePointLocalGroupName("http://localhost:1", "TeamSite Members"),
            () -> ImmutableSet.of());
    IdentityGroup teamVisitors =
        repoContext.buildIdentityGroup(
            SiteConnector.encodeSharePointLocalGroupName("http://localhost:1", "TeamSite Visitors"),
            () -> ImmutableSet.of());
    Set<EntityKey> actual =
        Streams.stream(groups).map(g -> g.getGroupKey()).collect(Collectors.toSet());
    Set<EntityKey> expected =
        Arrays.asList(teamOwners, teamMembers, teamVisitors)
            .stream()
            .map(g -> g.getGroupKey())
            .collect(Collectors.toSet());
    assertEquals(expected, actual);
  }

  @Test
  public void testListGroupsSiteCollectionOnlyByUrl() throws IOException {
    SharePointIdentityRepository repo =
        new SharePointIdentityRepository(siteConnectorFactoryBuilder);
    Properties baseConfig = getBaseConfig();
    baseConfig.put("sharepoint.server", "http://localhost:1/sites/SiteCollection");
    overrideConfig(baseConfig);
    repo.init(repoContext);
    String xml = SharePointResponseHelper.getSiteCollectionResponse();
    setupSiteCollectionForGroups("http://localhost:1/sites/SiteCollection", xml);
    CheckpointCloseableIterable<IdentityGroup> groups = repo.listGroups(null /* Checkpoint */);
    IdentityGroup teamOwners =
        repoContext.buildIdentityGroup(
            SiteConnector.encodeSharePointLocalGroupName(
                "http://localhost:1/sites/SiteCollection", "TeamSite Owners"),
            () -> ImmutableSet.of());
    IdentityGroup teamMembers =
        repoContext.buildIdentityGroup(
            SiteConnector.encodeSharePointLocalGroupName(
                "http://localhost:1/sites/SiteCollection", "TeamSite Members"),
            () -> ImmutableSet.of());
    IdentityGroup teamVisitors =
        repoContext.buildIdentityGroup(
            SiteConnector.encodeSharePointLocalGroupName(
                "http://localhost:1/sites/SiteCollection", "TeamSite Visitors"),
            () -> ImmutableSet.of());
    Set<EntityKey> actual =
        Streams.stream(groups).map(g -> g.getGroupKey()).collect(Collectors.toSet());
    Set<EntityKey> expected =
        Arrays.asList(teamOwners, teamMembers, teamVisitors)
            .stream()
            .map(g -> g.getGroupKey())
            .collect(Collectors.toSet());
    assertEquals(expected, actual);
  }

  private Properties getBaseConfig() {
    Properties properties = new Properties();
    properties.put("sharepoint.server", "http://localhost:1");
    properties.put("sharepoint.username", "user");
    properties.put("sharepoint.password", "password");
    properties.put("api.identitySourceId", "idSource1");
    return properties;
  }

  private void overrideConfig(Properties properties) {
    setupConfig.initConfig(properties);
    repoContext = RepositoryContext.fromConfiguration();
  }

  private void setupVirtualServerForGroups() throws IOException {
    String xml = SharePointResponseHelper.getSiteCollectionResponse();
    SiteConnector scRoot =
        new SiteConnector.Builder("http://localhost:1", "http://localhost:1")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .setActiveDirectoryClient(activeDirectoryClient)
            .build();
    when(siteConnectorFactory.getInstance("http://localhost:1", "http://localhost:1"))
        .thenReturn(scRoot);
    Site site =
        SiteDataClient.jaxbParse(xml.replaceAll("/sites/SiteCollection", ""), Site.class, false);
    when(siteDataClient.getContentSite()).thenReturn(site);
    SiteDataClient siteDataClientSC = mock(SiteDataClient.class);
    SiteConnector scCollection =
        new SiteConnector.Builder(
                "http://localhost:1/sites/SiteCollection",
                "http://localhost:1/sites/SiteCollection")
            .setSiteDataClient(siteDataClientSC)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .setActiveDirectoryClient(activeDirectoryClient)
            .build();
    when(siteConnectorFactory.getInstance(
            "http://localhost:1/sites/SiteCollection", "http://localhost:1/sites/SiteCollection"))
        .thenReturn(scCollection);
    Site siteSC = SiteDataClient.jaxbParse(xml, Site.class, false);
    when(siteDataClientSC.getContentSite()).thenReturn(siteSC);
    setupVirualServer();
    setupContentDb("{4fb7dea1-2912-4927-9eda-1ea2f0977cf8}");
  }

  private void setupSiteCollectionForGroups(String siteCollectionUrl, String xml)
      throws IOException {
    SiteConnector scRoot =
        new SiteConnector.Builder(siteCollectionUrl, siteCollectionUrl)
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .setActiveDirectoryClient(activeDirectoryClient)
            .build();
    when(siteConnectorFactory.getInstance(siteCollectionUrl, siteCollectionUrl)).thenReturn(scRoot);
    Site site = SiteDataClient.jaxbParse(xml, Site.class, false);
    when(siteDataClient.getContentSite()).thenReturn(site);
  }

  private void setupVirualServer() throws IOException {
    VirtualServer vs =
        SiteDataClient.jaxbParse(
            SharePointResponseHelper.loadTestResponse("vs.xml"), VirtualServer.class, false);
    when(siteDataClient.getContentVirtualServer()).thenReturn(vs);
  }

  private void setupContentDb(String id) throws IOException {
    ContentDatabase cd =
        SiteDataClient.jaxbParse(
            SharePointResponseHelper.loadTestResponse("cd.xml"), ContentDatabase.class, false);
    when(siteDataClient.getContentContentDatabase(id, true)).thenReturn(cd);
  }
}
