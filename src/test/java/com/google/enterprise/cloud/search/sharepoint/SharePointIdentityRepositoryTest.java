package com.google.enterprise.cloud.search.sharepoint;

import static org.hamcrest.CoreMatchers.anything;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.identity.IdentityGroup;
import com.google.enterprise.cloudsearch.sdk.identity.RepositoryContext;
import com.microsoft.schemas.sharepoint.soap.ContentDatabase;
import com.microsoft.schemas.sharepoint.soap.VirtualServer;
import com.microsoft.schemas.sharepoint.soap.directory.UserGroupSoap;
import com.microsoft.schemas.sharepoint.soap.people.PeopleSoap;
import java.io.IOException;
import java.util.Properties;
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
  @Mock SiteConnector siteConnector;
  @Mock SiteDataClient siteDataClient;
  @Mock PeopleSoap peopleSoap;
  @Mock UserGroupSoap userGroupSoap;
  @Mock RepositoryContext repoContext;

  @Before
  public void setup() {
    when(siteConnectorFactoryBuilder.setRequestContext(any()))
        .thenReturn(siteConnectorFactoryBuilder);
    when(siteConnectorFactoryBuilder.setXmlValidation(false))
        .thenReturn(siteConnectorFactoryBuilder);
    when(siteConnectorFactoryBuilder.setActiveDirectoryClient(any()))
        .thenReturn(siteConnectorFactoryBuilder);
    when(siteConnectorFactoryBuilder.build()).thenReturn(siteConnectorFactory);
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
    assertThat(groups, not(hasItem(anything())));
  }

  @Test
  public void testListGroupsSiteCollectionOnlyExplicit() throws IOException {
    SharePointIdentityRepository repo =
        new SharePointIdentityRepository(siteConnectorFactoryBuilder);
    Properties baseConfig = getBaseConfig();
    baseConfig.put("sharepoint.siteCollectionOnly", "true");
    overrideConfig(baseConfig);
    repo.init(repoContext);
    setupSiteCollectionForGroups("http://localhost:1");
    CheckpointCloseableIterable<IdentityGroup> groups = repo.listGroups(null /* Checkpoint */);
    assertThat(groups, not(hasItem(anything())));
  }

  @Test
  public void testListGroupsSiteCollectionOnlyByUrl() throws IOException {
    SharePointIdentityRepository repo =
        new SharePointIdentityRepository(siteConnectorFactoryBuilder);
    Properties baseConfig = getBaseConfig();
    baseConfig.put("sharepoint.server", "http://localhost:1/sites/SiteCollection");
    overrideConfig(baseConfig);
    repo.init(repoContext);
    setupSiteCollectionForGroups("http://localhost:1/sites/SiteCollection");
    CheckpointCloseableIterable<IdentityGroup> groups = repo.listGroups(null /* Checkpoint */);
    assertThat(groups, not(hasItem(anything())));
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

  private void setupVirtualServerForGroups() throws IOException {
    setupSiteCollectionForGroups("http://localhost:1");
    setupSiteCollectionForGroups("http://localhost:1/sites/SiteCollection");
    setupVirualServer();
    setupContentDb("{4fb7dea1-2912-4927-9eda-1ea2f0977cf8}");
  }

  private void setupSiteCollectionForGroups(String siteCollectionUrl) throws IOException {
    SiteConnector scRoot =
        new SiteConnector.Builder(siteCollectionUrl, siteCollectionUrl)
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    when(siteConnectorFactory.getInstance(siteCollectionUrl, siteCollectionUrl)).thenReturn(scRoot);
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
