package com.google.enterprise.cloudsearch.sharepoint;

import static org.junit.Assert.assertEquals;

import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class SharePointConfigurationTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Test
  public void testFromConfigurationConfigNotInitialized() {
    thrown.expect(IllegalStateException.class);
    SharePointConfiguration.fromConfiguration();
  }

  @Test
  public void testFromConfigurationConfigNoSharePointUrl() {
    setupConfig.initConfig(new Properties());
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("sharepoint.server");
    SharePointConfiguration.fromConfiguration();
  }

  @Test
  public void testFromConfigurationInavlidSharepointUrl() {
    Properties properties = new Properties();
    properties.put("sharepoint.server", "something");
    setupConfig.initConfig(properties);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Invalid SharePoint URL");
    SharePointConfiguration.fromConfiguration();
  }

  @Test
  public void testFromConfigurationWithDefaults() throws Exception {
    setupConfig.initConfig(getBaseConfiguration());
    SharePointConfiguration configuration = SharePointConfiguration.fromConfiguration();
    assertEquals(true, configuration.isPerformBrowserLeniency());
    assertEquals(false, configuration.isPerformXmlValidation());
    SharePointUrl expectedUrl =
        new SharePointUrl.Builder("http://localhost").setPerformBrowserLeniency(true).build();
    assertEquals(expectedUrl, configuration.getSharePointUrl());
    assertEquals(false, configuration.isSiteCollectionUrl());
  }

  @Test
  public void testFromConfigurationWithSiteCollectionOnly() throws Exception {
    Properties baseConfiguration = getBaseConfiguration();
    baseConfiguration.put("sharepoint.siteCollectionOnly", "true");
    setupConfig.initConfig(baseConfiguration);
    SharePointConfiguration configuration = SharePointConfiguration.fromConfiguration();
    assertEquals(true, configuration.isPerformBrowserLeniency());
    assertEquals(false, configuration.isPerformXmlValidation());
    SharePointUrl expectedUrl =
        new SharePointUrl.Builder("http://localhost").setPerformBrowserLeniency(true).build();
    assertEquals(expectedUrl, configuration.getSharePointUrl());
    assertEquals(true, configuration.isSiteCollectionUrl());
  }

  @Test
  public void testFromConfigurationWithSiteCollectionOnlyByUrl() throws Exception {
    Properties baseConfiguration = getBaseConfiguration();
    baseConfiguration.replace("sharepoint.server", "http://localhost/sites/collection");
    setupConfig.initConfig(baseConfiguration);
    SharePointConfiguration configuration = SharePointConfiguration.fromConfiguration();
    assertEquals(true, configuration.isPerformBrowserLeniency());
    assertEquals(false, configuration.isPerformXmlValidation());
    SharePointUrl expectedUrl =
        new SharePointUrl.Builder("http://localhost/sites/collection")
            .setPerformBrowserLeniency(true)
            .build();
    assertEquals(expectedUrl, configuration.getSharePointUrl());
    assertEquals(true, configuration.isSiteCollectionUrl());
  }

  @Test
  public void testNegativeSocketTimeoutSecs() throws Exception {
    Properties baseConfiguration = getBaseConfiguration();
    baseConfiguration.put("sharepoint.webservices.socketTimeoutSecs", "-50");
    setupConfig.initConfig(baseConfiguration);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Invalid SharePoint Configuration");
    SharePointConfiguration.fromConfiguration();
  }

  @Test
  public void testNegativeReadTimeoutSecs() throws Exception {
    Properties baseConfiguration = getBaseConfiguration();
    baseConfiguration.put("sharepoint.webservices.readTimeOutSecs", "-50");
    setupConfig.initConfig(baseConfiguration);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Invalid SharePoint Configuration");
    SharePointConfiguration.fromConfiguration();
  }

  @Test
  public void testFromConfigurationWithNonDefaults() throws Exception {
    Properties baseConfiguration = getBaseConfiguration();
    baseConfiguration.put("sharepoint.userAgent", "agent");
    baseConfiguration.put("sharepoint.webservices.socketTimeoutSecs", "50");
    baseConfiguration.put("sharepoint.webservices.readTimeOutSecs", "120");
    setupConfig.initConfig(baseConfiguration);
    SharePointConfiguration configuration = SharePointConfiguration.fromConfiguration();
    assertEquals(true, configuration.isPerformBrowserLeniency());
    assertEquals(false, configuration.isPerformXmlValidation());
    SharePointUrl expectedUrl =
        new SharePointUrl.Builder("http://localhost").setPerformBrowserLeniency(true).build();
    assertEquals(expectedUrl, configuration.getSharePointUrl());
    assertEquals("agent", configuration.getSharePointUserAgent());
    assertEquals(
        TimeUnit.MILLISECONDS.convert(50, TimeUnit.SECONDS),
        configuration.getWebservicesSocketTimeoutMills());
    assertEquals(
        TimeUnit.MILLISECONDS.convert(120, TimeUnit.SECONDS),
        configuration.getWebservicesReadTimeoutMills());
  }

  private Properties getBaseConfiguration() {
    Properties properties = new Properties();
    properties.put("sharepoint.server", "http://localhost");
    properties.put("sharepoint.username", "username");
    properties.put("sharepoint.password", "password");
    return properties;
  }
}
