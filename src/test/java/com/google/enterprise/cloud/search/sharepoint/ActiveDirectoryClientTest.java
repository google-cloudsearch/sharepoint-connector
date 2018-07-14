package com.google.enterprise.cloud.search.sharepoint;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.enterprise.cloud.search.sharepoint.ActiveDirectoryClient.ADServer;
import com.google.enterprise.cloud.search.sharepoint.ActiveDirectoryClient.LdapContextBuilder;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ActiveDirectoryClientTest {

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Mock ADServer mockAdServer;
  @Mock LdapContextBuilder mockContextBuilder;
  @Mock LdapContext mockContext;
  @Mock Attributes mockAttributes;

  @Before
  public void setup() throws Exception {
    when(mockContextBuilder.buildContext(any())).thenReturn(mockContext);
  }

  @Test
  public void testConstructorWithNullAdServer() throws Exception {
    thrown.expect(NullPointerException.class);
    new ActiveDirectoryClient(null);
  }

  @Test
  public void testConstructor() throws Exception {
    assertNotNull(new ActiveDirectoryClient(mockAdServer));
    verify(mockAdServer).start();
  }

  @Test
  public void testSidEmptyString() throws Exception {
    ActiveDirectoryClient adClient = new ActiveDirectoryClient(mockAdServer);
    assertNull(adClient.getUserAccountBySid(""));
  }

  @Test
  public void testInvalidSidString() throws Exception {
    ActiveDirectoryClient adClient = new ActiveDirectoryClient(mockAdServer);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("Invalid SID"));
    adClient.getUserAccountBySid("s-2-");
  }

  @Test
  public void testGetUserAccountBySid() throws Exception {
    ActiveDirectoryClient adClient = new ActiveDirectoryClient(mockAdServer);
    when(mockAdServer.getUserAccountBySid("S-1-5-21-736914693-3137354690-2813686979"))
        .thenReturn(Optional.of("MYDOMAIN"));
    when(mockAdServer.getUserAccountBySid("S-1-5-21-736914693-3137354690-2813686979-1132"))
        .thenReturn(Optional.of("USER1"));
    String actual = adClient.getUserAccountBySid("S-1-5-21-736914693-3137354690-2813686979-1132");
    assertEquals("MYDOMAIN\\USER1", actual);
  }

  @Test
  public void testGetUserAccountBySidDomainNotAvailable() throws Exception {
    ActiveDirectoryClient adClient = new ActiveDirectoryClient(mockAdServer);
    when(mockAdServer.getUserAccountBySid("S-1-5-21-736914693-3137354690-2813686979"))
        .thenReturn(Optional.empty());
    assertNull(adClient.getUserAccountBySid("S-1-5-21-736914693-3137354690-2813686979-1132"));
  }

  @Test
  public void testGetUserAccountBySidAccountNameNotAvailable() throws Exception {
    ActiveDirectoryClient adClient = new ActiveDirectoryClient(mockAdServer);
    when(mockAdServer.getUserAccountBySid("S-1-5-21-736914693-3137354690-2813686979"))
        .thenReturn(Optional.of("MYDOMAIN"));
    when(mockAdServer.getUserAccountBySid("S-1-5-21-736914693-3137354690-2813686979-1132"))
        .thenReturn(Optional.empty());
    assertNull(adClient.getUserAccountBySid("S-1-5-21-736914693-3137354690-2813686979-1132"));
  }

  @Test
  public void testFromConfigurationNoSidHost() throws IOException {
    setupConfig.initConfig(new Properties());
    assertFalse(ActiveDirectoryClient.fromConfiguration().isPresent());
  }

  @Test
  public void testFromConfiguration() throws IOException, NamingException {
    setupLadapContextAndConfig();
    assertTrue(ActiveDirectoryClient.fromConfiguration(mockContextBuilder).isPresent());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetUserEmailBySidWithLdapGlobalCatalog() throws IOException, NamingException {
    setupLadapContextAndConfig();
    ActiveDirectoryClient client =
        ActiveDirectoryClient.fromConfiguration(mockContextBuilder).get();
    NamingEnumeration<SearchResult> mockSearchResult = mock(NamingEnumeration.class);
    when(mockContext.search(
            eq(""), eq("(objectSid=S-1-5-21-736914693-3137354690-2813686979-1132)"), any()))
        .thenReturn(mockSearchResult);
    when(mockSearchResult.hasMoreElements()).thenReturn(true);
    Attribute emailAttribute = mock(Attribute.class);
    when(emailAttribute.get(0)).thenReturn("user1@mydomain.ongoogle.com");
    Attributes searchResultsAttributes = mock(Attributes.class);
    when(searchResultsAttributes.get("email")).thenReturn(emailAttribute);
    when(mockSearchResult.next())
        .thenReturn(new SearchResult("result", null, searchResultsAttributes));
    assertEquals(
        "user1@mydomain.ongoogle.com",
        client.getUserEmailBySid("S-1-5-21-736914693-3137354690-2813686979-1132"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetUserEmailBySidWithLdap() throws IOException, NamingException {
    setupLadapContextAndConfig(389);
    ActiveDirectoryClient client =
        ActiveDirectoryClient.fromConfiguration(mockContextBuilder).get();
    NamingEnumeration<SearchResult> mockSearchResult = mock(NamingEnumeration.class);
    when(mockContext.search(
            eq("DC=MYDOMAIN,DC=COM"),
            eq("(objectSid=S-1-5-21-736914693-3137354690-2813686979-1132)"),
            any()))
        .thenReturn(mockSearchResult);
    when(mockSearchResult.hasMoreElements()).thenReturn(true);
    Attribute emailAttribute = mock(Attribute.class);
    when(emailAttribute.get(0)).thenReturn("user1@mydomain.ongoogle.com");
    Attributes searchResultsAttributes = mock(Attributes.class);
    when(searchResultsAttributes.get("email")).thenReturn(emailAttribute);
    when(mockSearchResult.next())
        .thenReturn(new SearchResult("result", null, searchResultsAttributes));
    assertEquals(
        "user1@mydomain.ongoogle.com",
        client.getUserEmailBySid("S-1-5-21-736914693-3137354690-2813686979-1132"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetUserAccountBySidWithLdap() throws IOException, NamingException {
    setupLadapContextAndConfig(389);
    ActiveDirectoryClient client =
        ActiveDirectoryClient.fromConfiguration(mockContextBuilder).get();
    NamingEnumeration<SearchResult> mockSearchResultDomain = mock(NamingEnumeration.class);
    when(mockContext.search(
            eq("DC=MYDOMAIN,DC=COM"),
            eq("(objectSid=S-1-5-21-736914693-3137354690-2813686979)"),
            any()))
        .thenReturn(mockSearchResultDomain);
    when(mockSearchResultDomain.hasMoreElements()).thenReturn(true);
    Attribute nameAttribute = mock(Attribute.class);
    when(nameAttribute.get(0)).thenReturn("MYDOMAIN");
    Attributes searchResultsAttributesDomain = mock(Attributes.class);
    when(searchResultsAttributesDomain.get("name")).thenReturn(nameAttribute);
    when(mockSearchResultDomain.next())
        .thenReturn(new SearchResult("result", null, searchResultsAttributesDomain));

    NamingEnumeration<SearchResult> mockSearchResultUser = mock(NamingEnumeration.class);
    when(mockContext.search(
            eq("DC=MYDOMAIN,DC=COM"),
            eq("(objectSid=S-1-5-21-736914693-3137354690-2813686979-1132)"),
            any()))
        .thenReturn(mockSearchResultUser);
    when(mockSearchResultUser.hasMoreElements()).thenReturn(true);
    Attribute accountAttribute = mock(Attribute.class);
    when(accountAttribute.get(0)).thenReturn("GROUP1");
    Attributes searchResultsAttributesAccount = mock(Attributes.class);
    when(searchResultsAttributesAccount.get("sAMAccountName")).thenReturn(accountAttribute);
    when(mockSearchResultUser.next())
        .thenReturn(new SearchResult("result", null, searchResultsAttributesAccount));

    assertEquals(
        "MYDOMAIN\\GROUP1",
        client.getUserAccountBySid("S-1-5-21-736914693-3137354690-2813686979-1132"));
  }

  private void setupLadapContextAndConfig() throws NamingException {
    setupLadapContextAndConfig(3268);
  }

  private void setupLadapContextAndConfig(int port) throws NamingException {
    Properties properties = new Properties();
    properties.put("sidlookup.host", "10.10.10.10");
    properties.put("sidlookup.port", Integer.toString(port));
    properties.put("sidlookup.username", "username");
    properties.put("sidlookup.password", "password");
    setupConfig.initConfig(properties);
    when(mockContext.getAttributes("")).thenReturn(mockAttributes);
    Attribute dnAttribute = mock(Attribute.class);
    when(mockAttributes.get("defaultNamingContext")).thenReturn(dnAttribute);
    when(dnAttribute.get(0)).thenReturn("DC=MYDOMAIN,DC=COM");
  }
}
