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

import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sharepoint.ActiveDirectoryClient.ADServer;
import com.google.enterprise.cloudsearch.sharepoint.ActiveDirectoryClient.LdapContextBuilder;
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

/** Unit tests for {@link ActiveDirectoryClient} */
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

  @Test
  public void testDefaultNamingContextMissing() throws IOException, NamingException {
    setupLdapContextAndConfig(389);
    Attribute mockEmptyAttribute = mock(Attribute.class);
    when(mockAttributes.get("defaultNamingContext")).thenReturn(mockEmptyAttribute);
    thrown.expect(IOException.class);
    ActiveDirectoryClient.fromConfiguration(mockContextBuilder).get();
  }

  @Test
  public void testGetUserEmailByAccountNameWithLdap() throws IOException, NamingException {
    setupLdapContextAndConfig(389);
    ActiveDirectoryClient client =
        ActiveDirectoryClient.fromConfiguration(mockContextBuilder).get();
    NamingEnumeration<SearchResult> mockSearchResult = getMockNamingEnumeration();
    when(mockContext.search(
            eq("DC=MYDOMAIN,DC=COM"),
            eq("(&(objectCategory=person)(objectClass=user)(sAMAccountName=user1))"),
            any()))
        .thenReturn(mockSearchResult);
    when(mockSearchResult.hasMoreElements()).thenReturn(true);
    Attribute emailAttribute = mock(Attribute.class);
    when(emailAttribute.get(0)).thenReturn("user1@mydomain.ongoogle.com");
    Attributes searchResultsAttributes = mock(Attributes.class);
    when(searchResultsAttributes.get("mail")).thenReturn(emailAttribute);
    when(mockSearchResult.next())
        .thenReturn(new SearchResult("result", null, searchResultsAttributes));
    ActiveDirectoryPrincipal principal = ActiveDirectoryPrincipal.parse("MYDOMAIN\\user1");
    assertEquals("user1@mydomain.ongoogle.com", client.getUserEmailByPrincipal(principal));
  }

  @Test
  public void testGetUserEmailByAccountNameEmailIsEmpty() throws IOException, NamingException {
    setupLdapContextAndConfig(389);
    ActiveDirectoryClient client =
        ActiveDirectoryClient.fromConfiguration(mockContextBuilder).get();
    NamingEnumeration<SearchResult> mockSearchResult = getMockNamingEnumeration();
    when(mockContext.search(
            eq("DC=MYDOMAIN,DC=COM"),
            eq("(&(objectCategory=person)(objectClass=user)(sAMAccountName=user1))"),
            any()))
        .thenReturn(mockSearchResult);
    when(mockSearchResult.hasMoreElements()).thenReturn(true);
    Attribute emailAttribute = mock(Attribute.class);
    when(emailAttribute.get(0)).thenReturn("");
    Attributes searchResultsAttributes = mock(Attributes.class);
    when(searchResultsAttributes.get("mail")).thenReturn(emailAttribute);
    when(mockSearchResult.next())
        .thenReturn(new SearchResult("result", null, searchResultsAttributes));
    ActiveDirectoryPrincipal principal = ActiveDirectoryPrincipal.parse("MYDOMAIN\\user1");
    assertNull(client.getUserEmailByPrincipal(principal));
  }

  @Test
  public void testGetUserEmailByAccountNameWithUpn() throws IOException, NamingException {
    setupLdapContextAndConfig(389);
    ActiveDirectoryClient client =
        ActiveDirectoryClient.fromConfiguration(mockContextBuilder).get();
    NamingEnumeration<SearchResult> mockSearchResult = getMockNamingEnumeration();
    when(mockContext.search(
            eq("DC=MYDOMAIN,DC=COM"),
            eq("(&(objectCategory=person)(objectClass=user)(userPrincipalName=u1@mydomain.com))"),
            any()))
        .thenReturn(mockSearchResult);
    when(mockSearchResult.hasMoreElements()).thenReturn(true);
    Attribute emailAttribute = mock(Attribute.class);
    when(emailAttribute.get(0)).thenReturn("user1@mydomain.ongoogle.com");
    Attributes searchResultsAttributes = mock(Attributes.class);
    when(searchResultsAttributes.get("mail")).thenReturn(emailAttribute);
    when(mockSearchResult.next())
        .thenReturn(new SearchResult("result", null, searchResultsAttributes));
    ActiveDirectoryPrincipal principal = ActiveDirectoryPrincipal.parse("u1@mydomain.com");
    assertEquals("user1@mydomain.ongoogle.com", client.getUserEmailByPrincipal(principal));
  }

  @Test
  public void testGetUserEmailByAccountNameWithOtherDomain() throws IOException, NamingException {
    setupLdapContextAndConfig(389);
    ActiveDirectoryClient client =
        ActiveDirectoryClient.fromConfiguration(mockContextBuilder).get();
    ActiveDirectoryPrincipal principal = ActiveDirectoryPrincipal.parse("OTHERDOMAIN\\user1");
    assertNull(client.getUserEmailByPrincipal(principal));
  }

  @Test
  public void testGetUserEmailByAccountNameWithOtherDomainDnsRoot()
      throws IOException, NamingException {
    setupLdapContextAndConfig(389);
    ActiveDirectoryClient client =
        ActiveDirectoryClient.fromConfiguration(mockContextBuilder).get();
    ActiveDirectoryPrincipal principal = ActiveDirectoryPrincipal.parse("u1@myotherdomain.com");
    assertNull(client.getUserEmailByPrincipal(principal));
  }

  @Test
  public void testGetUserEmailByAccountNameEmptyResult() throws IOException, NamingException {
    setupLdapContextAndConfig(389);
    ActiveDirectoryClient client =
        ActiveDirectoryClient.fromConfiguration(mockContextBuilder).get();
    NamingEnumeration<SearchResult> mockSearchResult = getMockNamingEnumeration();
    when(mockContext.search(
            eq("DC=MYDOMAIN,DC=COM"),
            eq("(&(objectCategory=person)(objectClass=user)(sAMAccountName=user1))"),
            any()))
        .thenReturn(mockSearchResult);
    when(mockSearchResult.hasMoreElements()).thenReturn(false);
    ActiveDirectoryPrincipal principal = ActiveDirectoryPrincipal.parse("MYDOMAIN\\user1");
    assertNull(client.getUserEmailByPrincipal(principal));
  }

  @Test
  public void testGetUserAccountBySidWithLdap() throws IOException, NamingException {
    setupLdapContextAndConfig(389);
    ActiveDirectoryClient client =
        ActiveDirectoryClient.fromConfiguration(mockContextBuilder).get();
    NamingEnumeration<SearchResult> mockSearchResultDomain = getMockNamingEnumeration();
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

    NamingEnumeration<SearchResult> mockSearchResultUser = getMockNamingEnumeration();
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

  @Test
  public void testGetUserAccountBySidNotFound() throws IOException, NamingException {
    setupLdapContextAndConfig(389);
    ActiveDirectoryClient client =
        ActiveDirectoryClient.fromConfiguration(mockContextBuilder).get();
    NamingEnumeration<SearchResult> mockSearchResultDomain = getMockNamingEnumeration();
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

    NamingEnumeration<SearchResult> mockSearchResultUser = getMockNamingEnumeration();
    when(mockContext.search(
            eq("DC=MYDOMAIN,DC=COM"),
            eq("(objectSid=S-1-5-21-736914693-3137354690-2813686979-1132)"),
            any()))
        .thenReturn(mockSearchResultUser);
    when(mockSearchResultUser.hasMoreElements()).thenReturn(false);
    assertNull(client.getUserAccountBySid("S-1-5-21-736914693-3137354690-2813686979-1132"));
  }

  @Test
  public void testGetUserAccountBySidWithEmptyName() throws IOException, NamingException {
    setupLdapContextAndConfig(389);
    ActiveDirectoryClient client =
        ActiveDirectoryClient.fromConfiguration(mockContextBuilder).get();
    NamingEnumeration<SearchResult> mockSearchResultDomain = getMockNamingEnumeration();
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

    NamingEnumeration<SearchResult> mockSearchResultUser = getMockNamingEnumeration();
    when(mockContext.search(
            eq("DC=MYDOMAIN,DC=COM"),
            eq("(objectSid=S-1-5-21-736914693-3137354690-2813686979-1132)"),
            any()))
        .thenReturn(mockSearchResultUser);
    when(mockSearchResultUser.hasMoreElements()).thenReturn(true);
    Attribute accountAttribute = mock(Attribute.class);
    when(accountAttribute.get(0)).thenReturn("");
    Attributes searchResultsAttributesAccount = mock(Attributes.class);
    when(searchResultsAttributesAccount.get("sAMAccountName")).thenReturn(accountAttribute);
    when(mockSearchResultUser.next())
        .thenReturn(new SearchResult("result", null, searchResultsAttributesAccount));

    assertNull(client.getUserAccountBySid("S-1-5-21-736914693-3137354690-2813686979-1132"));
  }

  @Test
  public void testGetUserAccountBySidNamingException() throws IOException, NamingException {
    setupLdapContextAndConfig(389);
    ActiveDirectoryClient client =
        ActiveDirectoryClient.fromConfiguration(mockContextBuilder).get();
    when(mockContext.search(
            eq("DC=MYDOMAIN,DC=COM"),
            eq("(objectSid=S-1-5-21-736914693-3137354690-2813686979)"),
            any()))
        .thenThrow(new NamingException("error in ldap query"));
    thrown.expect(IOException.class);
    client.getUserAccountBySid("S-1-5-21-736914693-3137354690-2813686979-1132");
  }

  private void setupLadapContextAndConfig() throws NamingException {
    setupLdapContextAndConfig(3268);
  }

  private void setupLdapContextAndConfig(int port) throws NamingException {
    Properties properties = new Properties();
    properties.put("adLookup.host", "10.10.10.10");
    properties.put("adLookup.port", Integer.toString(port));
    properties.put("adLookup.username", "username");
    properties.put("adLookup.password", "password");
    setupConfig.initConfig(properties);
    when(mockContext.getAttributes("")).thenReturn(mockAttributes);
    Attribute dnAttribute = mock(Attribute.class);
    when(mockAttributes.get("defaultNamingContext")).thenReturn(dnAttribute);
    when(dnAttribute.get(0)).thenReturn("DC=MYDOMAIN,DC=COM");

    Attribute configurationContextAttribute = mock(Attribute.class);
    when(mockAttributes.get("configurationNamingContext"))
        .thenReturn(configurationContextAttribute);
    when(configurationContextAttribute.get(0)).thenReturn("CN=Configuration,DC=MYDOMAIN,DC=COM");
    NamingEnumeration<SearchResult> mockSearchResult = getMockNamingEnumeration();
    when(mockContext.search(
            eq("CN=Configuration,DC=MYDOMAIN,DC=COM"), eq("(ncName=DC=MYDOMAIN,DC=COM)"), any()))
        .thenReturn(mockSearchResult);
    when(mockSearchResult.hasMore()).thenReturn(true);
    Attribute dnsRoot = mock(Attribute.class);
    when(dnsRoot.get(0)).thenReturn("mydomain.com");
    Attribute nETBIOSName = mock(Attribute.class);
    when(nETBIOSName.get(0)).thenReturn("mydomain");
    Attributes searchResultsAttributes = mock(Attributes.class);
    when(searchResultsAttributes.get("dnsRoot")).thenReturn(dnsRoot);
    when(searchResultsAttributes.get("nETBIOSName")).thenReturn(nETBIOSName);
    when(mockSearchResult.next())
        .thenReturn(new SearchResult("result", null, searchResultsAttributes));
  }

  @SuppressWarnings("unchecked")
  private NamingEnumeration<SearchResult> getMockNamingEnumeration() {
    return mock(NamingEnumeration.class);
  }
}
