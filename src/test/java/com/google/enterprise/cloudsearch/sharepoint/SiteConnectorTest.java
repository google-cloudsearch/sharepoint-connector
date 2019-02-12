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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.cloudidentity.v1.model.EntityKey;
import com.google.api.services.cloudidentity.v1.model.Membership;
import com.google.api.services.cloudidentity.v1.model.MembershipRole;
import com.google.api.services.cloudsearch.v1.model.Principal;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.enterprise.cloudsearch.sdk.identity.IdentityGroup;
import com.google.enterprise.cloudsearch.sdk.identity.IdentitySourceConfiguration;
import com.google.enterprise.cloudsearch.sdk.identity.RepositoryContext;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
import com.google.enterprise.cloudsearch.sharepoint.SharePointConfiguration.SharePointDeploymentType;
import com.google.enterprise.cloudsearch.sharepoint.SiteConnector.SPBasePermissions;
import com.microsoft.schemas.sharepoint.soap.ACL;
import com.microsoft.schemas.sharepoint.soap.Permission;
import com.microsoft.schemas.sharepoint.soap.PermissionsForACL;
import com.microsoft.schemas.sharepoint.soap.Scopes.Scope;
import com.microsoft.schemas.sharepoint.soap.Site;
import com.microsoft.schemas.sharepoint.soap.SiteDataSoap;
import com.microsoft.schemas.sharepoint.soap.TrueFalseType;
import com.microsoft.schemas.sharepoint.soap.UserDescription;
import com.microsoft.schemas.sharepoint.soap.Users;
import com.microsoft.schemas.sharepoint.soap.VirtualServer;
import com.microsoft.schemas.sharepoint.soap.Web;
import com.microsoft.schemas.sharepoint.soap.directory.GetUserCollectionFromSiteResponse;
import com.microsoft.schemas.sharepoint.soap.directory.User;
import com.microsoft.schemas.sharepoint.soap.directory.UserGroupSoap;
import com.microsoft.schemas.sharepoint.soap.people.ArrayOfPrincipalInfo;
import com.microsoft.schemas.sharepoint.soap.people.ArrayOfString;
import com.microsoft.schemas.sharepoint.soap.people.PeopleSoap;
import com.microsoft.schemas.sharepoint.soap.people.PrincipalInfo;
import com.microsoft.schemas.sharepoint.soap.people.SPPrincipalType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Unit tests for {@link SiteConnector} */
@RunWith(MockitoJUnitRunner.class)
public class SiteConnectorTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Mock SiteDataClient siteDataClient;
  @Mock SiteDataSoap siteDataSoap;
  @Mock PeopleSoap peopleSoap;
  @Mock UserGroupSoap userGroupSoap;
  @Mock ActiveDirectoryClient adLookupClient;

  @Test
  public void testBuilder() {
    new SiteConnector.Builder("http://sp.com", "http://sp.com")
        .setSiteDataClient(siteDataClient)
        .setPeople(peopleSoap)
        .setUserGroup(userGroupSoap)
        .build();
  }

  @Test
  public void testBuilderEmptySite() {
    thrown.expect(IllegalArgumentException.class);
    new SiteConnector.Builder("", "http://sp.com")
        .setSiteDataClient(siteDataClient)
        .setPeople(peopleSoap)
        .setUserGroup(userGroupSoap)
        .build();
  }

  @Test
  public void testBuilderEmptyWeb() {
    thrown.expect(IllegalArgumentException.class);
    new SiteConnector.Builder("http://sp.com", "")
        .setSiteDataClient(siteDataClient)
        .setPeople(peopleSoap)
        .setUserGroup(userGroupSoap)
        .build();
  }

  @Test
  public void testBuilderNullSiteDataClient() {
    thrown.expect(NullPointerException.class);
    new SiteConnector.Builder("http://sp.com", "http://sp.com")
        .setSiteDataClient(null)
        .setPeople(peopleSoap)
        .setUserGroup(userGroupSoap)
        .build();
  }

  @Test
  public void testBuilderPeople() {
    thrown.expect(NullPointerException.class);
    new SiteConnector.Builder("http://sp.com", "http://sp.com")
        .setSiteDataClient(siteDataClient)
        .setPeople(null)
        .setUserGroup(userGroupSoap)
        .build();
  }

  @Test
  public void testBuilderUserGroup() {
    thrown.expect(NullPointerException.class);
    new SiteConnector.Builder("http://sp.com", "http://sp.com")
        .setSiteDataClient(siteDataClient)
        .setPeople(peopleSoap)
        .setUserGroup(null)
        .build();
  }

  @Test
  public void testGetWebAcls() throws IOException {
    // GDC_PSL\\spuser1
    Permission permSpUser1 = createPermission(2, SiteConnector.LIST_ITEM_MASK);
    Permission notEnough = createPermission(100, SPBasePermissions.EMPTYMASK);
    // TeamSite Owners
    Permission permLocalGroup = createPermission(3, SPBasePermissions.FULLMASK);
    Web web = new Web();
    web.setACL(new ACL());
    web.getACL().setPermissions(new PermissionsForACL());
    web.getACL()
        .getPermissions()
        .getPermission()
        .addAll(Arrays.asList(permSpUser1, notEnough, permLocalGroup));
    setupGetContentSite(loadTestResponse("sites-SiteCollection-sc.xml"));
    SiteConnector sc =
        new SiteConnector.Builder(
                "http://localhost:1/sites/SiteCollection",
                "http://localhost:1/sites/SiteCollection")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .setReferenceIdentitySourceConfiguration(
                ImmutableMap.of(
                    "GDC-PSL", new IdentitySourceConfiguration.Builder("idSourceGdcPsl").build()))
            .build();
    Principal spUser1 = Acl.getUserPrincipal("GDC-PSL\\spuser1", "idSourceGdcPsl");
    Principal teamSiteOwners =
        Acl.getGroupPrincipal(
            SiteConnector.encodeSharePointLocalGroupName(
                "http://localhost:1/sites/SiteCollection", "TeamSite Owners"));
    assertEquals(Arrays.asList(spUser1, teamSiteOwners), sc.getWebAcls(web));
  }

  @Test
  public void testGetListAcls() throws IOException {
    // GDC_PSL\\spuser1
    Permission permSpUser1 = createPermission(2, SiteConnector.LIST_ITEM_MASK);
    Permission notEnough = createPermission(100, SPBasePermissions.EMPTYMASK);
    // TeamSite Owners
    Permission permLocalGroup = createPermission(3, SPBasePermissions.FULLMASK);
    Permission notAvailable = createPermission(101, SPBasePermissions.FULLMASK);
    // GDC-PSL\\administrator using UserGroup
    Permission adminViaUserGroup = createPermission(200, SPBasePermissions.FULLMASK);
    // group300@gdc-psl.com using UserGroup
    Permission group300ViaUserGroup = createPermission(300, SPBasePermissions.FULLMASK);
    com.microsoft.schemas.sharepoint.soap.List list =
        new com.microsoft.schemas.sharepoint.soap.List();
    list.setACL(new ACL());
    list.getACL().setPermissions(new PermissionsForACL());
    list.getACL()
        .getPermissions()
        .getPermission()
        .addAll(
            Arrays.asList(
                permSpUser1,
                notEnough,
                permLocalGroup,
                notAvailable,
                adminViaUserGroup,
                group300ViaUserGroup));
    com.microsoft.schemas.sharepoint.soap.directory.Users users =
        new com.microsoft.schemas.sharepoint.soap.directory.Users();
    users
        .getUser()
        .add(
            createUserGroupUser(
                200,
                "GDC-PSL\\administrator",
                "S-1-5-21-7369146",
                "Administrator",
                "admin@domain.com",
                false,
                true));
    users
        .getUser()
        .add(
            createUserGroupUser(
                300,
                "c:0-.t|adfsv2|group300@gdc-psl.com",
                "S-1-5-21-7369300",
                "group300@gdc-psl.com",
                "Group300@domain.com",
                true,
                false));
    GetUserCollectionFromSiteResponse.GetUserCollectionFromSiteResult result =
        new GetUserCollectionFromSiteResponse.GetUserCollectionFromSiteResult();
    GetUserCollectionFromSiteResponse.GetUserCollectionFromSiteResult.GetUserCollectionFromSite
        siteUsers =
            new GetUserCollectionFromSiteResponse.GetUserCollectionFromSiteResult
                .GetUserCollectionFromSite();
    siteUsers.setUsers(users);
    result.setGetUserCollectionFromSite(siteUsers);
    when(userGroupSoap.getUserCollectionFromSite()).thenReturn(result);
    setupGetContentSite(loadTestResponse("sites-SiteCollection-sc.xml"));
    SiteConnector sc =
        new SiteConnector.Builder(
                "http://localhost:1/sites/SiteCollection",
                "http://localhost:1/sites/SiteCollection")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .setReferenceIdentitySourceConfiguration(
                ImmutableMap.of(
                    "GDC-PSL",
                    new IdentitySourceConfiguration.Builder("idSourceGdcPsl").build(),
                    "gdc-psl.com",
                    new IdentitySourceConfiguration.Builder("idSourceGdcPsl").build()))
            .build();
    Principal spUser1 = Acl.getUserPrincipal("GDC-PSL\\spuser1", "idSourceGdcPsl");
    Principal teamSiteOwners =
        Acl.getGroupPrincipal(
            SiteConnector.encodeSharePointLocalGroupName(
                "http://localhost:1/sites/SiteCollection", "TeamSite Owners"));
    Principal admin = Acl.getUserPrincipal("GDC-PSL\\administrator", "idSourceGdcPsl");
    Principal group300 = Acl.getGroupPrincipal("group300@gdc-psl.com", "idSourceGdcPsl");
    assertEquals(Arrays.asList(spUser1, teamSiteOwners, admin, group300), sc.getListAcl(list));
  }

  @Test
  public void testScopeAcls() throws IOException {
    // GDC_PSL\\spuser1
    Permission permSpUser1 = createPermission(2, SiteConnector.LIST_ITEM_MASK);
    Permission notEnough = createPermission(100, SPBasePermissions.EMPTYMASK);
    // TeamSite Owners
    Permission permLocalGroup = createPermission(3, SPBasePermissions.FULLMASK);
    // GSA-CONNECTORS\\User1
    Permission domainNotMapped = createPermission(14, SiteConnector.LIST_ITEM_MASK);
    // Group with no domain roleprovider:super
    Permission groupNoDomain = createPermission(19, SiteConnector.LIST_ITEM_MASK);
    Scope scope = new Scope();
    scope
        .getPermission()
        .addAll(
            Arrays.asList(permSpUser1, notEnough, permLocalGroup, domainNotMapped, groupNoDomain));
    setupGetContentSite(loadTestResponse("sites-SiteCollection-sc.xml"));
    SiteConnector sc =
        new SiteConnector.Builder(
                "http://localhost:1/sites/SiteCollection",
                "http://localhost:1/sites/SiteCollection")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .setReferenceIdentitySourceConfiguration(
                ImmutableMap.of(
                    "GDC-PSL",
                    new IdentitySourceConfiguration.Builder("idSourceGdcPsl").build(),
                    SiteConnector.DEFAULT_REFERENCE_IDENTITY_SOURCE_NAME,
                    new IdentitySourceConfiguration.Builder("idSourceDefault").build()))
            .build();
    Principal spUser1 = Acl.getUserPrincipal("GDC-PSL\\spuser1", "idSourceGdcPsl");
    Principal teamSiteOwners =
        Acl.getGroupPrincipal("[http://localhost:1/sites/SiteCollection]TeamSite Owners");
    Principal principalGroupNoDomain =
        Acl.getGroupPrincipal("roleprovider:super", "idSourceDefault");
    assertEquals(
        Arrays.asList(spUser1, teamSiteOwners, principalGroupNoDomain), sc.getScopeAcl(scope));
  }

  @Test
  public void testScopeAclsSharePointOnline() throws IOException {
    // i:0#.f|membershipprovider|user2007
    Permission permuser2007 = createPermission(15, SiteConnector.LIST_ITEM_MASK);
    Permission notEnough = createPermission(100, SPBasePermissions.EMPTYMASK);
    // TeamSite Owners
    Permission permLocalGroup = createPermission(3, SPBasePermissions.FULLMASK);
    // GSA-CONNECTORS\\User1
    Permission domainNotMapped = createPermission(14, SiteConnector.LIST_ITEM_MASK);
    // Group with no domain roleprovider:super
    Permission groupNoDomain = createPermission(19, SiteConnector.LIST_ITEM_MASK);
    Scope scope = new Scope();
    scope
        .getPermission()
        .addAll(
            Arrays.asList(permuser2007, notEnough, permLocalGroup, domainNotMapped, groupNoDomain));
    setupGetContentSite(loadTestResponse("sites-SiteCollection-sc.xml"));
    SiteConnector sc =
        new SiteConnector.Builder(
                "http://localhost:1/sites/SiteCollection",
                "http://localhost:1/sites/SiteCollection")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .setSharePointDeploymentType(SharePointDeploymentType.ONLINE)
            .setReferenceIdentitySourceConfiguration(
                ImmutableMap.of(
                    SiteConnector.DEFAULT_REFERENCE_IDENTITY_SOURCE_NAME,
                    new IdentitySourceConfiguration.Builder("idSourceDefault").build()))
            .build();
    Principal user2007 = Acl.getUserPrincipal("user2007", "idSourceDefault");
    Principal teamSiteOwners =
        Acl.getGroupPrincipal("[http://localhost:1/sites/SiteCollection]TeamSite Owners");
    Principal principalGroupNoDomain = Acl.getGroupPrincipal("super", "idSourceDefault");
    assertEquals(
        Arrays.asList(user2007, teamSiteOwners, principalGroupNoDomain), sc.getScopeAcl(scope));
  }

  @Test
  public void testScopeAclsStripDomain() throws IOException {
    // GDC_PSL\\spuser1
    Permission permSpUser1 = createPermission(2, SiteConnector.LIST_ITEM_MASK);
    Permission notEnough = createPermission(100, SPBasePermissions.EMPTYMASK);
    // TeamSite Owners
    Permission permLocalGroup = createPermission(3, SPBasePermissions.FULLMASK);
    Scope scope = new Scope();
    scope.getPermission().addAll(Arrays.asList(permSpUser1, notEnough, permLocalGroup));
    setupGetContentSite(loadTestResponse("sites-SiteCollection-sc.xml"));
    SiteConnector sc =
        new SiteConnector.Builder(
                "http://localhost:1/sites/SiteCollection",
                "http://localhost:1/sites/SiteCollection")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .setStripDomainInUserPrincipals(true)
            .setReferenceIdentitySourceConfiguration(
                ImmutableMap.of(
                    "GDC-PSL", new IdentitySourceConfiguration.Builder("idSourceGdcPsl").build()))
            .build();
    Principal spUser1 = Acl.getUserPrincipal("spuser1", "idSourceGdcPsl");
    Principal teamSiteOwners =
        Acl.getGroupPrincipal("[http://localhost:1/sites/SiteCollection]TeamSite Owners");
    assertEquals(Arrays.asList(spUser1, teamSiteOwners), sc.getScopeAcl(scope));
  }

  @Test
  public void testGetSiteCollectionAdmins() {
    SiteConnector sc =
        new SiteConnector.Builder(
                "http://localhost:1/sites/SiteCollection",
                "http://localhost:1/sites/SiteCollection")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .setReferenceIdentitySourceConfiguration(
                ImmutableMap.of(
                    "DOMAIN", new IdentitySourceConfiguration.Builder("idSourceDOMAIN").build()))
            .build();
    AtomicInteger userIds = new AtomicInteger();
    UserDescription user1 = createUser(userIds.incrementAndGet(), "DOMAIN\\user1Admin", "User1");
    user1.setIsSiteAdmin(TrueFalseType.TRUE);
    UserDescription group1 =
        createGroup(userIds.incrementAndGet(), "DOMAIN\\group1Admin", "Group 1");
    group1.setIsSiteAdmin(TrueFalseType.TRUE);
    UserDescription userRegular =
        createUser(userIds.incrementAndGet(), "DOMAIN\\userRegular", "userRegular");
    userRegular.setIsSiteAdmin(TrueFalseType.FALSE);
    UserDescription userInvalidLogin =
        createUser(userIds.incrementAndGet(), "i:0:invalid", "invalid user");
    userInvalidLogin.setIsSiteAdmin(TrueFalseType.TRUE);
    Web web = new Web();
    web.setUsers(new Users());
    web.getUsers().getUser().addAll(Arrays.asList(user1, group1, userRegular, userInvalidLogin));
    List<Principal> expected =
        Arrays.asList(
            Acl.getUserPrincipal("DOMAIN\\user1Admin", "idSourceDOMAIN"),
            Acl.getGroupPrincipal("DOMAIN\\group1Admin", "idSourceDOMAIN"));
    assertEquals(expected, sc.getSiteCollectionAdmins(web));
  }

  @Test
  public void testGetSharePointGroups() throws IOException {
    String siteUrl = "http://localhost:1/sites/SiteCollection";
    SiteConnector sc =
        new SiteConnector.Builder(siteUrl, siteUrl)
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .setActiveDirectoryClient(adLookupClient)
            .build();
    setupGetContentSite(loadTestResponse("sites-SiteCollection-sc.xml"));
    ActiveDirectoryPrincipal spUser2 = ActiveDirectoryPrincipal.parse("GDC-PSL\\spuser2");
    when(adLookupClient.getUserEmailByPrincipal(spUser2)).thenReturn("spuser2@mygoogledomain.com");
    EntityKey spuser2Key = new EntityKey().setId("spuser2@mygoogledomain.com");
    Membership spuser2Membership =
        new Membership()
            .setPreferredMemberKey(spuser2Key)
            .setRoles(ImmutableList.of(new MembershipRole().setName("MEMBER")));
    RepositoryContext context = mock(RepositoryContext.class);
    RepositoryContext referenceContext = mock(RepositoryContext.class);
    when(context.getRepositoryContextForReferenceIdentitySource("BUILTIN"))
        .thenReturn(Optional.of(referenceContext));
    EntityKey builtinUsersKey =
        new EntityKey().setId("BUILTIN\\users").setNamespace("idSourceBuiltin");
    Membership builtinUsersMembership =
        new Membership()
            .setPreferredMemberKey(builtinUsersKey)
            .setRoles(ImmutableList.of(new MembershipRole().setName("MEMBER")));
    when(referenceContext.buildEntityKeyForGroup("BUILTIN\\users"))
        .thenReturn(builtinUsersKey);
    EntityKey adminKey = new EntityKey().setId("admin@mygoogledomain.com");
    Membership adminMembership =
        new Membership()
            .setPreferredMemberKey(adminKey)
            .setRoles(ImmutableList.of(new MembershipRole().setName("MEMBER")));
    IdentityGroup teamOwners =
        setupIdentityGroupOnContext(
            context, siteUrl, "TeamSite Owners", ImmutableSet.of(adminMembership));
    IdentityGroup teamMembers =
        setupIdentityGroupOnContext(
            context,
            siteUrl,
            "TeamSite Members",
            ImmutableSet.of(builtinUsersMembership, spuser2Membership));
    IdentityGroup teamVisitors =
        setupIdentityGroupOnContext(context, siteUrl, "TeamSite Visitors", ImmutableSet.of());
    assertThat(
        sc.getSharePointGroups(context),
        equalTo(ImmutableList.of(teamOwners, teamMembers, teamVisitors)));
  }

  @Test
  public void testGetSharePointGroupsSharePointOnline() throws IOException {
    String siteUrl = "http://localhost:1/sites/SiteCollection";
    SiteConnector sc =
        new SiteConnector.Builder(siteUrl, siteUrl)
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .setSharePointDeploymentType(SharePointDeploymentType.ONLINE)
            .build();
    setupGetContentSite(
        loadTestResponse("sites-SiteCollection-sc.xml")
            .replace(
                "LoginName=\"GDC-PSL\\administrator\"",
                "LoginName=\"i:0#.f|membershipprovider|administrator\"")
            .replace(
                "LoginName=\"GDC-PSL\\spuser2\" Email=\"\"",
                "LoginName=\"i:0#.f|membershipprovider|spuser2\" "
                    + "Email=\"spuser2@mygoogledomain.com\"")
            .replace(
                "LoginName=\"BUILTIN\\users\"", "LoginName=\"c:0t.c|tenant|o356TenantGroup\""));
    EntityKey spuser2Key = new EntityKey().setId("spuser2@mygoogledomain.com");
    Membership spuser2Membership =
        new Membership()
            .setPreferredMemberKey(spuser2Key)
            .setRoles(ImmutableList.of(new MembershipRole().setName("MEMBER")));
    RepositoryContext context = mock(RepositoryContext.class);
    RepositoryContext referenceContext = mock(RepositoryContext.class);
    when(context.getRepositoryContextForReferenceIdentitySource(
            SiteConnector.DEFAULT_REFERENCE_IDENTITY_SOURCE_NAME))
        .thenReturn(Optional.of(referenceContext));
    EntityKey o356TenantGroupKey =
        new EntityKey().setId("o356TenantGroup").setNamespace("identitysources/idsourceo365");
    Membership o356TenantGroupMembership =
        new Membership()
            .setPreferredMemberKey(o356TenantGroupKey)
            .setRoles(ImmutableList.of(new MembershipRole().setName("MEMBER")));
    when(referenceContext.buildEntityKeyForGroup("o356TenantGroup")).thenReturn(o356TenantGroupKey);

    EntityKey adminKey = new EntityKey().setId("admin@mygoogledomain.com");
    Membership adminMembership =
        new Membership()
            .setPreferredMemberKey(adminKey)
            .setRoles(ImmutableList.of(new MembershipRole().setName("MEMBER")));
    IdentityGroup teamOwners =
        setupIdentityGroupOnContext(
            context, siteUrl, "TeamSite Owners", ImmutableSet.of(adminMembership));
    IdentityGroup teamMembers =
        setupIdentityGroupOnContext(
            context,
            siteUrl,
            "TeamSite Members",
            ImmutableSet.of(spuser2Membership, o356TenantGroupMembership));
    IdentityGroup teamVisitors =
        setupIdentityGroupOnContext(context, siteUrl, "TeamSite Visitors", ImmutableSet.of());
    assertThat(
        sc.getSharePointGroups(context),
        equalTo(ImmutableList.of(teamOwners, teamMembers, teamVisitors)));
  }

  @Test
  public void testWebAppPolicyAcl() throws IOException {
    VirtualServer vs =
        SiteDataClient.jaxbParse(loadTestResponse("vs.xml"), VirtualServer.class, false);
    ImmutableList<String> usersToResolve =
        new ImmutableList.Builder<String>()
            .add("NT AUTHORITY\\LOCAL SERVICE")
            .add("GDC-PSL\\spuser1")
            .add("GDC-PSL\\Administrator")
            .add("GDC-PSL\\Unknown")
            .build();
    ArrayOfString aos = new ArrayOfString();
    aos.getString().addAll(usersToResolve);
    PrincipalInfo localServiceInfo =
        createPrincipalInfo(
            "NT AUTHORITY\\LOCAL SERVICE",
            "NT AUTHORITY\\LOCAL SERVICE",
            SPPrincipalType.SECURITY_GROUP);
    PrincipalInfo spUser1Info =
        createPrincipalInfo("GDC-PSL\\spuser1", "spuser1", SPPrincipalType.USER);
    PrincipalInfo adminInfo =
        createPrincipalInfo("GDC-PSL\\Administrator", "dministrator", SPPrincipalType.USER);
    PrincipalInfo unknownInfo =
        createPrincipalInfo("GDC-PSL\\Unknown", "dministrator", SPPrincipalType.USER);
    unknownInfo.setIsResolved(false);
    ArrayOfPrincipalInfo resolveInfo = new ArrayOfPrincipalInfo();
    resolveInfo
        .getPrincipalInfo()
        .addAll(Arrays.asList(localServiceInfo, spUser1Info, adminInfo, unknownInfo));
    when(peopleSoap.resolvePrincipals(any(ArrayOfString.class), eq(SPPrincipalType.ALL), eq(false)))
        .thenReturn(resolveInfo);
    ImmutableMap<String, IdentitySourceConfiguration> referenceIdentitySources =
        new ImmutableMap.Builder<String, IdentitySourceConfiguration>()
            .put("NT AUTHORITY", new IdentitySourceConfiguration.Builder("idSourceNT").build())
            .put("GDC-PSL", new IdentitySourceConfiguration.Builder("idSourceGdcPsl").build())
            .build();
    SiteConnector sc =
        new SiteConnector.Builder("http://sp.com", "http://sp.com")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .setReferenceIdentitySourceConfiguration(referenceIdentitySources)
            .build();
    Acl expected =
        new Acl.Builder()
            .setDeniedReaders(
                Collections.singletonList(
                    Acl.getUserPrincipal("GDC-PSL\\spuser1", "idSourceGdcPsl")))
            .setReaders(
                Arrays.asList(
                    Acl.getGroupPrincipal("NT AUTHORITY\\LOCAL SERVICE", "idSourceNT"),
                    Acl.getUserPrincipal("GDC-PSL\\Administrator", "idSourceGdcPsl")))
            .build();
    assertEquals(expected, sc.getWebApplicationPolicyAcl(vs));
  }

  @Test
  public void testEncodeDocId() {
    SiteConnector sc =
        new SiteConnector.Builder("http://sp.com", "http://sp.com/web/subsite")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    assertEquals("http://sp.com/web/subsite", sc.encodeDocId("http://sp.com/web/subsite"));
    assertEquals("http://sp.com/web/subsite/Folder", sc.encodeDocId("/web/subsite/Folder"));
    assertEquals("http://sp.com/anotherWeb", sc.encodeDocId("/anotherWeb"));
    assertEquals("http://sp.com/web/subsite/lists/Tasks", sc.encodeDocId("lists/Tasks"));
  }

  @Test
  public void testGetParentUrl() {
    SiteConnector sc =
        new SiteConnector.Builder("http://sp.com", "http://sp.com/web/subsite")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    assertEquals("http://sp.com/web", sc.getWebParentUrl());
  }

  @Test
  public void testGetParentUrlOnRoot() {
    SiteConnector sc =
        new SiteConnector.Builder("http://sp.com", "http://sp.com")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    thrown.expect(IllegalStateException.class);
    sc.getWebParentUrl();
  }

  private Permission createPermission(int memberId, long mask) {
    Permission perm = new Permission();
    perm.setMemberid(memberId);
    perm.setMask(BigInteger.valueOf(mask));
    return perm;
  }

  private void setupGetContentSite(String xml) throws IOException {
    Site site = SiteDataClient.jaxbParse(xml, Site.class, false);
    when(siteDataClient.getContentSite()).thenReturn(site);
  }

  private UserDescription createUser(int id, String login, String name) {
    return createUserDescription(id, login, name, TrueFalseType.FALSE);
  }

  private UserDescription createGroup(int id, String login, String name) {
    return createUserDescription(id, login, name, TrueFalseType.TRUE);
  }

  private IdentityGroup setupIdentityGroupOnContext(
      RepositoryContext context, String siteUrl, String groupName, Set<Membership> members) {
    String groupId = SiteConnector.encodeSharePointLocalGroupName(siteUrl, groupName);
    IdentityGroup toReturn =
        new IdentityGroup.Builder()
            .setGroupIdentity(groupId)
            .setGroupKey(new EntityKey().setNamespace("idSource1").setId(groupId))
            .setMembers(members)
            .build();
    doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Supplier<Set<Membership>> input =
                  (Supplier<Set<Membership>>) invocation.getArgument(1);
              assertEquals(members, input.get());
              return toReturn;
            })
        .when(context)
        .buildIdentityGroup(eq(groupId), any());
    return toReturn;
  }

  private UserDescription createUserDescription(
      int id, String login, String name, TrueFalseType isGroup) {
    UserDescription user = new UserDescription();
    user.setLoginName(login);
    user.setID(id);
    user.setName(name);
    user.setIsDomainGroup(isGroup);
    return user;
  }

  private static String loadTestResponse(String fileName) {
    try {
      return loadResourceAsString("spresponses/" + fileName);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static String loadResourceAsString(String resource) throws IOException {
    return readInputStreamToString(SiteConnectorTest.class.getResourceAsStream(resource));
  }

  private static String readInputStreamToString(InputStream inputStream) throws IOException {
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int length;
    while ((length = inputStream.read(buffer)) != -1) {
      result.write(buffer, 0, length);
    }
    return result.toString("UTF-8");
  }

  private User createUserGroupUser(
      long id,
      String loginName,
      String sid,
      String name,
      String email,
      boolean isDomainGroup,
      boolean isSiteAdmin) {
    User u = new User();
    u.setID(id);
    u.setLoginName(loginName);
    u.setSid(sid);
    u.setName(name);
    u.setEmail(email);
    u.setIsDomainGroup(
        isDomainGroup
            ? com.microsoft.schemas.sharepoint.soap.directory.TrueFalseType.TRUE
            : com.microsoft.schemas.sharepoint.soap.directory.TrueFalseType.FALSE);
    u.setIsSiteAdmin(
        isSiteAdmin
            ? com.microsoft.schemas.sharepoint.soap.directory.TrueFalseType.TRUE
            : com.microsoft.schemas.sharepoint.soap.directory.TrueFalseType.FALSE);
    return u;
  }

  private PrincipalInfo createPrincipalInfo(
      String accountName, String dispalyName, SPPrincipalType type) {
    PrincipalInfo p = new PrincipalInfo();
    p.setAccountName(accountName);
    p.setDisplayName(dispalyName);
    p.setIsResolved(true);
    p.setPrincipalType(type);
    return p;
  }
}
