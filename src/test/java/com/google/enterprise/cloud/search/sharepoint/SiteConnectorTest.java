package com.google.enterprise.cloud.search.sharepoint;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.google.api.services.cloudsearch.v1.model.Principal;
import com.google.common.collect.ImmutableList;
import com.google.enterprise.cloud.search.sharepoint.SiteConnector.SPBasePermissions;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SiteConnectorTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Mock SiteDataClient siteDataClient;
  @Mock SiteDataSoap siteDataSoap;
  @Mock PeopleSoap peopleSoap;
  @Mock UserGroupSoap userGroupSoap;

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
            .build();
    Principal spUser1 = Acl.getUserPrincipal(Acl.getPrincipalName("GDC-PSL\\spuser1", "default"));
    Principal teamSiteOwners =
        Acl.getGroupPrincipal(
            Acl.getPrincipalName("TeamSite Owners", "http://localhost:1/sites/SiteCollection"));
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
            .build();
    Principal spUser1 = Acl.getUserPrincipal(Acl.getPrincipalName("GDC-PSL\\spuser1", "default"));
    Principal teamSiteOwners =
        Acl.getGroupPrincipal(
            Acl.getPrincipalName("TeamSite Owners", "http://localhost:1/sites/SiteCollection"));
    Principal admin =
        Acl.getUserPrincipal(Acl.getPrincipalName("GDC-PSL\\administrator", "default"));
    Principal group300 =
        Acl.getGroupPrincipal(Acl.getPrincipalName("group300@gdc-psl.com", "default"));
    assertEquals(Arrays.asList(spUser1, teamSiteOwners, admin, group300), sc.getListAcl(list));
  }

  @Test
  public void testScopeAcls() throws IOException {
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
            .build();
    Principal spUser1 = Acl.getUserPrincipal(Acl.getPrincipalName("GDC-PSL\\spuser1", "default"));
    Principal teamSiteOwners =
        Acl.getGroupPrincipal(
            Acl.getPrincipalName("TeamSite Owners", "http://localhost:1/sites/SiteCollection"));
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
            Acl.getUserPrincipal(Acl.getPrincipalName("DOMAIN\\user1Admin", "default")),
            Acl.getGroupPrincipal(Acl.getPrincipalName("DOMAIN\\group1Admin", "default")));
    assertEquals(expected, sc.getSiteCollectionAdmins(web));
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
    SiteConnector sc =
        new SiteConnector.Builder("http://sp.com", "http://sp.com")
            .setSiteDataClient(siteDataClient)
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    Acl expected =
        new Acl.Builder()
            .setDeniedReaders(
                Collections.singletonList(
                    Acl.getUserPrincipal(Acl.getPrincipalName("GDC-PSL\\spuser1", "default"))))
            .setReaders(
                Arrays.asList(
                    Acl.getGroupPrincipal(
                        Acl.getPrincipalName("NT AUTHORITY\\LOCAL SERVICE", "default")),
                    Acl.getUserPrincipal(
                        Acl.getPrincipalName("GDC-PSL\\Administrator", "default"))))
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
