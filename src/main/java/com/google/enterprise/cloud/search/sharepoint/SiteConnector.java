package com.google.enterprise.cloud.search.sharepoint;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.springboardindex.model.ExternalGroup;
import com.google.api.services.springboardindex.model.Principal;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.enterprise.adaptor.sharepoint.MemberIdMapping;
import com.google.enterprise.adaptor.sharepoint.SiteDataClient;
import com.google.enterprise.springboard.sdk.Acl;
import com.microsoft.schemas.sharepoint.soap.GroupMembership;
import com.microsoft.schemas.sharepoint.soap.Permission;
import com.microsoft.schemas.sharepoint.soap.PolicyUser;
import com.microsoft.schemas.sharepoint.soap.Scopes.Scope;
import com.microsoft.schemas.sharepoint.soap.Site;
import com.microsoft.schemas.sharepoint.soap.TrueFalseType;
import com.microsoft.schemas.sharepoint.soap.UserDescription;
import com.microsoft.schemas.sharepoint.soap.VirtualServer;
import com.microsoft.schemas.sharepoint.soap.Web;
import com.microsoft.schemas.sharepoint.soap.directory.GetUserCollectionFromSiteResponse;
import com.microsoft.schemas.sharepoint.soap.directory.User;
import com.microsoft.schemas.sharepoint.soap.directory.UserGroupSoap;
import com.microsoft.schemas.sharepoint.soap.directory.GetUserCollectionFromSiteResponse.GetUserCollectionFromSiteResult;
import com.microsoft.schemas.sharepoint.soap.people.ArrayOfPrincipalInfo;
import com.microsoft.schemas.sharepoint.soap.people.ArrayOfString;
import com.microsoft.schemas.sharepoint.soap.people.PeopleSoap;
import com.microsoft.schemas.sharepoint.soap.people.PrincipalInfo;
import com.microsoft.schemas.sharepoint.soap.people.SPPrincipalType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

class SiteConnector {
  private static final Logger log = Logger.getLogger(SiteConnector.class.getName());
  private static final String IDENTITY_CLAIMS_PREFIX = "i:0";
  private static final String OTHER_CLAIMS_PREFIX = "c:0";
  static final long LIST_ITEM_MASK =
      SPBasePermissions.OPEN | SPBasePermissions.VIEWPAGES | SPBasePermissions.VIEWLISTITEMS;
  private static final String DEFAULT_NAMESPACE = "default";

  private final SiteDataClient siteDataClient;
  private final UserGroupSoap userGroup;
  private final PeopleSoap people;
  private final String siteUrl;
  private final String webUrl;
  private final String defaultNamespace;
  /**
   * Lock for refreshing MemberIdMapping. We use a unique lock because it is held while waiting on
   * I/O.
   */
  private final Object refreshMemberIdMappingLock = new Object();

  /**
   * Lock for refreshing SiteUserMapping. We use a unique lock because it is held while waiting on
   * I/O.
   */
  private final Object refreshSiteUserMappingLock = new Object();

  private SiteConnector(Builder builder) {
    this.siteDataClient = builder.siteDataClient;
    this.userGroup = builder.userGroup;
    this.people = builder.people;
    this.siteUrl = builder.siteUrl;
    this.webUrl = builder.webUrl;
    this.defaultNamespace = builder.defaultNamespace;
  }

  SiteDataClient getSiteDataClient() {
    return siteDataClient;
  }

  /**
   * Convert relative url to absolute URL. Input can be absolute URL or relative to web or relative
   * to root.
   *
   * @param url input URL to encode
   * @return absolute URL
   */
  String encodeDocId(String url) {
    if (url.toLowerCase().startsWith("https://") || url.toLowerCase().startsWith("http://")) {
      return url;
    } else if (!url.startsWith("/")) {
      // url is relative to web
      url = webUrl + "/" + url;
    } else {
      // Rip off everything after the third slash (including the slash).
      // Get http://example.com from http://example.com/some/folder.
      String[] parts = webUrl.split("/", 4);
      // url is relative to root
      url = parts[0] + "//" + parts[2] + url;
    }
    return url;
  }

  @VisibleForTesting
  List<ExternalGroup> computeMembersForGroups(GroupMembership groups) {
    checkNotNull(groups);
    List<ExternalGroup> defs = new ArrayList<ExternalGroup>();
    for (GroupMembership.Group group : groups.getGroup()) {
      // Use SharePoint Site URL as namespace for local SharePoint Groups
      String sharepointLocalGroup = Acl.getPrincipalName(group.getGroup().getName(), siteUrl);
      List<Principal> members = new ArrayList<Principal>();
      if (group.getUsers() != null) {
        for (UserDescription user : group.getUsers().getUser()) {
          Principal principal = userDescriptionToPrincipal(user);
          if (principal == null) {
            log.log(
                Level.WARNING,
                "Unable to determine login name. Skipping user with ID {0}",
                user.getID());
            continue;
          }
          members.add(principal);
        }
      }
      defs.add(Acl.getExternalGroup(sharepointLocalGroup, members));
    }
    return defs;
  }

  Acl getWebApplicationPolicyAcl(VirtualServer vs) {
    final long necessaryPermissionMask = LIST_ITEM_MASK;
    List<Principal> permits = new ArrayList<Principal>();
    List<Principal> denies = new ArrayList<Principal>();
    // A PolicyUser is either a user or group, but we aren't provided with
    // which. We make a web service call to determine which. When using claims
    // is enabled, we actually do know the type, but we need additional
    // information to produce a clear ACL. As such, we blindly get more info
    // for all the PolicyUsers at once in a single batch.
    List<String> policyUsers = new ArrayList<String>();
    for (PolicyUser policyUser : vs.getPolicies().getPolicyUser()) {
      policyUsers.add(policyUser.getLoginName());
    }
    Map<String, PrincipalInfo> resolvedPolicyUsers = resolvePrincipals(policyUsers);

    for (PolicyUser policyUser : vs.getPolicies().getPolicyUser()) {
      String loginName = policyUser.getLoginName();
      PrincipalInfo p = resolvedPolicyUsers.get(loginName);
      if (p == null || !p.isIsResolved()) {
        log.log(Level.WARNING, "Unable to resolve Policy User = {0}", loginName);
        continue;
      }
      if (p.getPrincipalType() != SPPrincipalType.SECURITY_GROUP
          && p.getPrincipalType() != SPPrincipalType.USER) {
        log.log(
            Level.WARNING,
            "Principal {0} is an unexpected type: {1}",
            new Object[] {p.getAccountName(), p.getPrincipalType()});
        continue;
      }
      boolean isGroup = p.getPrincipalType() == SPPrincipalType.SECURITY_GROUP;
      String accountName = getLoginNameForPrincipal(p.getAccountName(), p.getDisplayName());
      if (accountName == null) {
        log.log(Level.WARNING, "Unable to decode claim. Skipping policy user {0}", loginName);
        continue;
      }
      log.log(Level.FINER, "Policy User accountName = {0}", accountName);
      Principal principal;
      if (isGroup) {
        principal =
            Acl.getExternalGroupPrincipal(Acl.getPrincipalName(accountName, defaultNamespace));
      } else {
        principal =
            Acl.getExternalUserPrincipal(Acl.getPrincipalName(accountName, defaultNamespace));
      }
      long grant = policyUser.getGrantMask().longValue();
      if ((necessaryPermissionMask & grant) == necessaryPermissionMask) {
        permits.add(principal);
      }
      long deny = policyUser.getDenyMask().longValue();
      // If at least one necessary bit is masked, then deny user.
      if ((necessaryPermissionMask & deny) != 0) {
        denies.add(principal);
      }
    }
    return new Acl.Builder().setReaders(permits).setDeniedReaders(denies).build();
  }

  List<Principal> getSiteCollectionAdmins(Web web) {
    List<Principal> admins = new ArrayList<>();
    for (UserDescription user : web.getUsers().getUser()) {
      if (user.getIsSiteAdmin() != TrueFalseType.TRUE) {
        continue;
      }
      Principal principal = userDescriptionToPrincipal(user);
      if (principal == null) {
        log.log(
            Level.WARNING,
            "Unable to determine login name. Skipping admin user with ID " + "{0}",
            user.getID());
        continue;
      }
      admins.add(principal);
    }
    return admins;
  }

  /**
   * Returns the url of the parent of the web. The parent url is not the same as the siteUrl, since
   * there may be multiple levels of webs. It is an error to call this method when there is no
   * parent, which is the case iff {@link #isWebSiteCollection} is {@code true}.
   */
  String getWebParentUrl() {
    if (isWebSiteCollection()) {
      throw new IllegalStateException();
    }
    int slashIndex = webUrl.lastIndexOf("/");
    return webUrl.substring(0, slashIndex);
  }

  /** Returns true if webUrl is a site collection. */
  private boolean isWebSiteCollection() {
    return siteUrl.equals(webUrl);
  }

  private Map<String, PrincipalInfo> resolvePrincipals(List<String> principalsToResolve) {
    Map<String, PrincipalInfo> resolved = new HashMap<String, PrincipalInfo>();
    if (principalsToResolve.isEmpty()) {
      return resolved;
    }
    ArrayOfString aos = new ArrayOfString();
    aos.getString().addAll(principalsToResolve);
    ArrayOfPrincipalInfo resolvePrincipals =
        people.resolvePrincipals(aos, SPPrincipalType.ALL, false);
    List<PrincipalInfo> principals = resolvePrincipals.getPrincipalInfo();
    // using loginname from input list principalsToResolve as a key
    // instead of returned PrincipalInfo.getAccountName() as with claims
    // authentication PrincipalInfo.getAccountName() is always encoded.
    // e.g. if login name from Policy is NT Authority\Local Service
    // returned account name is i:0#.w|NT Authority\Local Service
    for (int i = 0; i < principalsToResolve.size() && i < principals.size(); i++) {
      resolved.put(principalsToResolve.get(i), principals.get(i));
    }
    return resolved;
  }

  List<Principal> getWebAcls(Web rootWb) throws IOException {
    return generateAcl(rootWb.getACL().getPermissions().getPermission(), LIST_ITEM_MASK);
  }

  List<Principal> getListAcl(com.microsoft.schemas.sharepoint.soap.List list) throws IOException {
    return generateAcl(list.getACL().getPermissions().getPermission(), LIST_ITEM_MASK);
  }

  List<Principal> getScopeAcl(Scope scope) throws IOException {
    return generateAcl(scope.getPermission(), LIST_ITEM_MASK);
  }

  private List<Principal> generateAcl(
      List<Permission> permissions, final long necessaryPermissionMask) throws IOException {
    List<Principal> permits = new LinkedList<Principal>();
    MemberIdMapping mapping = getMemberIdMapping();
    boolean memberIdMappingRefreshed = false;
    MemberIdMapping siteUserMapping = null;
    boolean siteUserMappingRefreshed = false;
    for (Permission permission : permissions) {
      // Although it is named "mask", this is really a bit-field of
      // permissions.
      long mask = permission.getMask().longValue();
      if ((necessaryPermissionMask & mask) != necessaryPermissionMask) {
        continue;
      }
      Integer id = permission.getMemberid();
      Principal principal = mapping.getPrincipal(id);
      if (principal == null) {
        log.log(
            Level.FINE,
            "Member id {0} is not available in memberid"
                + " mapping for Web [{1}] under Site Collection [{2}].",
            new Object[] {id, webUrl, siteUrl});
        if (siteUserMapping == null) {
          siteUserMapping = getSiteUserMapping();
        }
        principal = siteUserMapping.getPrincipal(id);
      }
      if (principal == null && !memberIdMappingRefreshed) {
        // Try to refresh member id mapping and check again.
        mapping = refreshMemberIdMapping(mapping);
        memberIdMappingRefreshed = true;
        principal = mapping.getPrincipal(id);
      }
      if (principal == null && !siteUserMappingRefreshed) {
        // Try to refresh site user mapping and check again.
        siteUserMapping = refreshSiteUserMapping(siteUserMapping);
        siteUserMappingRefreshed = true;
        principal = siteUserMapping.getPrincipal(id);
      }

      if (principal == null) {
        log.log(
            Level.WARNING,
            "Could not resolve member id {0} for Web " + "[{1}] under Site Collection [{2}].",
            new Object[] {id, webUrl, siteUrl});
        continue;
      }
      permits.add(principal);
    }
    return permits;
  }

  private MemberIdMapping getMemberIdMapping() throws IOException {
    try {
      return retrieveMemberIdMapping();
    } catch (IOException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  private MemberIdMapping retrieveMemberIdMapping() throws IOException {
    log.entering("SiteConnector", "retrieveMemberIdMapping");
    Site site = siteDataClient.getContentSite();
    Map<Integer, Principal> map = new HashMap<Integer, Principal>();
    for (GroupMembership.Group group : site.getGroups().getGroup()) {
      String sharepointLocalGroup =
          Acl.getPrincipalName(group.getGroup().getName(), site.getMetadata().getURL());
      Principal localGroup = Acl.getExternalGroupPrincipal(sharepointLocalGroup);
      map.put(group.getGroup().getID(), localGroup);
    }
    for (UserDescription user : site.getWeb().getUsers().getUser()) {
      Principal principal = userDescriptionToPrincipal(user);
      if (principal == null) {
        log.log(
            Level.WARNING,
            "Unable to determine login name. Skipping user with ID {0}",
            user.getID());
        continue;
      }
      map.put(user.getID(), principal);
    }
    MemberIdMapping mapping = new MemberIdMapping(map);
    log.exiting("SiteConnector", "retrieveMemberIdMapping", mapping);
    return mapping;
  }

  private MemberIdMapping retrieveSiteUserMapping() {
    log.entering("SiteConnector", "retrieveSiteUserMapping");
    GetUserCollectionFromSiteResponse.GetUserCollectionFromSiteResult result =
        userGroup.getUserCollectionFromSite();
    Map<Integer, Principal> map = new HashMap<Integer, Principal>();
    MemberIdMapping mapping;
    if (result == null) {
      mapping = new MemberIdMapping(map);
      log.exiting("SiteConnector", "retrieveSiteUserMapping", mapping);
      return mapping;
    }
    GetUserCollectionFromSiteResult.GetUserCollectionFromSite siteUsers =
        result.getGetUserCollectionFromSite();
    if (siteUsers.getUsers() == null) {
      mapping = new MemberIdMapping(map);
      log.exiting("SiteConnector", "retrieveSiteUserMapping", mapping);
      return mapping;
    }
    for (User user : siteUsers.getUsers().getUser()) {
      boolean isDomainGroup =
          (user.getIsDomainGroup()
              == com.microsoft.schemas.sharepoint.soap.directory.TrueFalseType.TRUE);

      String userName = getLoginNameForPrincipal(user.getLoginName(), user.getName());
      if (userName == null) {
        log.log(
            Level.WARNING,
            "Unable to determine login name. Skipping user with ID {0}",
            user.getID());
        continue;
      }
      if (isDomainGroup) {
        map.put(
            (int) user.getID(),
            Acl.getExternalGroupPrincipal(Acl.getPrincipalName(userName, defaultNamespace)));
      } else {
        map.put(
            (int) user.getID(),
            Acl.getExternalUserPrincipal(Acl.getPrincipalName(userName, defaultNamespace)));
      }
    }
    mapping = new MemberIdMapping(map);
    log.exiting("SiteConnector", "retrieveSiteUserMapping", mapping);
    return mapping;
  }

  /**
   * Provide a more recent MemberIdMapping than {@code mapping}, because the mapping is known to be
   * out-of-date.
   */
  private MemberIdMapping refreshMemberIdMapping(MemberIdMapping mapping) throws IOException {
    // Synchronize callers to prevent a rush of invalidations due to multiple
    // callers noticing that the map was out of date at the same time.
    synchronized (refreshMemberIdMappingLock) {
      // NOTE: This may block on I/O, so we must be wary of what locks are
      // held.
      MemberIdMapping maybeNewMapping = getMemberIdMapping();
      if (mapping != maybeNewMapping) {
        // The map has already been refreshed.
        return maybeNewMapping;
      }
      //memberIdsCache.invalidate(siteUrl);
    }
    return getMemberIdMapping();
  }

  /**
   * Provide a more recent SiteUserMapping than {@code mapping}, because the mapping is known to be
   * out-of-date.
   */
  private MemberIdMapping refreshSiteUserMapping(MemberIdMapping mapping) {
    // Synchronize callers to prevent a rush of invalidations due to multiple
    // callers noticing that the map was out of date at the same time.
    synchronized (refreshSiteUserMappingLock) {
      // NOTE: This may block on I/O, so we must be wary of what locks are
      // held.
      MemberIdMapping maybeNewMapping = getSiteUserMapping();
      if (mapping != maybeNewMapping) {
        // The map has already been refreshed.
        return maybeNewMapping;
      }
      //siteUserCache.invalidate(siteUrl);
    }
    return getSiteUserMapping();
  }

  private MemberIdMapping getSiteUserMapping() {
    return retrieveSiteUserMapping();
  }

  private Principal userDescriptionToPrincipal(UserDescription user) {
    boolean isDomainGroup = (user.getIsDomainGroup() == TrueFalseType.TRUE);
    String userName = getLoginNameForPrincipal(user.getLoginName(), user.getName());
    if (userName == null) {
      return null;
    }
    if (isDomainGroup) {
      return Acl.getExternalGroupPrincipal(Acl.getPrincipalName(userName, defaultNamespace));
    } else {
      return Acl.getExternalUserPrincipal(Acl.getPrincipalName(userName, defaultNamespace));
    }
  }

  private String getLoginNameForPrincipal(String loginName, String displayName) {
    return decodeClaim(loginName, displayName);
  }

  @VisibleForTesting
  static String decodeClaim(String loginName, String name) {
    if (!loginName.startsWith(IDENTITY_CLAIMS_PREFIX)
        && !loginName.startsWith(OTHER_CLAIMS_PREFIX)) {
      return loginName;
    }
    // https://social.technet.microsoft.com/wiki/contents/articles/13921.sharepoint-20102013-claims-encoding.aspx
    // AD User
    if (loginName.startsWith("i:0#.w|")) {
      return loginName.substring(7);
      // AD Group
    } else if (loginName.startsWith("c:0+.w|")) {
      return name;
    } else if (loginName.equals("c:0(.s|true")) {
      return "Everyone";
    } else if (loginName.equals("c:0!.s|windows")) {
      return "NT AUTHORITY\\authenticated users";
      // Forms authentication role
    } else if (loginName.startsWith("c:0-.f|")) {
      return loginName.substring(7).replace("|", ":");
      // Forms authentication user
    } else if (loginName.startsWith("i:0#.f|")) {
      return loginName.substring(7).replace("|", ":");
      // Identity and role claims for trusted providers such as ADFS
    } else if (loginName.matches("^([i|c]\\:0.\\.t\\|).*$")) {
      String[] parts = loginName.split(Pattern.quote("|"), 3);
      if (parts.length == 3) {
        return parts[2];
      }
    }
    log.log(Level.WARNING, "Unsupported claims value {0}", loginName);
    return null;
  }

  String getSiteUrl() {
    return siteUrl;
  }

  String getWebUrl() {
    return webUrl;
  }

  static class Builder {
    private SiteDataClient siteDataClient;
    private UserGroupSoap userGroup;
    private PeopleSoap people;
    private String siteUrl;
    private String webUrl;
    private String defaultNamespace = DEFAULT_NAMESPACE;

    Builder(String siteUrl, String webUrl) {
      this.siteUrl = siteUrl;
      this.webUrl = webUrl;
    }

    Builder setDefaultNamespace(String defaultNamespace) {
      this.defaultNamespace = defaultNamespace;
      return this;
    }

    Builder setSiteDataClient(SiteDataClient siteDataClient) {
      this.siteDataClient = siteDataClient;
      return this;
    }

    Builder setUserGroup(UserGroupSoap userGroup) {
      this.userGroup = userGroup;
      return this;
    }

    Builder setPeople(PeopleSoap people) {
      this.people = people;
      return this;
    }

    SiteConnector build() {
      checkArgument(!Strings.isNullOrEmpty(siteUrl));
      checkArgument(!Strings.isNullOrEmpty(webUrl));
      checkNotNull(siteDataClient);
      checkNotNull(userGroup);
      checkNotNull(people);
      return new SiteConnector(this);
    }
  }

  /** As defined at http://msdn.microsoft.com/en-us/library/ee394878.aspx . */
  @SuppressWarnings("unused")
  public static class SPBasePermissions {
    public static final long EMPTYMASK = 0x0000000000000000;
    public static final long VIEWLISTITEMS = 0x0000000000000001;
    public static final long ADDLISTITEMS = 0x0000000000000002;
    public static final long EDITLISTITEMS = 0x0000000000000004;
    public static final long DELETELISTITEMS = 0x0000000000000008;
    public static final long APPROVEITEMS = 0x0000000000000010;
    public static final long OPENITEMS = 0x0000000000000020;
    public static final long VIEWVERSIONS = 0x0000000000000040;
    public static final long DELETEVERSIONS = 0x0000000000000080;
    public static final long CANCELCHECKOUT = 0x0000000000000100;
    public static final long MANAGEPERSONALVIEWS = 0x0000000000000200;
    public static final long MANAGELISTS = 0x0000000000000800;
    public static final long VIEWFORMPAGES = 0x0000000000001000;
    public static final long OPEN = 0x0000000000010000;
    public static final long VIEWPAGES = 0x0000000000020000;
    public static final long ADDANDCUSTOMIZEPAGES = 0x0000000000040000;
    public static final long APPLYTHEMEANDBORDER = 0x0000000000080000;
    public static final long APPLYSTYLESHEETS = 0x0000000000100000;
    public static final long VIEWUSAGEDATA = 0x0000000000200000;
    public static final long CREATESSCSITE = 0x0000000000400000;
    public static final long MANAGESUBWEBS = 0x0000000000800000;
    public static final long CREATEGROUPS = 0x0000000001000000;
    public static final long MANAGEPERMISSIONS = 0x0000000002000000;
    public static final long BROWSEDIRECTORIES = 0x0000000004000000;
    public static final long BROWSEUSERINFO = 0x0000000008000000;
    public static final long ADDDELPRIVATEWEBPARTS = 0x0000000010000000;
    public static final long UPDATEPERSONALWEBPARTS = 0x0000000020000000;
    public static final long MANAGEWEB = 0x0000000040000000;
    public static final long USECLIENTINTEGRATION = 0x0000001000000000L;
    public static final long USEREMOTEAPIS = 0x0000002000000000L;
    public static final long MANAGEALERTS = 0x0000004000000000L;
    public static final long CREATEALERTS = 0x0000008000000000L;
    public static final long EDITMYUSERINFO = 0x0000010000000000L;
    public static final long ENUMERATEPERMISSIONS = 0x4000000000000000L;
    public static final long FULLMASK = 0x7FFFFFFFFFFFFFFFL;
  }
}
