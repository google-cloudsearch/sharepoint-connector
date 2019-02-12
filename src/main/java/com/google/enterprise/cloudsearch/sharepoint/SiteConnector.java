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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.cloudidentity.v1.model.EntityKey;
import com.google.api.services.cloudidentity.v1.model.Membership;
import com.google.api.services.cloudidentity.v1.model.MembershipRole;
import com.google.api.services.cloudsearch.v1.model.Principal;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.enterprise.cloudsearch.sdk.identity.IdentityGroup;
import com.google.enterprise.cloudsearch.sdk.identity.IdentitySourceConfiguration;
import com.google.enterprise.cloudsearch.sdk.identity.RepositoryContext;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
import com.google.enterprise.cloudsearch.sharepoint.ActiveDirectoryPrincipal.PrincipalFormat;
import com.google.enterprise.cloudsearch.sharepoint.SharePointConfiguration.SharePointDeploymentType;
import com.microsoft.schemas.sharepoint.soap.GroupMembership;
import com.microsoft.schemas.sharepoint.soap.Permission;
import com.microsoft.schemas.sharepoint.soap.PolicyUser;
import com.microsoft.schemas.sharepoint.soap.Scopes.Scope;
import com.microsoft.schemas.sharepoint.soap.Site;
import com.microsoft.schemas.sharepoint.soap.TrueFalseType;
import com.microsoft.schemas.sharepoint.soap.UserDescription;
import com.microsoft.schemas.sharepoint.soap.Users;
import com.microsoft.schemas.sharepoint.soap.VirtualServer;
import com.microsoft.schemas.sharepoint.soap.Web;
import com.microsoft.schemas.sharepoint.soap.directory.GetUserCollectionFromSiteResponse;
import com.microsoft.schemas.sharepoint.soap.directory.GetUserCollectionFromSiteResponse.GetUserCollectionFromSiteResult;
import com.microsoft.schemas.sharepoint.soap.directory.User;
import com.microsoft.schemas.sharepoint.soap.directory.UserGroupSoap;
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
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

class SiteConnector {
  private static final Logger log = Logger.getLogger(SiteConnector.class.getName());
  private static final String IDENTITY_CLAIMS_PREFIX = "i:0";
  private static final String OTHER_CLAIMS_PREFIX = "c:0";
  private static final String SHAREPOINT_LOCAL_GROUP_FORMAT = "[%s]%s";
  private static final Supplier<Set<Membership>> EMPTY_MEMBERSHIP = () -> ImmutableSet.of();
  private static final ImmutableList<MembershipRole> MEMBER_ROLES =
      ImmutableList.of(new MembershipRole().setName("MEMBER"));
  static final long LIST_ITEM_MASK =
      SPBasePermissions.OPEN | SPBasePermissions.VIEWPAGES | SPBasePermissions.VIEWLISTITEMS;
  /** Default identity source for external principals when no domain information is available */
  static final String DEFAULT_REFERENCE_IDENTITY_SOURCE_NAME = "defaultIdentitySource";

  private final SiteDataClient siteDataClient;
  private final UserGroupSoap userGroup;
  private final PeopleSoap people;
  private final String siteUrl;
  private final String webUrl;
  private final Optional<ActiveDirectoryClient> activeDirectoryClient;
  private final SharePointDeploymentType sharePointDeploymentType;
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

  private final ImmutableMap<String, IdentitySourceConfiguration>
      referenceIdentitySourceConfiguration;
  private final Optional<IdentitySourceConfiguration> defaultIdentitySourceConfiguration;
  private final boolean stripDomainInUserPrincipals;

  private SiteConnector(Builder builder) {
    this.siteDataClient = builder.siteDataClient;
    this.userGroup = builder.userGroup;
    this.people = builder.people;
    this.siteUrl = builder.siteUrl;
    this.webUrl = builder.webUrl;
    this.activeDirectoryClient = Optional.ofNullable(builder.activeDirectoryClient);
    this.referenceIdentitySourceConfiguration = builder.referenceIdentitySourceConfiguration;
    this.defaultIdentitySourceConfiguration =
        Optional.ofNullable(
            referenceIdentitySourceConfiguration.get(DEFAULT_REFERENCE_IDENTITY_SOURCE_NAME));
    this.stripDomainInUserPrincipals = builder.stripDomainInUserPrincipals;
    this.sharePointDeploymentType = builder.sharePointDeploymentType;
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

  private boolean isSharePointOnlineDeployment() {
    return sharePointDeploymentType == SharePointDeploymentType.ONLINE;
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
      String accountName =
          getLoginNameForPrincipal(
              p.getAccountName(), p.getDisplayName(), policyUser.getSid(), isGroup);
      if (accountName == null) {
        log.log(Level.WARNING, "Unable to decode claim. Skipping policy user {0}", loginName);
        continue;
      }
      log.log(Level.FINER, "Policy User accountName = {0}", accountName);
      Principal principal = getPrincipal(accountName, isGroup).orElse(null);
      if (principal == null) {
        continue;
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

  @VisibleForTesting
  static String encodeSharePointLocalGroupName(String siteIdentifier, String groupName) {
    return String.format(SHAREPOINT_LOCAL_GROUP_FORMAT, siteIdentifier, groupName);
  }

  /** Returns true if webUrl is a site collection. */
  boolean isWebSiteCollection() {
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

  public List<IdentityGroup> getSharePointGroups(RepositoryContext repositoryContext)
      throws IOException {
    Site site = siteDataClient.getContentSite();
    ImmutableList.Builder<IdentityGroup> groups = new ImmutableList.Builder<>();
    for (GroupMembership.Group group : site.getGroups().getGroup()) {
      String localGroup =
          encodeSharePointLocalGroupName(site.getMetadata().getURL(), group.getGroup().getName());
      Users users = group.getUsers();
      if (users == null) {
        groups.add(repositoryContext.buildIdentityGroup(localGroup, EMPTY_MEMBERSHIP));
        continue;
      }
      List<UserDescription> members = users.getUser();
      if (members == null) {
        groups.add(repositoryContext.buildIdentityGroup(localGroup, EMPTY_MEMBERSHIP));
        continue;
      }
      ImmutableSet.Builder<Membership> groupMembers = new ImmutableSet.Builder<>();
      for (UserDescription member : members) {
        getMembership(member, repositoryContext).ifPresent(groupMembers::add);
      }
      groups.add(repositoryContext.buildIdentityGroup(localGroup, () -> groupMembers.build()));
    }
    return groups.build();
  }

  private Optional<Membership> getMembership(UserDescription user, RepositoryContext context)
      throws IOException {
    boolean isDomainGroup = (user.getIsDomainGroup() == TrueFalseType.TRUE);
    EntityKey memberKey = null;
    if (isDomainGroup) {
      String groupId =
          getLoginNameForPrincipal(
              user.getLoginName(), user.getName(), user.getSid(), isDomainGroup);
      if (!Strings.isNullOrEmpty(groupId)) {
        Optional<RepositoryContext> referenceContext =
            isSharePointOnlineDeployment()
                ? getRepositoryContextForSharePointOnlineGroup(context)
                : getRepositoryContextForActiveDirectoryGroup(groupId, context);
        if (referenceContext.isPresent()) {
          memberKey = referenceContext.get().buildEntityKeyForGroup(groupId);
        } else {
          log.log(
              Level.WARNING,
              "Identity source configuration not available for principal {0}",
              groupId);
        }
      }
    } else {
      memberKey = getUserMembership(user).orElse(null);
    }
    if (memberKey == null) {
      log.log(
          Level.WARNING,
          "Unable to resolve membership for user [name = {0}, loginName: {1}]",
          new Object[] {user.getName(), user.getLoginName()});
      return Optional.empty();
    }
    return Optional.of(new Membership().setPreferredMemberKey(memberKey).setRoles(MEMBER_ROLES));
  }

  private Optional<RepositoryContext> getRepositoryContextForActiveDirectoryGroup(
      String groupId, RepositoryContext context) {
    ActiveDirectoryPrincipal groupPrincipal = ActiveDirectoryPrincipal.parse(groupId);
    if (Strings.isNullOrEmpty(groupPrincipal.getDomain())) {
      return Optional.empty();
    }
    return context.getRepositoryContextForReferenceIdentitySource(groupPrincipal.getDomain());
  }

  private Optional<RepositoryContext> getRepositoryContextForSharePointOnlineGroup(
      RepositoryContext context) {
    return context.getRepositoryContextForReferenceIdentitySource(
        DEFAULT_REFERENCE_IDENTITY_SOURCE_NAME);
  }

  private Optional<EntityKey> getUserMembership(UserDescription user) throws IOException {
    if (!Strings.isNullOrEmpty(user.getEmail())) {
      return Optional.of(new EntityKey().setId(user.getEmail()));
    }
    if (activeDirectoryClient.isPresent()) {
      String loginName =
          isSharePointOnlineDeployment()
              ? decodeSharePointOnlineClaim(user.getLoginName())
              : decodeClaim(user.getLoginName(), user.getName());
      ActiveDirectoryPrincipal principal = ActiveDirectoryPrincipal.parse(loginName);
      String userEmailByAccountName =
          activeDirectoryClient.get().getUserEmailByPrincipal(principal);
      if (Strings.isNullOrEmpty(userEmailByAccountName)) {
        return Optional.empty();
      }
      return Optional.of(new EntityKey().setId(userEmailByAccountName));
    }
    return Optional.empty();
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
      int id = permission.getMemberid();
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
      Principal localGroup =
          Acl.getGroupPrincipal(
              encodeSharePointLocalGroupName(
                  site.getMetadata().getURL(), group.getGroup().getName()));
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

      String userName =
          getLoginNameForPrincipal(
              user.getLoginName(), user.getName(), user.getSid(), isDomainGroup);
      if (userName == null) {
        log.log(
            Level.WARNING,
            "Unable to determine login name. Skipping user with ID {0}",
            user.getID());
        continue;
      }

      Principal principal = getPrincipal(userName, isDomainGroup).orElse(null);
      if (principal != null) {
        map.put((int) user.getID(), principal);
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
    String userName =
        getLoginNameForPrincipal(user.getLoginName(), user.getName(), user.getSid(), isDomainGroup);
    if (userName == null) {
      return null;
    }
    return getPrincipal(userName, isDomainGroup).orElse(null);
  }

  private String getLoginNameForPrincipal(
      String loginName, String displayName, String sid, boolean isDomainGroup) {
    if (isDomainGroup
        && activeDirectoryClient.isPresent()
        && loginName.startsWith("c:0+.w|")
        && !Strings.isNullOrEmpty(sid)) {
      try {
        return activeDirectoryClient.get().getUserAccountBySid(sid);
      } catch (IOException ex) {
        log.log(
            Level.WARNING,
            String.format(
                "Error performing SID lookup for "
                    + "User %s. Returing display name %s as fallback.",
                loginName, displayName),
            ex);
        return displayName;
      }
    }
    return isSharePointOnlineDeployment()
        ? decodeSharePointOnlineClaim(loginName)
        : decodeClaim(loginName, displayName);
  }

  private Optional<Principal> getPrincipal(String id, boolean isGroup) {
    return isSharePointOnlineDeployment()
        ? getSharePointOnlinePrincipal(id, isGroup)
        : getActiveDirectoryPrincipal(id, isGroup);
  }

  private Optional<Principal> getActiveDirectoryPrincipal(String id, boolean isGroup) {
    ActiveDirectoryPrincipal adPrincipal = ActiveDirectoryPrincipal.parse(id);
    if (adPrincipal.getFormat() == PrincipalFormat.NONE) {
      if (defaultIdentitySourceConfiguration.isPresent()) {
        if (isGroup) {
          return Optional.of(Acl.getGroupPrincipal(
              id, defaultIdentitySourceConfiguration.get().getIdentitySourceId()));
        } else {
          return Optional.of(Acl.getUserPrincipal(
              id, defaultIdentitySourceConfiguration.get().getIdentitySourceId()));
        }
      } else {
        log.log(
            Level.WARNING,
            "No default identity source configuration available. Returning empty for principal {0}",
            id);
       return Optional.empty();
      }
    }
    String domain = adPrincipal.getDomain();
    IdentitySourceConfiguration identitySource = referenceIdentitySourceConfiguration.get(domain);
    if (identitySource == null) {
      log.log(
          Level.WARNING,
          "No identity source configuration available for domain {0}. "
              + "Returning empty for principal {1}",
          new Object[] {domain, id});
      return Optional.empty();
    }
    if (isGroup) {
      return Optional.of(Acl.getGroupPrincipal(id, identitySource.getIdentitySourceId()));
    } else {
      String userId =
          stripDomainInUserPrincipals
              ? adPrincipal.getPrincipalNameInFormat(PrincipalFormat.NONE)
              : id;
      return Optional.of(Acl.getUserPrincipal(userId, identitySource.getIdentitySourceId()));
    }
  }

  private Optional<Principal> getSharePointOnlinePrincipal(String id, boolean isGroup) {
    if (!defaultIdentitySourceConfiguration.isPresent()) {
      return Optional.empty();
    }
    if (isGroup) {
      return Optional.of(
          Acl.getGroupPrincipal(
              id, defaultIdentitySourceConfiguration.get().getIdentitySourceId()));
    } else {
      return Optional.of(
          Acl.getUserPrincipal(id, defaultIdentitySourceConfiguration.get().getIdentitySourceId()));
    }
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

  @VisibleForTesting
  static String decodeSharePointOnlineClaim(String loginName) {
    // For SharePoint Online everything we support is claims authentication.
    if (!loginName.startsWith(IDENTITY_CLAIMS_PREFIX)
        && !loginName.startsWith(OTHER_CLAIMS_PREFIX)) {
      return null;
    }
    // Forms authentication role / user
    if (loginName.startsWith("c:0-.f|") || loginName.startsWith("i:0#.f|")) {
      String[] parts = loginName.split(Pattern.quote("|"), 3);
      if (parts.length == 3) {
        return parts[2];
      }
      // Office 365 Security Groups
    } else if (loginName.startsWith("c:0t.c|tenant|")) {
      return loginName.substring("c:0t.c|tenant|".length());
      // Azure Active Directory Groups
    } else if (loginName.startsWith("c:0o.c|federateddirectoryclaimprovider|")) {
      return loginName.substring("c:0o.c|federateddirectoryclaimprovider|".length());
    }
    log.log(Level.WARNING, "Unsupported claims value {0} for SharePoint Online", loginName);
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
    private ActiveDirectoryClient activeDirectoryClient;
    private ImmutableMap<String, IdentitySourceConfiguration> referenceIdentitySourceConfiguration =
        ImmutableMap.of();
    private boolean stripDomainInUserPrincipals;
    private SharePointDeploymentType sharePointDeploymentType =
        SharePointDeploymentType.ON_PREMISES;

    Builder(String siteUrl, String webUrl) {
      this.siteUrl = siteUrl;
      this.webUrl = webUrl;
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

    Builder setActiveDirectoryClient(ActiveDirectoryClient activeDirectoryClient) {
      this.activeDirectoryClient = activeDirectoryClient;
      return this;
    }

    Builder setReferenceIdentitySourceConfiguration(
        Map<String, IdentitySourceConfiguration> referenceIdentitySourceConfiguration) {
      this.referenceIdentitySourceConfiguration =
          ImmutableMap.copyOf(referenceIdentitySourceConfiguration);
      return this;
    }

    Builder setStripDomainInUserPrincipals(boolean stripDomainInUserPrincipals) {
      this.stripDomainInUserPrincipals = stripDomainInUserPrincipals;
      return this;
    }

    Builder setSharePointDeploymentType(SharePointDeploymentType sharePointDeploymentType) {
      this.sharePointDeploymentType = sharePointDeploymentType;
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
