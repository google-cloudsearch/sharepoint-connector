package com.google.enterprise.cloudsearch.sharepoint;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sharepoint.ActiveDirectoryPrincipal.PrincipalFormat;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.CommunicationException;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

/*
 * ActiveDirectory client to resolve principals.
 */
class ActiveDirectoryClient {
  private static final Logger log =
      Logger.getLogger(ActiveDirectoryClient.class.getName());
  private static final String ATTR_SAMACCOUNTNAME = "sAMAccountName";
  private static final String ATTR_NETBIOSNAME = "nETBIOSName";
  private static final String ATTR_DNSROOT = "dnsRoot";
  private static final String ATTR_DEFAULTNAMINGCONTEXT = "defaultNamingContext";
  private static final String ATTR_CONFIGURATIONNAMINGCONTEXT = "configurationNamingContext";
  private static final String ATTR_NAME = "name";
  private static final String ATTR_MAIL = "mail";
  private static final String ATTR_SID = "sid";

  private final ADServer adServer;
  private final LoadingCache<String, Optional<String>> cache =
      CacheBuilder.newBuilder()
          // Cache will auto expire in 30 minutes after initial write or update.
          .expireAfterWrite(30, TimeUnit.MINUTES)
          .build(
              new CacheLoader<String, Optional<String>>() {
                @Override
                public Optional<String> load(String key) throws IOException {
                  log.log(Level.FINE, "Performing SID lookup for {0}", key);
                  Optional<String> resolved = adServer.getUserAccountBySid(key);
                  if (!resolved.isPresent()) {
                    // CacheBuilder doesn't allow to return null here.
                    // Throwing IOEXception will result in repeated attempts
                    // to resolve unknown SID. To avoid repeated attempts to resolve
                    // SID, returning empty string here.
                    log.log(Level.WARNING, "Could not resolve SID {0} to account name.", key);
                  }
                  log.log(Level.FINE, "SID {0} resolved to {1}", new Object[] {key, resolved});
                  return resolved;
                }
              });

  private final LoadingCache<ActiveDirectoryPrincipal, Optional<String>> cacheEmailByPrincipal =
      CacheBuilder.newBuilder()
          // Cache will auto expire in 30 minutes after initial write or update.
          .expireAfterWrite(30, TimeUnit.MINUTES)
          .build(
              new CacheLoader<ActiveDirectoryPrincipal, Optional<String>>() {
                @Override
                public Optional<String> load(ActiveDirectoryPrincipal principal)
                    throws IOException {
                  log.log(Level.FINE, "Performing lookup for {0}", principal);
                  Optional<String> resolved = adServer.getEmailByPrincipal(principal);
                  if (!resolved.isPresent()) {
                    log.log(Level.WARNING, "Could not resolve principal {0} to email.", principal);
                  }
                  log.log(
                      Level.FINE,
                      "Principal {0} resolved to {1}",
                      new Object[] {principal, resolved});
                  return resolved;
                }
              });

  String getUserAccountBySid(String sid) throws IOException {
    if (Strings.isNullOrEmpty(sid)) {
      return null;
    }
    validateSid(sid);
    try {
      String domainSid = sid.substring(0, sid.lastIndexOf("-"));
      Optional<String> domain = cache.get(domainSid);
      if (!domain.isPresent()) {
        log.log(Level.WARNING, "Could not resolve domain for domain SID {0}."
            + " Returning null as account name for SID {1}",
            new Object[] {domainSid, sid});
        return null;
      }
      Optional<String> accountname = cache.get(sid);
      if (!accountname.isPresent()) {
        log.log(Level.WARNING, "Could not resolve accountname for SID {0}."
            + " Returning null as account name.", sid);
        return null;
      }

      String logonName = domain.get() + "\\" + accountname.get();
      log.log(Level.FINE, "Returning loginname as {0} for SID {1}",
          new Object[] {logonName, sid});
      return logonName;
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }
  }

  String getUserEmailByPrincipal(ActiveDirectoryPrincipal principal) throws IOException {
    try {
      return cacheEmailByPrincipal.get(principal).orElse(null);
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }
  }

  private static void validateSid(String sid) {
    checkArgument(sid.startsWith("S-1-") || sid.startsWith("s-1-"), "Invalid SID: %s", sid);
  }

  @VisibleForTesting
  ActiveDirectoryClient(ADServer adServer) throws IOException {
    checkNotNull(adServer);
    this.adServer = adServer;
    adServer.start();
  }

  /**
   * Creates an instance of {@link ActiveDirectoryClient} if configured. Returns {@link
   * Optional#empty} otherwise.
   *
   * @return optional instance of {@link ActiveDirectoryClient}
   * @throws IOException if creation of instance fails
   */
  static Optional<ActiveDirectoryClient> fromConfiguration() throws IOException {
    return fromConfiguration((env) -> new InitialLdapContext(env, null));
  }

  @VisibleForTesting
  static Optional<ActiveDirectoryClient> fromConfiguration(LdapContextBuilder contextBuilder)
      throws IOException {
    checkState(Configuration.isInitialized(), "Configuration not initialized yet");
    String host = Configuration.getString("adLookup.host", "").get();
    if (Strings.isNullOrEmpty(host)) {
      log.config("AD lookup not configured");
      return Optional.empty();
    }
    int port = Configuration.getInteger("adLookup.port", 389).get();
    Configuration.checkConfiguration(port > 0, "Invalid port %s for AD lookup", port);
    String username = Configuration.getString("adLookup.username", null).get();
    String password = Configuration.getString("adLookup.password", null).get();
    String method = Configuration.getString("adLookup.method", "standard").get();
    return Optional.of(
        new ActiveDirectoryClient(
            new ADServerImpl(host, port, username, password, method, contextBuilder)));
  }

  interface ADServer {
    /*
     * Resolves input SID to user account name. Returns {@link Optional.empty} if SID is not
     * available.
     */
    Optional<String> getUserAccountBySid(String sid) throws IOException;

    /*
     * Resolves input principal to user email. Returns {@link Optional.empty}
     * if principal is not available.
     */
    Optional<String> getEmailByPrincipal(ActiveDirectoryPrincipal principal) throws IOException;

    /*
     * Initializes LDAP Context and verifies that successful connection can
     * established with AD server using provided connection properties.
     */
    void start() throws IOException;
  }

  interface LdapContextBuilder {
    LdapContext buildContext(Hashtable<String, String> env) throws NamingException;
  }

  private static class AdServerConfiguration {
    private final String dn;
    private final String dnsRoot;
    private final String netbiosName;

    private AdServerConfiguration(String dn, String dnsRoot, String netbiosName) {
      checkArgument(!Strings.isNullOrEmpty(dn), "dn can not be null or empty");
      this.dn = dn;
      checkArgument(!Strings.isNullOrEmpty(dnsRoot), "dnsRoot can not be null or empty");
      this.dnsRoot = dnsRoot;
      checkArgument(!Strings.isNullOrEmpty(netbiosName), "nETBIOSName can not be null or empty");
      this.netbiosName = netbiosName;
    }
  }

  static class ADServerImpl implements ADServer {
    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final String protocol;
    private final SearchControls searchCtls;
    private final String[] attributes = {
      ATTR_SAMACCOUNTNAME, ATTR_NAME, ATTR_MAIL, ATTR_DNSROOT, ATTR_NETBIOSNAME
    };
    private final LdapContextBuilder contextBuilder;

    private volatile LdapContext context;
    private final AtomicReference<AdServerConfiguration> serverConfiguration =
        new AtomicReference<>();

    ADServerImpl(
        String host,
        int port,
        String username,
        String password,
        String method,
        LdapContextBuilder contextBuilder) {
      checkNotNull(host);
      checkArgument(!("".equals(host)));
      checkNotNull(username);
      checkArgument(!("".equals(username)));
      checkNotNull(password);
      checkArgument(!("".equals(password)));
      checkArgument(port > 0);
      this.host = host;
      this.port = port;
      this.username = username;
      this.password = password;
      this.protocol = "ssl".equalsIgnoreCase(method) ? "ldaps" : "ldap";
      this.searchCtls = new SearchControls();
      searchCtls.setSearchScope(SearchControls.SUBTREE_SCOPE);
      searchCtls.setReturningAttributes(attributes);
      this.contextBuilder = checkNotNull(contextBuilder);
    }

    @Override
    public Optional<String> getUserAccountBySid(String sid) throws IOException {
      try {
        Optional<SearchResult> results = getSidLookupResult(sid);
        if (!results.isPresent()) {
          return Optional.empty();
        }
        SearchResult sr = results.get();
        Attributes attrbs = sr.getAttributes();
        // use sAMAccountName when available
        String sAMAccountName = (String) getAttribute(attrbs, ATTR_SAMACCOUNTNAME);
        if (!Strings.isNullOrEmpty(sAMAccountName)) {
          return Optional.of(sAMAccountName);
        }
        log.log(Level.FINER, "sAMAccountName is null for SID {0}. This might"
            + " be domain object.", sid);
        String name = (String) getAttribute(attrbs, ATTR_NAME);
        if (Strings.isNullOrEmpty(name)) {
          log.log(Level.WARNING, "name is null for SID {0}. Returing empty.", sid);
          return Optional.empty();
        }
        return Optional.of(name);
      } catch (NamingException ne) {
        throw new IOException(ne);
      }
    }

    @Override
    public Optional<String> getEmailByPrincipal(ActiveDirectoryPrincipal principal)
        throws IOException {
      if (principal.getFormat() == PrincipalFormat.NETBIOS) {
        if (!principal.getDomain().equalsIgnoreCase(serverConfiguration.get().netbiosName)) {
          log.log(
              Level.WARNING,
              "NETBIOS mismatch for resolving principal {0}. Expected {1}. Returing empty.",
              new Object[] {principal, serverConfiguration.get().netbiosName});
          return Optional.empty();
        }
        try {
          return getEmailFromSearchResult(
              getSAMAccountNameLookupResult(principal.getName()),
              ATTR_SAMACCOUNTNAME,
              principal.getName());
        } catch (NamingException ne) {
          throw new IOException(ne);
        }
      } else if (principal.getFormat() == PrincipalFormat.DNS) {
        if (!principal.getDomain().equalsIgnoreCase(serverConfiguration.get().dnsRoot)) {
          log.log(
              Level.WARNING,
              "DnsRoot mismatch for resolving principal {0}. Returing empty.",
              principal);
          return Optional.empty();
        }
        try {
          String lookupValue =
              PrincipalFormat.DNS.format(principal.getName(), principal.getDomain());
          return getEmailFromSearchResult(
              getUPNLookupResult(lookupValue), "userPrincipalName", lookupValue);
        } catch (NamingException ne) {
          throw new IOException(ne);
        }
      } else if (principal.getFormat() == PrincipalFormat.NONE) {
        try {
          return getEmailFromSearchResult(
              getSAMAccountNameLookupResult(principal.getName()),
              ATTR_SAMACCOUNTNAME,
              principal.getName());
        } catch (NamingException ne) {
          throw new IOException(ne);
        }
      }
      return Optional.empty();
    }

    private Optional<String> getEmailFromSearchResult(
        Optional<SearchResult> results, String lookupField, String lookupValue)
        throws NamingException {
      if (!results.isPresent()) {
        return Optional.empty();
      }
      SearchResult sr = results.get();
      Attributes attrbs = sr.getAttributes();
      String email = (String) getAttribute(attrbs, ATTR_MAIL);
      if (Strings.isNullOrEmpty(email)) {
        log.log(
            Level.WARNING,
            "email is null for {0} = {1}. Returing empty.",
            new Object[] {lookupField, lookupValue});
        return Optional.empty();
      }
      return Optional.of(email);
    }

    @Override
    public void start() throws IOException {
      initializeContext();
      refreshConnection();
    }

    private Optional<SearchResult> getSidLookupResult(String sid)
        throws NamingException, IOException {
      validateSid(sid);
      String query = String.format("(objectSid=%s)", sid);
      return getSearchResult(query);
    }

    private Optional<SearchResult> getSAMAccountNameLookupResult(String sAMAccountName)
        throws NamingException, IOException {
      String query =
          String.format(
              "(&(objectCategory=person)(objectClass=user)(sAMAccountName=%s))", sAMAccountName);
      return getSearchResult(query);
    }

    private Optional<SearchResult> getUPNLookupResult(String userPrincipalName)
        throws NamingException, IOException {
      String query =
          String.format(
              "(&(objectCategory=person)(objectClass=user)(userPrincipalName=%s))",
              userPrincipalName);
      return getSearchResult(query);
    }

    private Optional<SearchResult> getSearchResult(String query)
        throws IOException, NamingException {
      refreshConnection();
      // Use search base as empty when querying using global catalog
      String searchBase = (port == 389 || port == 636) ? serverConfiguration.get().dn : "";
      log.log(
          Level.FINE,
          "Querying host {0} on port {1,number,#} with query {2} and search base {3}",
          new Object[] {host, port, query, searchBase});
      NamingEnumeration<SearchResult> results = executeQuery(query, searchBase);
      if (!results.hasMoreElements()) {
        log.log(
            Level.WARNING,
            "No result found on host {0} on port {1,number,#} with query {2} and search base {3}."
                + " Returing empty.",
            new Object[] {host, port, query, searchBase});
        return Optional.empty();
      }
      return Optional.of(results.next());
    }

    private NamingEnumeration<SearchResult> executeQuery(String query, String searchBase)
        throws NamingException {
      return context.search(searchBase, query, searchCtls);
    }

    private synchronized void initializeContext() throws IOException {
      // Check if current context is still useful by calling
      // context.getAttributes.
      if (context != null) {
        try {
          context.getAttributes("");
          return;
        } catch (NamingException ignore) {
          ignore = null;
        }
      }
      Hashtable<String, String> env = new Hashtable<String, String>();
      env.put(Context.INITIAL_CONTEXT_FACTORY,
          "com.sun.jndi.ldap.LdapCtxFactory");
      env.put("com.sun.jndi.ldap.read.timeout", "90000");
      env.put(Context.SECURITY_AUTHENTICATION, "simple");
      env.put(Context.SECURITY_PRINCIPAL, username);
      env.put(Context.SECURITY_CREDENTIALS, password);
      String ldapUrl = String.format("%s://%s:%d", protocol, host, port);
      env.put(Context.PROVIDER_URL, ldapUrl);
      try {
        context = contextBuilder.buildContext(env);
      } catch (NamingException ne) {
        throw new IOException(ne);
      }
    }

    private void refreshConnection() throws IOException {
      refreshConnection(true);
    }

    private void refreshConnection(boolean retry) throws IOException {
      if (context == null) {
        throw new IOException("LDAP Context not initialized.");
      }
      try {
        Attributes attributes = context.getAttributes("");
        if (serverConfiguration.get() != null) {
          return;
        }
        String defaultNamingContext = (String) getAttribute(attributes, ATTR_DEFAULTNAMINGCONTEXT);
        if (Strings.isNullOrEmpty(defaultNamingContext)) {
          throw new IOException("Default naming context is null or empty");
        }
        String configurationContext =
            (String) getAttribute(attributes, ATTR_CONFIGURATIONNAMINGCONTEXT);
        if (Strings.isNullOrEmpty(configurationContext)) {
          throw new IOException("Configuration naming context is null or empty");
        }
        serverConfiguration.set(
            getAdServerConfiguration(defaultNamingContext, configurationContext));
      } catch (CommunicationException ce) {
        if (retry) {
          log.log(
              Level.INFO,
              "Error refreshing LDAP connection to host {0}"
                  + " on port {1,number,#} for SID lookup. Retrying.",
              new Object[] {host, port});
          initializeContext();
          refreshConnection(false);
        } else {
          throw new IOException(ce);
        }
      } catch (NamingException ne) {
        if (retry) {
          log.log(
              Level.INFO,
              "Error refreshing LDAP connection to host {0}"
                  + " on port {1,number,#} for SID lookup. Retrying.",
              new Object[] {host, port});
          initializeContext();
          refreshConnection(false);
        } else {
          throw new IOException(ne);
        }
      }
    }

    private AdServerConfiguration getAdServerConfiguration(
        String defaultNamingContext, String configurationContext) throws NamingException {
      String query = String.format("(ncName=%s)", defaultNamingContext);
      NamingEnumeration<SearchResult> ldapResults = executeQuery(query, configurationContext);
      if (!ldapResults.hasMore()) {
        throw new NamingException(
            "Naming Configuration is not available for dn " + defaultNamingContext);
      }
      SearchResult result = ldapResults.next();
      Attributes attributes = result.getAttributes();
      String nETBIOSNameFromResult = (String) getAttribute(attributes, ATTR_NETBIOSNAME);
      String dnsRootNameFromResult = (String) getAttribute(attributes, ATTR_DNSROOT);
      return new AdServerConfiguration(
          defaultNamingContext, dnsRootNameFromResult, nETBIOSNameFromResult);
    }

    private Object getAttribute(Attributes attributes, String name)
        throws NamingException {
      Attribute attribute = attributes.get(name);
      if (attribute != null) {
        return attribute.get(0);
      } else {
        return null;
      }
    }
  }
}

