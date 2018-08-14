package com.google.enterprise.cloudsearch.sharepoint;

import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

class NtlmAuthenticator extends Authenticator {
  private final String username;
  private final char[] password;
  private final Set<String> permittedHosts = new HashSet<String>();

  NtlmAuthenticator(String username, String password) {
    this.username = username;
    this.password = password.toCharArray();
  }

  void addPermitForHost(URL urlContainingHost) {
    permittedHosts.add(urlToHostString(urlContainingHost));
  }

  boolean isPermittedHost(URL toVerify) {
    return permittedHosts.contains(urlToHostString(toVerify));
  }

  private String urlToHostString(URL url) {
    // If the port is missing (so that the default is used), we replace it
    // with the default port for the protocol in order to prevent being able
    // to prevent being tricked into connecting to a different port (consider
    // being configured for https, but then getting tricked to use http and
    // everything being in the clear).
    return "" + url.getHost() + ":" + (url.getPort() != -1 ? url.getPort() : url.getDefaultPort());
  }

  @Override
  protected PasswordAuthentication getPasswordAuthentication() {
    URL url = getRequestingURL();
    if (isPermittedHost(url)) {
      return new PasswordAuthentication(username, password);
    } else {
      return super.getPasswordAuthentication();
    }
  }
}
