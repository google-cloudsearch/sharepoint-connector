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
