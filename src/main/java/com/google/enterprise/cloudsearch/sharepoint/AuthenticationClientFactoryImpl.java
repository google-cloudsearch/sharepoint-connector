// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.enterprise.cloudsearch.sharepoint;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Strings;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.Parser;
import com.google.enterprise.cloudsearch.sharepoint.SamlAuthenticationHandler.SamlHandshakeManager;
import com.microsoft.schemas.sharepoint.soap.authentication.AuthenticationSoap;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.namespace.QName;
import javax.xml.ws.EndpointReference;
import javax.xml.ws.Service;
import javax.xml.ws.wsaddressing.W3CEndpointReferenceBuilder;

/**
 * Authentication Factory implementation to return appropriate authentication client for
 * FormsAuthenticationHandler implementation.
 */
class AuthenticationClientFactoryImpl implements AuthenticationClientFactory {
  /** SharePoint's namespace. */
  private static final String XMLNS = "http://schemas.microsoft.com/sharepoint/soap/";

  private static final Logger log =
      Logger.getLogger(AuthenticationClientFactoryImpl.class.getName());

  private final Service authenticationService;
  private final AtomicReference<Optional<FormsAuthenticationHandler>> authenticationHandler;

  private static final Parser<FormsAuthenticationMode> AUTH_MODE_PARSER =
      value -> {
        try {
          return FormsAuthenticationMode.valueOf(value);
        } catch (IllegalArgumentException e) {
          throw new InvalidConfigurationException("Invalid FormsAuthenticationMode " + value, e);
        }
      };

  enum FormsAuthenticationMode {
    NONE,
    FORMS,
    ADFS,
    LIVE
  }

  public AuthenticationClientFactoryImpl() {
    this.authenticationService =
        Service.create(
            AuthenticationClientFactoryImpl.class.getResource("wsdl/Authentication.wsdl"),
            new QName(XMLNS, "Authentication"));
    authenticationHandler = new AtomicReference<>();
  }

  private static String handleEncoding(String endpoint) {
    // Handle Unicode. Java does not properly encode the POST path.
    return URI.create(endpoint).toASCIIString();
  }

  private AuthenticationSoap getAuthenticationSoap(String virtualServer) {
    String authenticationEndPoint = String.format("%s/_vti_bin/Authentication.asmx", virtualServer);
    EndpointReference endpointRef =
        new W3CEndpointReferenceBuilder().address(handleEncoding(authenticationEndPoint)).build();
    authenticationService.getPort(endpointRef, AuthenticationSoap.class);
    return authenticationService.getPort(endpointRef, AuthenticationSoap.class);
  }

  private SamlHandshakeManager getAdfsHandshakeManager(
      String virtualServer, String username, String password) {
    String stsendpoint = Configuration.getString("sharepoint.stsendpoint", null).get();
    String stsrealm = Configuration.getString("sharepoint.stsendpoint", null).get();
    String login = Configuration.getString("sharepoint.adfsLogin", "").get();
    String trustlocation = Configuration.getString("sharepoint.trustLocation", "").get();
    AdfsHandshakeManager.Builder manager =
        new AdfsHandshakeManager.Builder(virtualServer, username, password, stsendpoint, stsrealm);
    if (!Strings.isNullOrEmpty(login)) {
      log.log(Level.CONFIG, "Using non default login value for ADFS [{0}]", login);
      manager.setLoginUrl(login);
    }
    if (!Strings.isNullOrEmpty(trustlocation)) {
      log.log(Level.CONFIG, "Using non default trust location for ADFS [{0}]", trustlocation);
      manager.setTrustLocation(trustlocation);
    }
    return manager.build();
  }

  @Override
  public FormsAuthenticationHandler getFormsAuthenticationHandler() {
    checkState(
        authenticationHandler.get() != null, "Authentication client factory not initialized yet");
    return authenticationHandler.get().orElse(null);
  }

  @Override
  public void init(
      String virtualServer, String username, String password, ScheduledExecutorService executor) {
    checkState(Configuration.isInitialized(), "Configuration not initialized");
    FormsAuthenticationMode authenticationMode =
        Configuration.getValue(
                "sharepoint.formsAuthenticationMode",
                FormsAuthenticationMode.NONE,
                AUTH_MODE_PARSER)
            .get();
    log.log(
        Level.CONFIG, "Connector configured with FormsAuthenticationMode {0}", authenticationMode);
    switch (authenticationMode) {
      case NONE:
        authenticationHandler.set(Optional.empty());
        break;
      case FORMS:
        SharePointFormsAuthenticationHandler formsHandler =
            new SharePointFormsAuthenticationHandler.Builder(
                    username, password, executor, getAuthenticationSoap(virtualServer))
                .build();
        authenticationHandler.set(Optional.of(formsHandler));
        break;
      case ADFS:
        SamlAuthenticationHandler adfsHandler =
            new SamlAuthenticationHandler.Builder(
                    username,
                    password,
                    executor,
                    getAdfsHandshakeManager(virtualServer, username, password))
                .build();
        authenticationHandler.set(Optional.of(adfsHandler));
        break;
      case LIVE:
        SamlAuthenticationHandler liveHandler =
            new SamlAuthenticationHandler.Builder(
                    username,
                    password,
                    executor,
                    new LiveAuthenticationHandshakeManager.Builder(
                            virtualServer, username, password)
                        .build())
                .build();
        authenticationHandler.set(Optional.of(liveHandler));
        break;
      default:
        throw new IllegalStateException("unsupported AuthenticationMode " + authenticationMode);
    }
  }
}
