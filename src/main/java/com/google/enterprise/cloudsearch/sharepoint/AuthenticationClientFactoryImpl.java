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

import com.google.common.base.Strings;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.StartupException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.Parser;
import com.google.enterprise.cloudsearch.sharepoint.SamlAuthenticationHandler.SamlHandshakeManager;
import com.microsoft.schemas.sharepoint.soap.authentication.AuthenticationSoap;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ScheduledExecutorService;
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
    LIVE,
    CUSTOM
  }

  public AuthenticationClientFactoryImpl() {
    this.authenticationService =
        Service.create(
            AuthenticationClientFactoryImpl.class.getResource("wsdl/Authentication.wsdl"),
            new QName(XMLNS, "Authentication"));
  }

  private static String handleEncoding(String endpoint) {
    // Handle Unicode. Java does not properly encode the POST path.
    return URI.create(endpoint).toASCIIString();
  }

  private AuthenticationSoap getAuthenticationSoap(String virtualServer) {
    String authenticationEndPoint = String.format("%s/_vti_bin/Authentication.asmx", virtualServer);
    EndpointReference endpointRef =
        new W3CEndpointReferenceBuilder().address(handleEncoding(authenticationEndPoint)).build();
    return authenticationService.getPort(endpointRef, AuthenticationSoap.class);
  }

  private SamlHandshakeManager getAdfsHandshakeManager(
      String virtualServer, String username, String password) {
    String stsendpoint = Configuration.getString("sharepoint.sts.endpoint", null).get();
    String stsrealm = Configuration.getString("sharepoint.sts.realm", null).get();
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

  private FormsAuthenticationHandler getCustomFormsAuthenticationHandler(
      String username, String password, ScheduledExecutorService executor) {
    log.config("Connector configured to use custom forms authentication provider.");
    String factoryMethodName =
        Configuration.getString("formsAuthenticationHadler.factoryMethod", null).get();
    Configuration.checkConfiguration(
        !Strings.isNullOrEmpty(factoryMethodName), "Factory method can not be empty");
    int sepIndex = factoryMethodName.lastIndexOf(".");
    if (sepIndex == -1) {
      throw new InvalidConfigurationException(
          "Could not separate method name from class name: " + factoryMethodName);
    }
    log.log(Level.CONFIG, "Custom FormsAuthenticationHandler Factory [{0}]", factoryMethodName);
    String className = factoryMethodName.substring(0, sepIndex);
    String methodName = factoryMethodName.substring(sepIndex + 1);
    log.log(
        Level.FINE,
        "Split {0} into class {1} and method {2}",
        new Object[] {factoryMethodName, className, methodName});
    Class<?> klass;
    try {
      klass = Class.forName(className);
    } catch (ClassNotFoundException ex) {
      throw new InvalidConfigurationException(
          "Could not load class for FormsAuthenticationHandler: " + className, ex);
    }
    Method method;
    try {
      method =
          klass.getDeclaredMethod(
              methodName, String.class, String.class, ScheduledExecutorService.class);
    } catch (NoSuchMethodException ex) {
      throw new InvalidConfigurationException(
          "Could not find method: "
              + methodName
              + " on class: "
              + className
              + "with signature String, String, ScheduledExecutorService",
          ex);
    }

    log.log(Level.FINE, "Found method {0}", method);
    Object o;
    try {
      o = method.invoke(null, username, password, executor);
    } catch (Exception ex) {
      throw new RuntimeException("Failure while running factory method: " + factoryMethodName, ex);
    }
    if (!(o instanceof FormsAuthenticationHandler)) {
      throw new StartupException(
          o.getClass().getName() + " is not an instance of FormsAuthenticationHandler");
    }
    return (FormsAuthenticationHandler) o;
  }

  @Override
  public FormsAuthenticationHandler getFormsAuthenticationHandler(
      String virtualServer, String username, String password, ScheduledExecutorService executor) {
    String rootUrl;
    try {
      SharePointUrl configuredUrl = new SharePointUrl.Builder(virtualServer).build();
      rootUrl = configuredUrl.getRootUrl();
    } catch (URISyntaxException e) {
      throw new InvalidConfigurationException("failed to parse SharePoint URL.", e);
    }
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
        return null;
      case FORMS:
        return new SharePointFormsAuthenticationHandler.Builder(
                username, password, executor, getAuthenticationSoap(virtualServer))
            .build();
      case ADFS:
        return new SamlAuthenticationHandler.Builder(
                username,
                password,
                executor,
                getAdfsHandshakeManager(virtualServer, username, password))
            .build();
      case LIVE:
        return new SamlAuthenticationHandler.Builder(
                username,
                password,
                executor,
                new LiveAuthenticationHandshakeManager.Builder(rootUrl, username, password).build())
            .build();
      case CUSTOM:
        return getCustomFormsAuthenticationHandler(username, password, executor);
      default:
        throw new IllegalStateException("unsupported AuthenticationMode " + authenticationMode);
    }
  }
}
