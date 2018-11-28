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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.StartupException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Unit tests for {@link AuthenticationClientFactoryImpl} */
@RunWith(MockitoJUnitRunner.class)
public class AuthenticationClientFactoryImplTest {

  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock ScheduledExecutorService executor;

  private static final String CUSTOM_FORMS_AUTH_HANDLER_FACTORY =
      CustomFormsAuthenticationHandler.class.getName() + ".getInstance";

  @Test
  public void testInvalidFormsAuthenticationMode() {
    Properties config = new Properties();
    config.put("sharepoint.formsAuthenticationMode", "UNKNOWN");
    setupConfig.initConfig(config);
    AuthenticationClientFactory factory = new AuthenticationClientFactoryImpl();
    thrown.expect(InvalidConfigurationException.class);
    factory.getFormsAuthenticationHandler("http://sp.com", "username", "password", executor);
  }

  @Test
  public void testFormsAuthenticationModeNone() {
    Properties config = new Properties();
    config.put("sharepoint.formsAuthenticationMode", "NONE");
    setupConfig.initConfig(config);
    AuthenticationClientFactory factory = new AuthenticationClientFactoryImpl();
    assertNull(
        factory.getFormsAuthenticationHandler("http://sp.com", "username", "password", executor));
  }

  @Test
  public void testFormsAuthenticationModeSharePointForms() {
    Properties config = new Properties();
    config.put("sharepoint.formsAuthenticationMode", "FORMS");
    setupConfig.initConfig(config);
    AuthenticationClientFactory factory = new AuthenticationClientFactoryImpl();
    FormsAuthenticationHandler formsAuthenticationHandler =
        factory.getFormsAuthenticationHandler("http://sp.com", "username", "password", executor);
    assertThat(formsAuthenticationHandler, instanceOf(SharePointFormsAuthenticationHandler.class));
  }

  @Test
  public void testFormsAuthenticationModeLive() {
    Properties config = new Properties();
    config.put("sharepoint.formsAuthenticationMode", "LIVE");
    setupConfig.initConfig(config);
    AuthenticationClientFactory factory = new AuthenticationClientFactoryImpl();
    FormsAuthenticationHandler formsAuthenticationHandler =
        factory.getFormsAuthenticationHandler("http://sp.com", "username", "password", executor);
    assertThat(formsAuthenticationHandler, instanceOf(SamlAuthenticationHandler.class));
  }

  @Test
  public void testFormsAuthenticationModeAdfs() {
    Properties config = new Properties();
    config.put("sharepoint.formsAuthenticationMode", "ADFS");
    config.put("sharepoint.sts.endpoint", "endpoint");
    config.put("sharepoint.sts.realm", "urn:realm");
    setupConfig.initConfig(config);
    AuthenticationClientFactory factory = new AuthenticationClientFactoryImpl();
    FormsAuthenticationHandler formsAuthenticationHandler =
        factory.getFormsAuthenticationHandler("http://sp.com", "username", "password", executor);
    assertThat(formsAuthenticationHandler, instanceOf(SamlAuthenticationHandler.class));
  }

  @Test
  public void testCustomFormsAuthenticationHandler() {
    Properties config = new Properties();
    config.put("sharepoint.formsAuthenticationMode", "CUSTOM");
    config.put("formsAuthenticationHadler.factoryMethod", CUSTOM_FORMS_AUTH_HANDLER_FACTORY);
    setupConfig.initConfig(config);
    AuthenticationClientFactory factory = new AuthenticationClientFactoryImpl();
    FormsAuthenticationHandler handler =
        factory.getFormsAuthenticationHandler("http://sp.com", "username", "password", executor);
    assertTrue(handler instanceof CustomFormsAuthenticationHandler);
  }

  @Test
  public void testCustomFormsAuthenticationHandlerMissingFactoryMethod() {
    Properties config = new Properties();
    config.put("sharepoint.formsAuthenticationMode", "CUSTOM");
    setupConfig.initConfig(config);
    AuthenticationClientFactory factory = new AuthenticationClientFactoryImpl();
    thrown.expect(InvalidConfigurationException.class);
    factory.getFormsAuthenticationHandler("http://sp.com", "username", "password", executor);
  }

  @Test
  public void testCustomFormsAuthenticationHandlerClassNotFound() {
    Properties config = new Properties();
    config.put("sharepoint.formsAuthenticationMode", "CUSTOM");
    config.put("formsAuthenticationHadler.factoryMethod", "NoClassHere.method");
    setupConfig.initConfig(config);
    AuthenticationClientFactory factory = new AuthenticationClientFactoryImpl();
    thrown.expect(InvalidConfigurationException.class);
    factory.getFormsAuthenticationHandler("http://sp.com", "username", "password", executor);
  }

  @Test
  public void testCustomFormsAuthenticationHandlerNoMethod() {
    Properties config = new Properties();
    config.put("sharepoint.formsAuthenticationMode", "CUSTOM");
    config.put(
        "formsAuthenticationHadler.factoryMethod",
        CustomFormsAuthenticationHandler.class.getName());
    setupConfig.initConfig(config);
    AuthenticationClientFactory factory = new AuthenticationClientFactoryImpl();
    thrown.expect(InvalidConfigurationException.class);
    factory.getFormsAuthenticationHandler("http://sp.com", "username", "password", executor);
  }

  @Test
  public void testCustomFormsAuthenticationHandlerMethodWithWrongSignature() {
    Properties config = new Properties();
    config.put("sharepoint.formsAuthenticationMode", "CUSTOM");
    config.put(
        "formsAuthenticationHadler.factoryMethod",
        CustomFormsAuthenticationHandler.class.getName() + ".somethingElse");
    setupConfig.initConfig(config);
    AuthenticationClientFactory factory = new AuthenticationClientFactoryImpl();
    thrown.expect(InvalidConfigurationException.class);
    factory.getFormsAuthenticationHandler("http://sp.com", "username", "password", executor);
  }

  @Test
  public void testCustomFormsAuthenticationHandlerMethodWithWrongReturnType() {
    Properties config = new Properties();
    config.put("sharepoint.formsAuthenticationMode", "CUSTOM");
    config.put(
        "formsAuthenticationHadler.factoryMethod",
        CustomFormsAuthenticationHandler.class.getName() + ".getStringInstance");
    setupConfig.initConfig(config);
    AuthenticationClientFactory factory = new AuthenticationClientFactoryImpl();
    thrown.expect(StartupException.class);
    factory.getFormsAuthenticationHandler("http://sp.com", "username", "password", executor);
  }

  @Test
  public void testCustomFormsAuthenticationHandlerWithErrorCreatingInstance() {
    Properties config = new Properties();
    config.put("sharepoint.formsAuthenticationMode", "CUSTOM");
    config.put(
        "formsAuthenticationHadler.factoryMethod",
        CustomFormsAuthenticationHandler.class.getName() + ".getInstanceError");
    setupConfig.initConfig(config);
    AuthenticationClientFactory factory = new AuthenticationClientFactoryImpl();
    thrown.expect(RuntimeException.class);
    factory.getFormsAuthenticationHandler("http://sp.com", "username", "password", executor);
  }

  @Test
  public void testCustomFormsAuthenticationHandlerWithInstanceMethod() {
    Properties config = new Properties();
    config.put("sharepoint.formsAuthenticationMode", "CUSTOM");
    config.put(
        "formsAuthenticationHadler.factoryMethod",
        CustomFormsAuthenticationHandler.class.getName() + ".getInstanceMethod");
    setupConfig.initConfig(config);
    AuthenticationClientFactory factory = new AuthenticationClientFactoryImpl();
    thrown.expect(RuntimeException.class);
    factory.getFormsAuthenticationHandler("http://sp.com", "username", "password", executor);
  }

  private static class CustomFormsAuthenticationHandler extends FormsAuthenticationHandler {

    private CustomFormsAuthenticationHandler(
        String username, String password, ScheduledExecutorService executor) {
      super(username, password, executor);
    }

    @Override
    public boolean isFormsAuthentication() throws IOException {
      return true;
    }

    @Override
    public AuthenticationResult authenticate() throws IOException {
      return null;
    }

    FormsAuthenticationHandler getInstanceMethod(
        String username, String password, ScheduledExecutorService executor) {
      return new CustomFormsAuthenticationHandler(username, password, executor);
    }

    static FormsAuthenticationHandler getInstance(
        String username, String password, ScheduledExecutorService executor) {
      return new CustomFormsAuthenticationHandler(username, password, executor);
    }

    static FormsAuthenticationHandler getInstanceError(
        String username, String password, ScheduledExecutorService executor) throws IOException {
      throw new IOException("some thing went wrong");
    }

    static void somethingElse() {}

    static String getStringInstance(
        String username, String password, ScheduledExecutorService executor) {
      return "string instance";
    }
  }
}
