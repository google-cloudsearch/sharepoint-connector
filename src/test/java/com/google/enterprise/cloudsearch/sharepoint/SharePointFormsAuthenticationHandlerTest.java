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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.enterprise.cloudsearch.sharepoint.FormsAuthenticationHandlerTest.UnsupportedScheduledExecutor;
import com.microsoft.schemas.sharepoint.soap.authentication.AuthenticationMode;
import com.microsoft.schemas.sharepoint.soap.authentication.AuthenticationSoap;
import com.microsoft.schemas.sharepoint.soap.authentication.LoginErrorCode;
import com.microsoft.schemas.sharepoint.soap.authentication.LoginResult;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.ws.Binding;
import javax.xml.ws.BindingProvider;
import javax.xml.ws.EndpointReference;
import javax.xml.ws.handler.MessageContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for {@link SharePointFormsAuthenticationHandler} */
public class SharePointFormsAuthenticationHandlerTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static class UnsupportedAuthenticationSoap
      implements AuthenticationSoap {

    @Override
    public LoginResult login(String username, String password) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AuthenticationMode mode() {
      throw new UnsupportedOperationException();
    }
  }

  private static class MockFormsAuthenticationSoap
      extends UnsupportedAuthenticationSoap implements BindingProvider {
    @Override
    public LoginResult login(String username, String password) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AuthenticationMode mode() {
      return AuthenticationMode.FORMS;
    }

    @Override
    public Map<String, Object> getRequestContext() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> getResponseContext() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Binding getBinding() {
      throw new UnsupportedOperationException();
    }

    @Override
    public EndpointReference getEndpointReference() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T extends EndpointReference> T
        getEndpointReference(Class<T> clazz) {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  public void testBuilder() {
    new SharePointFormsAuthenticationHandler.Builder("username", "password",
        new UnsupportedScheduledExecutor(),
        new UnsupportedAuthenticationSoap()).build();
  }

  @Test
  public void testNullUserName() {
    thrown.expect(NullPointerException.class);
    new SharePointFormsAuthenticationHandler.Builder(null, "password",
        new UnsupportedScheduledExecutor(),
        new UnsupportedAuthenticationSoap()).build();
  }

  @Test
  public void testNullPassword() {
    thrown.expect(NullPointerException.class);
    new SharePointFormsAuthenticationHandler.Builder("username", null,
        new UnsupportedScheduledExecutor(),
        new UnsupportedAuthenticationSoap()).build();
  }

  @Test
  public void testNullAuthenticationClient() {
    thrown.expect(NullPointerException.class);
    new SharePointFormsAuthenticationHandler.Builder("username", "password",
        new UnsupportedScheduledExecutor(), null).build();
  }

  @Test
  public void testSharePointWithWindowsAuthentication() throws IOException{
    SharePointFormsAuthenticationHandler authenHandler
        = new SharePointFormsAuthenticationHandler.Builder("username",
            "password", new UnsupportedScheduledExecutor(),
            new MockFormsAuthenticationSoap(){
                @Override public AuthenticationMode mode() {
                  return AuthenticationMode.WINDOWS;
                }
            }).build();

    assertFalse(authenHandler.isFormsAuthentication());
    AuthenticationResult ar = authenHandler.authenticate();
    assertNotNull(ar);
    assertNull(ar.getCookie());
    assertEquals(LoginErrorCode.NOT_IN_FORMS_AUTHENTICATION_MODE.toString(),
        ar.getErrorCode());
  }

  @Test
  public void testSharePointWithFormsPasswordMismatch() throws IOException {
    SharePointFormsAuthenticationHandler authenHandler
        = new SharePointFormsAuthenticationHandler.Builder("username",
            "password", new UnsupportedScheduledExecutor(),
            new MockFormsAuthenticationSoap(){
                @Override public LoginResult login(
                    String username, String password) {
                  LoginResult lr = new LoginResult();
                  lr.setErrorCode(LoginErrorCode.PASSWORD_NOT_MATCH);
                  return lr;
                }
            }).build();

    assertTrue(authenHandler.isFormsAuthentication());
    AuthenticationResult ar = authenHandler.authenticate();
    assertNotNull(ar);
    assertNull(ar.getCookie());
    assertEquals(LoginErrorCode.PASSWORD_NOT_MATCH.toString(),
        ar.getErrorCode());
  }

  @Test
  public void testSharePointWithFormsAuthentication() throws IOException {
    SharePointFormsAuthenticationHandler authenHandler
        = new SharePointFormsAuthenticationHandler.Builder("username",
            "password", new UnsupportedScheduledExecutor(),
            new MockFormsAuthenticationSoap(){
              @Override public LoginResult login(
                    String username, String password) {
                LoginResult lr = new LoginResult();
                lr.setErrorCode(LoginErrorCode.NO_ERROR);
                return lr;
              }

              @Override public Map<String, Object> getResponseContext() {
                Map<String, Object> responseContext
                    = new HashMap<String, Object>();
                Map<String, List<String>> responseHeaders
                    = new HashMap<String, List<String>>();
                responseHeaders.put("Set-cookie",
                    Arrays.asList("AuthenticationCookie"));
                responseContext.put(MessageContext.HTTP_RESPONSE_HEADERS,
                    Collections.unmodifiableMap(responseHeaders));
                return  Collections.unmodifiableMap(responseContext);
              }
            }).build();
    assertTrue(authenHandler.isFormsAuthentication());
    AuthenticationResult ar = authenHandler.authenticate();
    assertNotNull(ar);
    assertEquals("AuthenticationCookie", ar.getCookie());
    assertEquals(LoginErrorCode.NO_ERROR.toString(),
        ar.getErrorCode());
  }
}
