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

import java.util.concurrent.ScheduledExecutorService;

/**
 * Authentication Factory to return appropriate authentication client for FormsAuthenticationHandler
 * implementation.
 */
interface AuthenticationClientFactory {
  /**
   * Initialize {@link AuthenticationClientFactory}
   *
   * @param virtualServer SharePoint virtual server URL
   * @param username SharePoint user account
   * @param password SharePoint user password
   * @param executor schedule executor service to periodically refresh authentication tokens
   */
  void init(
      String virtualServer, String username, String password, ScheduledExecutorService executor);

  /**
   * Get an instance of {@link FormsAuthenticationHandler}
   *
   * @return an instance of {@link FormsAuthenticationHandler}
   */
  FormsAuthenticationHandler getFormsAuthenticationHandler();
}
