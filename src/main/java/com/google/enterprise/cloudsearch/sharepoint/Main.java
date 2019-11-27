/*
 * Copyright 2019 Google LLC
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

/**
 * Runs SharePoint or identity connector based on command-line argument. With no argument,
 * runs the sharepoint connector.
 *
 * Usage:
 * <pre>
 * java -jar connector.jar --sharepoint
 * java -jar connector.jar --identity
 * </pre>
 */
public class Main {
  public static void main(String[] args) throws InterruptedException {
    boolean sharepoint = false;
    boolean identity = false;
    for (String arg : args) {
      if (arg.equals("--sharepoint")) {
        sharepoint = true;
      } else if (arg.equals("--identity")) {
        identity = true;
      }
    }
    if (sharepoint && identity) {
      System.out.println(
          "Invalid options; only one of --sharepoint and --identity may be specified");
      return;
    }
    if (identity) {
      SharePointIdentityConnector.main(args);
    } else {
      SharePointRepositoryConnector.main(args);
    }
  }
}
