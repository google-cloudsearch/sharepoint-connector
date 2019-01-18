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

import com.google.common.base.Objects;
import com.google.common.base.Strings;
/** Class to parse active directory principals and format conversion */
class ActiveDirectoryPrincipal {
  static enum PrincipalFormat {
    NONE,
    DNS,
    NETBIOS,
    ;

    String format(String plainName, String domain) {
      checkArgument(
          !Strings.isNullOrEmpty(domain) || this == PrincipalFormat.NONE,
          "Domain can not be empty if PrincipalFormat is not NONE");
      switch (this) {
        case NONE:
          return plainName;
        case DNS:
          return plainName + "@" + domain;
        case NETBIOS:
          return domain + "\\" + plainName;
        default:
          throw new AssertionError("Unsupported PrincipalFormat " + this);
      }
    }
  }

  private final String name;
  private final String domain;
  private final PrincipalFormat format;

  private ActiveDirectoryPrincipal(String name, String domain, PrincipalFormat format) {
    checkArgument(!Strings.isNullOrEmpty(name), "principal name can not be null or empty");
    this.name = name;
    checkArgument(
        !Strings.isNullOrEmpty(domain) || format == PrincipalFormat.NONE,
        "Domain can not be empty if PrincipalFormat is not NONE");
    this.domain = domain;
    this.format = format;
  }

  static ActiveDirectoryPrincipal parse(String principalNameToParse) {
    checkArgument(
        !Strings.isNullOrEmpty(principalNameToParse),
        "principal name to be parsed can not be null or empty");
    for (int i = 0; i < principalNameToParse.length(); i++) {
      char c = principalNameToParse.charAt(i);
      switch (c) {
        case '\\':
          return new ActiveDirectoryPrincipal(
              principalNameToParse.substring(i + 1),
              principalNameToParse.substring(0, i),
              PrincipalFormat.NETBIOS);
        case '@':
          return new ActiveDirectoryPrincipal(
              principalNameToParse.substring(0, i),
              principalNameToParse.substring(i + 1),
              PrincipalFormat.DNS);
        default:
      }
    }
    return new ActiveDirectoryPrincipal(principalNameToParse, "", PrincipalFormat.NONE);
  }

  String getName() {
    return name;
  }

  String getDomain() {
    return domain;
  }

  PrincipalFormat getFormat() {
    return format;
  }

  String getPrincipalNameInFormat(PrincipalFormat format) {
    return format.format(name, domain);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, domain, format);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof ActiveDirectoryPrincipal)) {
      return false;
    }
    ActiveDirectoryPrincipal other = (ActiveDirectoryPrincipal) obj;
    return Objects.equal(name, other.name)
        && Objects.equal(domain, other.domain)
        && Objects.equal(format, other.format);
  }

  @Override
  public String toString() {
    return "ActiveDirectoryPrincipal [name="
        + name
        + ", domain="
        + domain
        + ", format="
        + format
        + "]";
  }
}
