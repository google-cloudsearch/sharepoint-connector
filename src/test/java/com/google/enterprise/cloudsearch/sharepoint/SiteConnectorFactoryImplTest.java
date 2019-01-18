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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.ImmutableMap;
import com.google.enterprise.cloudsearch.sharepoint.SharePointConfiguration.SharePointDeploymentType;
import com.google.enterprise.cloudsearch.sharepoint.SiteConnectorFactory.SoapFactory;
import com.google.enterprise.cloudsearch.sharepoint.SiteConnectorFactoryImpl.SoapFactoryImpl;
import java.io.IOException;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Unit tests for validating {@link SiteConnectorFactoryImpl} */
@RunWith(MockitoJUnitRunner.class)
public class SiteConnectorFactoryImplTest {

  @Mock private SharePointRequestContext requestContext;

  @Test
  public void testMinimumBuilder() throws Exception {
    SiteConnectorFactoryImpl factory =
        new SiteConnectorFactoryImpl.Builder()
            .setRequestContext(requestContext)
            .setReferenceIdentitySourceConfiguration(ImmutableMap.of())
            .build();
    assertNotNull(factory.getInstance("http://sp.com", "http://sp.com"));
  }

  @Test
  public void testBuilderWithNonDefault() throws IOException {
    SoapFactory spySoapFactory = spy(new SoapFactoryImpl());
    SiteConnectorFactoryImpl factory =
        new SiteConnectorFactoryImpl.Builder()
            .setSoapFactory(spySoapFactory)
            .setRequestContext(requestContext)
            .setReferenceIdentitySourceConfiguration(ImmutableMap.of())
            .setSharePointDeploymentType(SharePointDeploymentType.ONLINE)
            .setStripDomainInUserPrincipals(false)
            .setXmlValidation(false)
            .setActiveDirectoryClient(Optional.of(mock(ActiveDirectoryClient.class)))
            .build();
    SiteConnector instance = factory.getInstance("http://sp.com", "http://sp.com/web");
    assertNotNull(instance);
    // get Instance second time. should be from cache
    assertSame(instance, factory.getInstance("http://sp.com", "http://sp.com/web"));
    // url with trailing slash
    assertSame(instance, factory.getInstance("http://sp.com/", "http://sp.com/web/"));

    verify(spySoapFactory, times(1)).newSiteData("http://sp.com/web/_vti_bin/SiteData.asmx");
    verify(spySoapFactory, times(1)).newUserGroup("http://sp.com/_vti_bin/UserGroup.asmx");
    verify(spySoapFactory, times(1)).newPeople("http://sp.com/_vti_bin/People.asmx");
    verifyNoMoreInteractions(spySoapFactory);

    verify(requestContext, times(3)).addContext(any());
    verifyNoMoreInteractions(requestContext);
  }
}
