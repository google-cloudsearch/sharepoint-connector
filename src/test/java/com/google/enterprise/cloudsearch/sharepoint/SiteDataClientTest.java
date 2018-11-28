/*
 * Copyright © 2018 Google Inc.
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.google.enterprise.cloudsearch.sharepoint.SiteDataClient.CursorPaginator;
import com.google.enterprise.cloudsearch.sharepoint.SiteDataClient.Paginator;
import com.microsoft.schemas.sharepoint.soap.ItemData;
import com.microsoft.schemas.sharepoint.soap.ObjectType;
import com.microsoft.schemas.sharepoint.soap.SPContentDatabase;
import com.microsoft.schemas.sharepoint.soap.SPSite;
import com.microsoft.schemas.sharepoint.soap.SiteDataSoap;
import java.io.IOException;
import javax.xml.ws.Holder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for validating {@link SiteDataClient} */
public class SiteDataClientTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testConstructor() {
    new SiteDataClient(mock(SiteDataSoap.class), true);
  }

  @Test
  public void testConstructorNullSiteDataSoap() {
    thrown.expect(NullPointerException.class);
    new SiteDataClient(null, true);
  }

  @Test
  public void testGetSite() throws IOException {
    SiteDataSoap mockSiteDataSoap = mock(SiteDataSoap.class);
    SiteDataClient client = new SiteDataClient(mockSiteDataSoap, false);
    doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Holder<String> result = (Holder<String>) invocation.getArgument(7);
              result.value = "<Site></Site>";
              return null;
            })
        .when(mockSiteDataSoap)
        .getContent(
            eq(ObjectType.SITE_COLLECTION),
            eq(null),
            eq(null),
            eq(null),
            eq(true),
            eq(false),
            eq(null),
            any());
    assertNotNull(client.getContentSite());
  }

  @Test
  public void testGetWeb() throws IOException {
    SiteDataSoap mockSiteDataSoap = mock(SiteDataSoap.class);
    SiteDataClient client = new SiteDataClient(mockSiteDataSoap, false);
    doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Holder<String> result = (Holder<String>) invocation.getArgument(7);
              result.value = "<Web></Web>";
              return null;
            })
        .when(mockSiteDataSoap)
        .getContent(
            eq(ObjectType.SITE),
            eq(null),
            eq(null),
            eq(null),
            eq(true),
            eq(false),
            eq(null),
            any());
    assertNotNull(client.getContentWeb());
  }

  @Test
  public void testGetVirtualServer() throws IOException {
    SiteDataSoap mockSiteDataSoap = mock(SiteDataSoap.class);
    SiteDataClient client = new SiteDataClient(mockSiteDataSoap, false);
    doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Holder<String> result = (Holder<String>) invocation.getArgument(7);
              result.value = "<VirtualServer></VirtualServer>";
              return null;
            })
        .when(mockSiteDataSoap)
        .getContent(
            eq(ObjectType.VIRTUAL_SERVER),
            eq(null),
            eq(null),
            eq(null),
            eq(true),
            eq(false),
            eq(null),
            any());
    assertNotNull(client.getContentVirtualServer());
  }

  @Test
  public void testGetContentDb() throws IOException {
    SiteDataSoap mockSiteDataSoap = mock(SiteDataSoap.class);
    SiteDataClient client = new SiteDataClient(mockSiteDataSoap, false);
    boolean getChildItems = true;
    doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Holder<String> result = (Holder<String>) invocation.getArgument(7);
              result.value = "<ContentDatabase></ContentDatabase>";
              return null;
            })
        .when(mockSiteDataSoap)
        .getContent(
            eq(ObjectType.CONTENT_DATABASE),
            eq("cd1"),
            eq(null),
            eq(null),
            eq(getChildItems),
            eq(false),
            eq(null),
            any());
    assertNotNull(client.getContentContentDatabase("cd1", getChildItems));
  }

  @Test
  public void testGetContentList() throws IOException {
    SiteDataSoap mockSiteDataSoap = mock(SiteDataSoap.class);
    SiteDataClient client = new SiteDataClient(mockSiteDataSoap, false);
    doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Holder<String> result = (Holder<String>) invocation.getArgument(7);
              result.value = "<List></List>";
              return null;
            })
        .when(mockSiteDataSoap)
        .getContent(
            eq(ObjectType.LIST),
            eq("list1"),
            eq(null),
            eq(null),
            eq(false),
            eq(false),
            eq(null),
            any());
    assertNotNull(client.getContentList("list1"));
  }

  @Test
  public void testGetContentListItem() throws IOException {
    SiteDataSoap mockSiteDataSoap = mock(SiteDataSoap.class);
    SiteDataClient client = new SiteDataClient(mockSiteDataSoap, false);
    doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Holder<String> result = (Holder<String>) invocation.getArgument(7);
              result.value = "<Item></Item>";
              return null;
            })
        .when(mockSiteDataSoap)
        .getContent(
            eq(ObjectType.LIST_ITEM),
            eq("list1"),
            eq(""),
            eq("item1"),
            eq(false),
            eq(false),
            eq(null),
            any());
    assertNotNull(client.getContentItem("list1", "item1"));
  }

  @Test
  public void testGetContentListItemAttachments() throws IOException {
    SiteDataSoap mockSiteDataSoap = mock(SiteDataSoap.class);
    SiteDataClient client = new SiteDataClient(mockSiteDataSoap, false);
    doAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Holder<String> result = (Holder<String>) invocation.getArgument(7);
              result.value = "<Item></Item>";
              return null;
            })
        .when(mockSiteDataSoap)
        .getContent(
            eq(ObjectType.LIST_ITEM_ATTACHMENTS),
            eq("list1"),
            eq(""),
            eq("item1"),
            eq(true),
            eq(false),
            eq(null),
            any());
    assertNotNull(client.getContentListItemAttachments("list1", "item1"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetContentFolderChildren() throws IOException {
    SiteDataSoap mockSiteDataSoap = mock(SiteDataSoap.class);
    SiteDataClient client = new SiteDataClient(mockSiteDataSoap, false);
    doAnswer(
            invocation -> {
              Holder<String> lastIdOnPage = (Holder<String>) invocation.getArgument(6);
              lastIdOnPage.value = "nextPage";
              Holder<String> result = (Holder<String>) invocation.getArgument(7);
              result.value = "<Folder></Folder>";
              return null;
            })
        .when(mockSiteDataSoap)
        .getContent(
            eq(ObjectType.FOLDER),
            eq("list1"),
            eq("/folder1"),
            eq(null),
            eq(true),
            eq(false),
            any(),
            any());
    Paginator<ItemData> contentFolderChildren =
        client.getContentFolderChildren("list1", "/folder1");
    assertNotNull(contentFolderChildren.next());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetUrlSegments() throws IOException {
    SiteDataSoap mockSiteDataSoap = mock(SiteDataSoap.class);
    SiteDataClient client = new SiteDataClient(mockSiteDataSoap, false);
    String url = "https://sp.com/lists/list1/item1";
    Holder<String> listId = new Holder<>();
    Holder<String> itemId = new Holder<>();
    doAnswer(
            invocation -> {
              Holder<String> listIdHolder = (Holder<String>) invocation.getArgument(4);
              listIdHolder.value = "list1-id";
              Holder<String> itemIdHolder = (Holder<String>) invocation.getArgument(5);
              itemIdHolder.value = "item1-id";
              Holder<Boolean> result = (Holder<Boolean>) invocation.getArgument(1);
              result.value = true;
              return null;
            })
        .when(mockSiteDataSoap)
        .getURLSegments(eq(url), any(), eq(null), eq(null), eq(listId), eq(itemId));
    assertTrue(client.getUrlSegments(url, listId, itemId));
    assertEquals("list1-id", listId.value);
    assertEquals("item1-id", itemId.value);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetSiteAndWeb() throws IOException {
    SiteDataSoap mockSiteDataSoap = mock(SiteDataSoap.class);
    SiteDataClient client = new SiteDataClient(mockSiteDataSoap, false);
    String url = "https://sp.com/web/lists/list1/item1";
    Holder<String> siteUrl = new Holder<>();
    Holder<String> webUrl = new Holder<>();
    doAnswer(
            invocation -> {
              Holder<String> siteUrlHolder = (Holder<String>) invocation.getArgument(2);
              siteUrlHolder.value = "https://sp.com";
              Holder<String> webUrlHolder = (Holder<String>) invocation.getArgument(3);
              webUrlHolder.value = "https://sp.com/web";
              Holder<Long> result = (Holder<Long>) invocation.getArgument(1);
              result.value = 0L;
              return null;
            })
        .when(mockSiteDataSoap)
        .getSiteAndWeb(eq(url), any(), eq(siteUrl), eq(webUrl));
    assertEquals(0, client.getSiteAndWeb(url, siteUrl, webUrl));
    assertEquals("https://sp.com", siteUrl.value);
    assertEquals("https://sp.com/web", webUrl.value);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetChangesContentDb() throws IOException {
    SiteDataSoap mockSiteDataSoap = mock(SiteDataSoap.class);
    SiteDataClient client = new SiteDataClient(mockSiteDataSoap, false);
    doAnswer(
            invocation -> {
              Holder<String> lastChangeId = (Holder<String>) invocation.getArgument(2);
              assertEquals("token1", lastChangeId.value);
              // Update last change Id included in this set of changes
              lastChangeId.value = "token2";
              Holder<String> result = (Holder<String>) invocation.getArgument(5);
              result.value = "<SPContentDatabase></SPContentDatabase>";
              Holder<Boolean> hasMore = (Holder<Boolean>) invocation.getArgument(6);
              hasMore.value = true;
              return null;
            })
        .when(mockSiteDataSoap)
        .getChanges(eq(ObjectType.CONTENT_DATABASE), eq("cd1"), any(), any(), any(), any(), any());
    CursorPaginator<SPContentDatabase, String> changes =
        client.getChangesContentDatabase("cd1", "token1");
    assertNotNull(changes.next());
    assertEquals("token2", changes.getCursor());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetChangesSPSite() throws IOException {
    SiteDataSoap mockSiteDataSoap = mock(SiteDataSoap.class);
    SiteDataClient client = new SiteDataClient(mockSiteDataSoap, false);
    doAnswer(
            invocation -> {
              Holder<String> lastChangeId = (Holder<String>) invocation.getArgument(2);
              assertEquals("token1", lastChangeId.value);
              // Update last change Id included in this set of changes
              lastChangeId.value = "token2";
              Holder<String> result = (Holder<String>) invocation.getArgument(5);
              result.value = "<SPSite></SPSite>";
              Holder<Boolean> hasMore = (Holder<Boolean>) invocation.getArgument(6);
              hasMore.value = true;
              return null;
            })
        .when(mockSiteDataSoap)
        .getChanges(eq(ObjectType.SITE_COLLECTION), eq("site1"), any(), any(), any(), any(), any());
    CursorPaginator<SPSite, String> changes = client.getChangesSPSite("site1", "token1");
    assertNotNull(changes.next());
    assertEquals("token2", changes.getCursor());
  }
}
