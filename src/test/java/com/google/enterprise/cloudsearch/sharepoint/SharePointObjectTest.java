package com.google.enterprise.cloudsearch.sharepoint;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SharePointObjectTest {
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testBuilder() throws IOException {
    SharePointObject vs = new SharePointObject.Builder(SharePointObject.VIRTUAL_SERVER).build();
    assertTrue(vs.isValid());
    validateParseAndEquals(vs);

    SharePointObject siteCollection =
        new SharePointObject.Builder(SharePointObject.SITE_COLLECTION)
            .setUrl("http://sp.com")
            .setSiteId("siteId")
            .setWebId("webId")
            .setObjectId("object")
            .build();
    assertTrue(siteCollection.isValid());
    validateParseAndEquals(siteCollection);

    SharePointObject web =
        new SharePointObject.Builder(SharePointObject.WEB)
            .setUrl("http://sp.com/web")
            .setSiteId("siteId")
            .setWebId("webId")
            .setObjectId("object")
            .build();
    assertTrue(web.isValid());
    validateParseAndEquals(web);

    SharePointObject list =
        new SharePointObject.Builder(SharePointObject.LIST)
            .setUrl("http://sp.com/web/list")
            .setSiteId("siteId")
            .setWebId("webId")
            .setObjectId("object")
            .setListId("listId")
            .build();
    assertTrue(list.isValid());
    validateParseAndEquals(list);

    SharePointObject item =
        new SharePointObject.Builder(SharePointObject.LIST_ITEM)
            .setUrl("http://sp.com/web/list")
            .setSiteId("siteId")
            .setWebId("webId")
            .setObjectId("object")
            .setListId("listId")
            .build();
    assertTrue(item.isValid());
    validateParseAndEquals(item);

    SharePointObject attachment =
        new SharePointObject.Builder(SharePointObject.ATTACHMENT)
            .setUrl("http://sp.com/web/list")
            .setSiteId("siteId")
            .setWebId("webId")
            .setObjectId("object")
            .setListId("listId")
            .setItemId("1")
            .build();
    assertTrue(attachment.isValid());
    validateParseAndEquals(attachment);

    thrown.expect(IllegalArgumentException.class);
    new SharePointObject.Builder("Invalid").build();

  }

  @Test
  public void testEmptyPayload() {
    SharePointObject empty = new SharePointObject();
    assertFalse(empty.isValid());
  }

  @Test
  public void testMissingValues() {
    SharePointObject siteCollection =
        new SharePointObject.Builder(SharePointObject.SITE_COLLECTION).build();
    assertFalse(siteCollection.isValid());
  }

  @Test
  public void testParsing() throws IOException {
    GenericJson toParse = new GenericJson();
    toParse.setFactory(JSON_FACTORY);
    toParse.put("objectType", "ATTACHMENT");
    toParse.put("siteId", "site1");
    toParse.put("webId", "web1");
    toParse.put("listId", "list1");
    toParse.put("objectId", "obj1");
    toParse.put("itemId", "item1");
    toParse.put("url", "http://sp.com");
    byte[] encoded = toParse.toPrettyString().getBytes();
    SharePointObject parsed = SharePointObject.parse(encoded);
    assertTrue(parsed.isValid());
    assertEquals("ATTACHMENT", parsed.getObjectType());
    assertEquals("site1", parsed.getSiteId());
    assertEquals("web1", parsed.getWebId());
    assertEquals("list1", parsed.getListId());
    assertEquals("obj1", parsed.getObjectId());
    assertEquals("item1", parsed.getItemId());
    assertEquals("http://sp.com", parsed.getUrl());
  }

  private void validateParseAndEquals(SharePointObject object) throws IOException {
    byte[] encoded = object.encodePayload();
    SharePointObject parsed = SharePointObject.parse(encoded);
    assertEquals(object, parsed);
  }
}
