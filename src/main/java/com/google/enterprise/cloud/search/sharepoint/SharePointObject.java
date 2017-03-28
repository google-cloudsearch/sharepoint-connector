package com.google.enterprise.cloud.search.sharepoint;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Key;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
/** Payload object for saving per document / item state. */
public class SharePointObject extends GenericJson {
  private static final Logger log = Logger.getLogger(SharePointObject.class.getName());
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  public static final String VIRTUAL_SERVER = "VIRTUAL_SERVER";
  public static final String SITE_COLLECTION = "SITE_COLLECTION";
  public static final String WEB = "WEB";
  public static final String LIST = "LIST";
  public static final String LIST_ITEM = "LIST_ITEM";
  public static final String ATTACHMENT = "ATTACHMENT";
  public static final String ASPX = "ASPX";
  public static final String NAMED_RESOURCE = "NAMED_RESOURCE";

  private static final Set<String> SUPPORTED_OBJECT_TYPE =
      ImmutableSet.of(
          VIRTUAL_SERVER,
          SITE_COLLECTION,
          WEB,
          LIST,
          LIST_ITEM,
          ATTACHMENT,
          ASPX,
          NAMED_RESOURCE);

  @Key private String objectType;
  @Key private String url;
  @Key private String objectId;
  @Key private String siteId;
  @Key private String webId;
  @Key private String listId;
  @Key private String itemId;

  /**
   * Default constructor for Json parsing
   */
  public SharePointObject() {
    super();
    setFactory(JSON_FACTORY);
  }

  public SharePointObject(Builder builder) {
    this.objectType = builder.objectType;
    this.url = builder.url;
    this.objectId = builder.objectId;
    this.siteId = builder.siteId;
    this.webId = builder.webId;
    this.listId = builder.listId;
    this.itemId = builder.itemId;
    setFactory(JSON_FACTORY);
  }

  private static SharePointObject parse(String payloadString) throws IOException {
    log.log(Level.FINE , "Parsing {0}", payloadString);
    return JSON_FACTORY.fromString(payloadString, SharePointObject.class);
  }

  static SharePointObject parse(byte[] payload) throws IOException {
    if (payload == null) {
      return new SharePointObject();
    }
    return parse(new String(payload, UTF_8));
  }

  byte[] encodePayload() throws IOException {
    return this.toPrettyString().getBytes(UTF_8);
  }

  boolean isValid() {
    if (!SUPPORTED_OBJECT_TYPE.contains(objectType)) {
      return false;
    }

    if (VIRTUAL_SERVER.equals(objectType) || NAMED_RESOURCE.equals(objectType)) {
      return true;
    }
    ArrayList<String> required = new ArrayList<String>();
    required.add(url);
    required.add(objectId);
    required.add(siteId);
    required.add(webId);
    if (ATTACHMENT.equals(objectType) || LIST_ITEM.equals(objectType) || LIST.equals(objectType)) {
      required.add(listId);
    }
    if (ATTACHMENT.equals(objectType)) {
      required.add(itemId);
    }
    for (String value : required) {
      if (Strings.isNullOrEmpty(value)) {
        return false;
      }
    }
    return true;
  }

  String getObjectType() {
    return objectType;
  }

  String getUrl() {
    return url;
  }

  String getObjectId() {
    return objectId;
  }

  String getSiteId() {
    return siteId;
  }

  String getWebId() {
    return webId;
  }

  String getListId() {
    return listId;
  }

  String getItemId() {
    return itemId;
  }

  public static class Builder {
    private String objectType;
    private String url;
    private String objectId;
    private String siteId;
    private String webId;
    private String listId;
    private String itemId;

    public Builder(String objectType) {
      this.objectType = objectType;
    }

    public Builder setObjectId(String objectId) {
      this.objectId = objectId;
      return this;
    }

    public Builder setUrl(String url) {
      this.url = url;
      return this;
    }

    public Builder setSiteId(String siteId) {
      this.siteId = siteId;
      return this;
    }

    public Builder setWebId(String webId) {
      this.webId = webId;
      return this;
    }

    public Builder setListId(String listId) {
      this.listId = listId;
      return this;
    }

    public Builder setItemId(String itemId) {
      this.itemId = itemId;
      return this;
    }

    public SharePointObject build() {
      checkArgument(SUPPORTED_OBJECT_TYPE.contains(objectType));
      return new SharePointObject(this);
    }
  }
}
