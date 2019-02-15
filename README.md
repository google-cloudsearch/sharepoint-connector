# Google Cloud Search SharePoint Connector

The Google Cloud Search SharePoint Connector enables indexing of content stored in a SharePoint
environment (online / on-prem) including support for ACLs and metadata. This connector implements
the [graph traversal strategy](https://developers.google.com/cloud-search/docs/guides/content-connector#graph-traversal) provided by the
[Content Connector SDK](https://developers.google.com/cloud-search/docs/guides/content-connector).

## Build instructions

1. Clone the connector repository from GitHub:
   ```
   git clone https://github.com/google-cloudsearch/sharepoint-connector.git
   cd sharepoint-connector
   ```

2. Checkout the desired version of the connector and build the ZIP file:
   ```
   git checkout tags/v1-0.0.4
   mvn package
   ```
   (To skip the tests when building the connector, use `mvn package -DskipTests`)

For further information on configuration and deployment of this connector, see
[Deploy the Microsoft SharePoint On-Prem Connector](https://developers.google.com/cloud-search/docs/guides/sharepoint-on-prem-connector)
or [Deploy the Microsoft SharePoint Online Connector](https://developers.google.com/cloud-search/docs/guides/sharepoint-online-connector).
