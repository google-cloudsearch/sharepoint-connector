# Google Cloud Search SharePoint Connector

The Google Cloud Search SharePoint Connector enables indexing of content stored in a SharePoint
environment (online / on-prem) including support for ACLs and metadata. This connector implements
the [graph traversal strategy](https://developers.google.com/cloud-search/docs/guides/content-connector#graph-traversal) provided by the
[Content Connector SDK](https://developers.google.com/cloud-search/docs/guides/content-connector).

Before running the SharePoint content connector, you must map the principals used in
SharePoint to identities in the Google Cloud Identity service. See the documentation for the [SharePoint On-Prem Connector](https://developers.google.com/cloud-search/docs/guides/sharepoint-on-prem-connector)
or the [SharePoint Online Connector](https://developers.google.com/cloud-search/docs/guides/sharepoint-online-connector) for more information.


## Build instructions

1. Build the connector

   a. Clone the connector repository from GitHub:
      ```
      git clone https://github.com/google-cloudsearch/sharepoint-connector.git
      cd sharepoint-connector
      ```

   b. Checkout the desired version of the connector and build the ZIP file:
      ```
      git checkout tags/v1-0.0.5
      mvn package
      ```
      (To skip the tests when building the connector, use `mvn package -DskipTests`)


2. Install the connector

   The `mvn package` command creates a ZIP file containing the
   connector and its dependencies with a name like
   `google-cloudsearch-sharepoint-connector-v1-0.0.5.zip`.

   a. Copy this ZIP file to the location where you want to install the connector.

   b. Unzip the connector ZIP file. A directory with a name like
      `google-cloudsearch-sharepoint-connector-v1-0.0.5` will be created.

   c. Change into this directory. You should see the connector jar file,
      `google-cloudsearch-sharepoint-connector-v1-0.0.5.jar`, as well as a `lib`
      directory containing the connector's dependencies.


3. Configure the connector

   a. Create a file containing the connector configuration parameters. Refer to the
      [SharePoint On-Prem configuration documentation](https://developers.google.com/cloud-search/docs/guides/sharepoint-on-prem-connector#2_specify_the_sharepoint_on-prem_connector_configuration) or the
      [SharePoint Online configuration documentation](https://developers.google.com/cloud-search/docs/guides/sharepoint-online-connector#configure-sp-connector)
      for specifics and for parameter details.


4. Run the connector

   The connector should be run from the unzipped installation directory, **not** the source
   code's `target` directory.

   ```
   java \
      -jar google-cloudsearch-sharepoint-connector-v1-0.0.5.jar \
      -Dconfig=my.config
   ```

   Where `my.config` is the configuration file containing the parameters for the
   connector execution.

   **Note:** If the configuration file is not specified, a default file name of
   `connector-config.properties` will be assumed.



For further information on configuration and deployment of this connector, see
[Deploy the Microsoft SharePoint On-Prem Connector](https://developers.google.com/cloud-search/docs/guides/sharepoint-on-prem-connector)
or [Deploy the Microsoft SharePoint Online Connector](https://developers.google.com/cloud-search/docs/guides/sharepoint-online-connector).
