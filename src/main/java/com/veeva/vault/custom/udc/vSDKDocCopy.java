package com.veeva.vault.custom.udc;

import com.veeva.vault.sdk.api.connection.ConnectionContext;
import com.veeva.vault.sdk.api.connection.ConnectionService;
import com.veeva.vault.sdk.api.connection.ConnectionUser;
import com.veeva.vault.sdk.api.core.*;
import com.veeva.vault.sdk.api.document.*;
import com.veeva.vault.sdk.api.http.*;
import com.veeva.vault.sdk.api.json.JsonArray;
import com.veeva.vault.sdk.api.json.JsonData;
import com.veeva.vault.sdk.api.json.JsonObject;
import com.veeva.vault.sdk.api.json.JsonValueType;
import com.veeva.vault.sdk.api.query.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/******************************************************************************
 * User-Defined Class:  vSDKDocCopy
 * Author:              John Tanner @ Veeva
 * Date:                2020-03-13
 *-----------------------------------------------------------------------------
 * Description: Provides a sample UDC with using the document copy to copy
 *              documents within Vault including files, versions, renditions
 *              and file attachments using new functionality developer in
 *              release 20R1. This example assumes documents are being copied
 *              between Vaults.
 *
 *-----------------------------------------------------------------------------
 * Copyright (c) 2023 Veeva Systems Inc.  All Rights Reserved.
 *      This code is based on pre-existing content developed and
 *      owned by Veeva Systems Inc. and may only be used in connection
 *      with the deliverable with which it was provided to Customer.
 *--------------------------------------------------------------------
 *
 *******************************************************************************/

@UserDefinedClassInfo()
public class vSDKDocCopy {

    /**
     *  Copies the documents by global Id from the source vault
     *  This uses a callback to the source vault to retrieve metadata for the source document
     *
     * @param connectionName to the source Vault
     * @param integrationPointApiName for the connection
     * @param docGlobalIds of the documents to be copied from the source Vault
     * @param includeVersions of false to only copy the latest version or true for all versions
     * @param includeViewableRendition of true will copy the viewable renditions from the source
     * @param includeAttachments of true will copy the document attachments from the source
     */
    public static void process(String connectionName,
                               String integrationPointApiName,
                               List<String> docGlobalIds,
                               Boolean includeVersions,
                               Boolean includeViewableRendition,
                               Boolean includeAttachments) {

        ConnectionService connectionService = ServiceLocator.locate(ConnectionService.class);
        ConnectionContext connectionContext = connectionService.newConnectionContext(connectionName,
                ConnectionUser.CONNECTION_AUTHORIZED_USER); // Use Connection API name and user
        DocumentService documentService = ServiceLocator.locate(DocumentService.class);
        LogService logService = ServiceLocator.locate(LogService.class);

        List<String> successfulIdList = VaultCollections.newList();
        List<String> failedIdList = VaultCollections.newList();
        String queryObject = "documents";

        StringBuilder query = new StringBuilder();
        query.append("SELECT id, global_id__sys, global_version_id__sys, latest_version__v,");
        query.append(" version_id, lifecycle__v, status__v, type__v, name__v ");
        if (includeVersions) {
            query.append("FROM ALLVERSIONS ").append(queryObject).append(" ");
        } else {
            query.append("FROM ").append(queryObject).append(" ");
        }
        query.append("WHERE global_id__sys CONTAINS ('" + String.join("','", docGlobalIds) + "')");
        query.append("ORDER BY global_version_id__sys ASC");

        StringBuilder logMessage = new StringBuilder();
        logMessage.append("Unprocessed documents query: ").append(query.toString());
        logService.info(logMessage.toString());

        //This is a vault to vault Http Request to the source connection
        //The configured connection provides the full DNS name.
        //For the path, you only need to append the API endpoint after the DNS.
        //The query endpoint takes a POST where the BODY is the query itself.
        HttpService httpService = ServiceLocator.locate(HttpService.class);
        FormHttpRequest request = httpService.newHttpRequestBuilder()
                .withConnectionName(connectionName)
                .withMethod(HttpMethod.POST)
                .withPath("/api/v24.2/query")
                .withHeader("Content-Type", "application/x-www-form-urlencoded")
                .withBodyParam("q", query.toString())
                .build();

        //Send the request the source vault via a callback. The response received back should be a JSON response.
        //First, the response is parsed into a `JsonData` object
        //From the response, the `getJsonObject()` will get the response as a parseable `JsonObject`
        //    * Here the `getValue` method can be used to retrieve `responseStatus`, `responseDetails`, and `data`
        //The `data` element is an array of JSON data. This is parsed into a `JsonArray` object.
        //    * Each queried record is returned as an element of the array and must be parsed into a `JsonObject`.
        //    * Individual fields can then be retrieved from each `JsonObject` that is in the `JsonArray`.
        httpService.sendRequest(request, HttpResponseBodyValueType.JSONDATA)
            .onSuccess(httpResponse -> {
                JsonData response = httpResponse.getResponseBody();

                String responseStatus = response.getJsonObject().getValue("responseStatus", JsonValueType.STRING);

                if (responseStatus.equals("SUCCESS")) {

                    JsonArray data = response.getJsonObject().getValue("data", JsonValueType.ARRAY);
                    logService.info("HTTP Query Request: SUCCESS");

                    String latestGlobalId = "";
                    List<DocumentVersion> newVersions = VaultCollections.newList();
                    String latestNewDocId = "";

                    //Retrieve each record returned from the VQL query, which corresponds to a document.
                    //Each element of the returned `data` JsonArray is a record with it's queried fields.
                    for (int i = 0; i < data.getSize(); i++) {
                        JsonObject queryRecord = data.getValue(i, JsonValueType.OBJECT);

                        String globalId = queryRecord.getValue("global_id__sys", JsonValueType.STRING);
                        String globalVersionId = queryRecord.getValue("global_version_id__sys", JsonValueType.STRING);
                        Boolean latestVersion = queryRecord.getValue("latest_version__v", JsonValueType.BOOLEAN);
                        BigDecimal id = queryRecord.getValue("id", JsonValueType.NUMBER);

                        // Retrieve the desired document metadata fields from the VQL query data
                        String versionId = queryRecord.getValue("version_id", JsonValueType.STRING);
                        String docName = queryRecord.getValue("name__v", JsonValueType.STRING);
                        String type = queryRecord.getValue("type__v", JsonValueType.STRING);
                        String lifecycle = queryRecord.getValue("lifecycle__v", JsonValueType.STRING);
                        String status = queryRecord.getValue("status__v", JsonValueType.STRING);
                        // Workout the major and minor versions to match the source
                        String[] parts = StringUtils.split(versionId, "_");
                        BigDecimal majorVersion = BigDecimal.valueOf(Integer.valueOf(parts[1]));
                        BigDecimal minorVersion = BigDecimal.valueOf(Integer.valueOf(parts[2]));

                        // Create a new document if different from the previous version
                        if (!globalId.equals(latestGlobalId)) {

                            String newDocId = getDocumentIdForVersion(globalVersionId);

                            // Create the doc if it doesn't already exist in the target vault
                            if (!docVersionExists(globalVersionId)) {

                                // Create a new document in the target, setting the metadata to match the
                                // values from the source
                                DocumentVersion documentVersion = documentService.newDocument();
                                documentVersion.setValue("name__v", docName);
                                documentVersion.setValue("type__v", VaultCollections.asList(type));
                                documentVersion.setValue("lifecycle__v", VaultCollections.asList(lifecycle));
                                documentVersion.setValue("status__v", VaultCollections.asList(status));
                                documentVersion.setValue("link__sys", globalId);
                                documentVersion.setValue("version_link__sys", globalVersionId);
                                documentVersion.setValue("major_version_number__v", majorVersion);
                                documentVersion.setValue("minor_version_number__v", minorVersion);

                                // Copy the source file reference
                                DocumentSourceFileReference documentSourceFileReference =
                                        documentService.newDocumentSourceFileReference(connectionContext, versionId);
                                documentVersion.setSourceFile(documentSourceFileReference);

                                // Suppress the rendition so it can be copied in from the source vault,
                                // rather than being regenerated
                                if (includeViewableRendition) {
                                    documentVersion.suppressRendition();
                                }

                                // Save the newly created document copy in the target vault
                                SaveDocumentVersionsResponse documents =
                                        documentService.createDocuments(VaultCollections.asList(documentVersion));
                                List<PositionalDocumentVersionId> docversion = documents.getSuccesses();
                                newDocId = docversion.get(0).getDocumentVersionId();
                                docversion.stream().forEach(newdoc ->
                                        logService.info("Document  Created is : " + newdoc.getDocumentVersionId()));

                                // Create viewable rendition (if required)
                                //
                                // Note: similar logic can be used for other rendition types if so desired
                                if (includeViewableRendition) {
                                    DocumentRenditionFileReference documentRenditionFileReference =
                                            documentService.newDocumentRenditionFileReference(connectionContext,
                                                    versionId,
                                                    "viewable_rendition__v");
                                    DocumentRendition viewableRendition =
                                            documentService.newDocumentRendition(documentRenditionFileReference,
                                                    newDocId,
                                                    "viewable_rendition__v");

                                    // Add the rendition
                                    SaveDocumentVersionsResponse docRendition =
                                            documentService.createRenditions(VaultCollections.asList(viewableRendition));

                                    //Logging
                                    List<PositionalDocumentVersionId> docRenditionsList = docRendition.getSuccesses();
                                    docRenditionsList.stream().forEach(renditionId ->
                                            logService.info("Document Rendition created is : "
                                                    + renditionId.getDocumentVersionId()));
                                }
                            }

                            //Get new doc id, for subsequent versions
                            String[] parts2 = StringUtils.split(newDocId, "_");
                            latestNewDocId = parts2[0];

                            // Create document attachments if required
                            if (includeAttachments) {
                                List<String> docAttachmentIds = getAttachmentIds(connectionName,id.toString());
                                List<DocumentAttachment> documentAttachments = VaultCollections.newList();

                                for (String docAttachmentId : docAttachmentIds) {
                                    DocumentAttachmentFileReference documentAttachmentFileReference =
                                            documentService.newDocumentAttachmentFileReference(connectionContext,
                                                    docAttachmentId);
                                    DocumentAttachment documentAttachment =
                                            documentService.newDocumentAttachment(documentAttachmentFileReference,
                                                    latestNewDocId);
                                    documentAttachments.add(documentAttachment);
                                    documentService.createAttachments(VaultCollections.asList(documentAttachment));

                                }
                                // Document Attachments are only created where they don't already exist
                                documentService.createAttachments(documentAttachments);
                            }


                            // Log the successfully updated records
                            successfulIdList.add(globalId);

                            // Otherwise create a new version
                        } else {

                            // Create the doc if it doesn't already exist in the target vault
                            if (!docVersionExists(globalVersionId)) {

                                logService.info("Target document id =" );
                                logService.info("Document version to be created against docId " + latestNewDocId
                                        + " with version_link__sys=" + globalVersionId);

                                // Create a new document version in the target, setting the metadata to match the
                                // values from the source
                                DocumentSourceFileReference versionSourceFileReference =
                                        documentService.newDocumentSourceFileReference(connectionContext, versionId);
                                // Pass Doc Id to create the version from
                                DocumentVersion newVersion = documentService.newVersion(latestNewDocId);
                                newVersion.setValue("name__v", docName);
                                newVersion.setValue("type__v", VaultCollections.asList(type));
                                newVersion.setValue("lifecycle__v", VaultCollections.asList(lifecycle));
                                newVersion.setValue("status__v", VaultCollections.asList(status));
                                newVersion.setValue("link__sys", globalId);
                                newVersion.setValue("version_link__sys", globalVersionId);
                                newVersion.setValue("major_version_number__v", majorVersion);
                                newVersion.setValue("minor_version_number__v", minorVersion);

                                // Copy the version's source file reference
                                newVersion.setSourceFile(versionSourceFileReference);

                                // Add the version to a list to be created once all versions form the document
                                // have been processed
                                newVersions.add(newVersion);
                            }

                            // If this is the final version for the document, migrate all the versions
                            if (latestVersion) {
                                documentService.migrateDocumentVersions(newVersions);
                                latestNewDocId = "";
                                newVersions.clear();
                            }
                        }
                        latestGlobalId = globalId;
                    }

                    // If the response is unsuccessful the transfer of the integration records
                    // should be marked as having failed
                } else {
                    for (String docGlobalId : docGlobalIds) {
                        failedIdList.add(docGlobalId);
                    }
                }
                response = null;
            })
            .onError(httpOperationError -> {
                logService.info("copyDocuments error: httpOperationError.");
                for (String docGlobalId : docGlobalIds) {
                    failedIdList.add(docGlobalId);
                }
            }).execute();

        // Update the source system to say whether the documents have been copied or not,
        // by making a callback to the source vault and updating the integration transaction
        // record associated with the source record.
        vSDKSparkHelper.setIntTransProcessedStatuses(
                successfulIdList,
                connectionName,
                queryObject,
                integrationPointApiName,
                true);

        vSDKSparkHelper.setIntTransProcessedStatuses(
                failedIdList,
                connectionName,
                queryObject,
                integrationPointApiName,
                false);

        request = null;

    }

    /**
     * Returns true if a document version already exists for the given version link value
     *
     * @param versionLinkId value to be checked for
     */
    private static Boolean docVersionExists(String versionLinkId) {

        LogService logService = ServiceLocator.locate(LogService.class);
        QueryService queryService = ServiceLocator.locate(QueryService.class);
        List<Integer> recordCount = VaultCollections.newList();

        // Query to see any incoming IDs match to any existing documents.
        Query query = queryService
                .newQueryBuilder()
                .withSelect(VaultCollections.asList("id"))
                .withFrom("ALLVERSIONS documents")
                .withWhere("version_link__sys = '" + versionLinkId + "'")
                .build();

        QueryCountRequest queryCountRequest  = queryService
                .newQueryCountRequestBuilder()
                .withQuery(query)
                .build();

        queryService.count(queryCountRequest)
                .onSuccess(queryResponse -> {
                    recordCount.add((int)queryResponse.getTotalCount());
                })
                .onError(queryOperationError -> {
                    logService.error("Failed to query records: " + queryOperationError.getMessage());
                })
                .execute();

        if (recordCount.get(0) == 0) {
            return false;
        } else {
            logService.info("Found existing document version with version_link__sys: " + versionLinkId);
            return true;
        }
    }

    /**
     * Returns the document Id for the target version if it exists, otherwise null is returned
     *
     * @param versionLinkId value to be checked for
     */
    private static String getDocumentIdForVersion(String versionLinkId) {

        LogService logService = ServiceLocator.locate(LogService.class);
        QueryService queryService = ServiceLocator.locate(QueryService.class);
        List<String> ids = VaultCollections.newList();

        // Query to see any incoming IDs match to any existing documents.
        Query query = queryService
                .newQueryBuilder()
                .withSelect(VaultCollections.asList("id"))
                .withFrom("ALLVERSIONS documents")
                .withWhere("version_link__sys = '" + versionLinkId + "'")
                .build();

        QueryExecutionRequest queryExecutionRequest = queryService.newQueryExecutionRequestBuilder()
                .withQuery(query)
                .build();

        QueryOperation<QueryExecutionResponse> queryOperation = queryService.query(queryExecutionRequest);

        queryOperation.onSuccess(queryExecutionResponse -> {
                    queryExecutionResponse.streamResults().forEach(queryExecutionResult -> {
                        ids.add(queryExecutionResult.getValue("id", ValueType.STRING));
                    });
                });
            queryOperation.onError(queryOperationError -> {
                logService.error("Failed to query records: " + queryOperationError.getMessage());
            });
            queryOperation.execute();

        if (!ids.isEmpty()) {
            StringBuilder debugMsg = new StringBuilder();
            debugMsg.append("Found existing document: id=").append(ids.get(0))
                    .append(", version_link__sys=").append(versionLinkId);
            logService.info(debugMsg.toString());
            return ids.get(0);
        } else {
            return "";
        }

    }

    /**
     * Returns the documentAttachment's Id from the source Vault version if it exists, otherwise null is returned.
     * This version can be used for copying documents within the same vault.
     *
     * @param sourceDocId value to be checked for attachments against
     */
    private static List<String> getAttachmentIdLocal(String sourceDocId){
        LogService logService = ServiceLocator.locate(LogService.class);
        QueryService queryService = ServiceLocator.locate(QueryService.class);
        List<String> idList = VaultCollections.newList();

        // Query to see any incoming doc IDs have attachments against them.
        Query query = queryService
                .newQueryBuilder()
                .withSelect(VaultCollections.asList("target_doc_id__v"))
                .withFrom("relationships")
                .withWhere("source_doc_id__v = '" + sourceDocId + "'")
                .build();

        QueryExecutionRequest queryExecutionRequest = queryService.newQueryExecutionRequestBuilder()
                .withQuery(query)
                .build();

        QueryOperation<QueryExecutionResponse> queryOperation = queryService.query(queryExecutionRequest);

        queryOperation.onSuccess(queryExecutionResponse -> {
            queryExecutionResponse.streamResults().forEach(queryExecutionResult -> {
                idList.add(queryExecutionResult.getValue("id", ValueType.STRING));
            });
        });
        queryOperation.onError(queryOperationError -> {
            logService.error("Failed to query records: " + queryOperationError.getMessage());
        });
        queryOperation.execute();

        StringBuilder logMessage = new StringBuilder();
        logMessage.append("Attachments query: ").append(query.toString());
        logService.info(logMessage.toString());

        return idList;
    }

    /**
     *  Returns the documentAttachment's Id from the source Vault version if it exists, otherwise null is returned
     *
     * @param connectionName to the source Vault
     * @param sourceDocId value to be checked for attachments against
     */
    private static List<String> getAttachmentIds(String connectionName,
                                                 String sourceDocId) {

        LogService logService = ServiceLocator.locate(LogService.class);
        List<String> idList = VaultCollections.newList();
        StringBuilder query = new StringBuilder();
        query.append("SELECT target_doc_id__v ");
        query.append("FROM relationships ");
        query.append("WHERE source_doc_id__v = '").append(sourceDocId).append("' ");


        StringBuilder logMessage = new StringBuilder();
        logMessage.append("Attachments Callback query: ").append(query.toString());
        logService.info(logMessage.toString());

        //This is a vault to vault Http Request to the source connection
        //The configured connection provides the full DNS name.
        //For the path, you only need to append the API endpoint after the DNS.
        //The query endpoint takes a POST where the BODY is the query itself.
        HttpService httpService = ServiceLocator.locate(HttpService.class);
        FormHttpRequest request = httpService.newHttpRequestBuilder()
                .withConnectionName(connectionName)
                .withMethod(HttpMethod.POST)
                .withPath("/api/v24.2/query")
                .withHeader("Content-Type", "application/x-www-form-urlencoded")
                .withBodyParam("q", query.toString())
                .build();

        httpService.sendRequest(request, HttpResponseBodyValueType.JSONDATA)
                .onSuccess(httpResponse -> {

                    JsonData response = httpResponse.getResponseBody();

                    if (response.isValidJson()) {
                        String responseStatus = response.getJsonObject().getValue("responseStatus", JsonValueType.STRING);

                        if (responseStatus.equals("SUCCESS")) {

                            JsonArray data = response.getJsonObject().getValue("data", JsonValueType.ARRAY);
                            logService.info("HTTP Query Request: SUCCESS");

                            //Retrieve each record returned from the VQL query.
                            for (int i = 0; i < data.getSize();i++) {
                                JsonObject queryRecord = data.getValue(i, JsonValueType.OBJECT);
                                idList.add(queryRecord.getValue("target_doc_id__v", JsonValueType.NUMBER).toString());
                            }
                        }
                    }
                    else {
                        logService.info("getAttachmentId error: Received a non-JSON response.");
                    }
                })
                .onError(httpOperationError -> {
                    logService.info("getAttachmentId error: httpOperationError.");
                }).execute();

        return idList;
    }
}
