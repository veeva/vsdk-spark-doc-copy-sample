package com.veeva.vault.custom.processors;

import com.veeva.vault.custom.udc.vSDKSparkHelper;
import com.veeva.vault.custom.udc.vSDKDocCopy;
import com.veeva.vault.sdk.api.core.*;
import com.veeva.vault.sdk.api.queue.*;

import java.util.List;
import java.lang.*;

/**
 * 
 * This is a Vault Java SDK MessageProcessor that demonstrates using Spark to extract documents to be copied
 * from a source vault to a target vault, for any messages on Spark vault to vault messaging queue.
 * 
 * On the target vault, the MessageProcessor processes `Message` records received in the inbound queue
 * `vsdk_doc_copy_in_queue__c`.
 *    - The vSDKSparkHelper UDC is used to retrieve documents to be copied from the Integration Transaction
 *      record in the source Vault, with a transaction status of "Pending" and source object of "documents"
 *    - The documentService is used to createDocuments, newDocumentRendition, createAttachments and
 *      migrateDocumentVersions.
 *
 */

@MessageProcessorInfo()
public class vSDKDocCopyMessageProcessor implements MessageProcessor {

    /**
     * Main Spark v2v MessaageProcessor. It determines the Integration Point
     * related to the message and call the appropriate Processing code
     * to copy documents between two Vaults
     */

	public void execute(MessageContext context) {

        //Processes a message from the queue which aren't blank
    	Message incomingMessage = context.getMessage();

        // Here we want to process messages specific to the different integration
        // points. These message attributes are set in the document action
        // "vSdkDocCopyDocumentAction" in the source Vault
        String integrationPointApiName =
                incomingMessage.getAttribute("integration_point",
                                             MessageAttributeValueType.STRING);

        switch(integrationPointApiName) {
            case "receive_document_copy__c":
                // Here we want to process the messages associated with the inbound
                // Integration Point "Receive Copy Document". This is done in the method
                // "processCopyDocuments"
                processCopyDocuments(incomingMessage, context);
                break;
                // Add further case statements here if you wish to handle further
                // integration points
        }
    }

    /**
     * Processes "Doc Copy" messages from the queue.
     *
     * This processing searches for an existing documents in the
     * target Vault based on the incoming message which has the source record ID.
     * Once a record is found, an API VQL query is made to the source vault to grab
     * additional data from the source record. This queried information is added
     * to the target record.
     *
     * @param message Spark message .
     * @param context is the MessageProcessor context
     */
	public static void processCopyDocuments(Message message,
                                                    MessageContext context) {

        LogService logService = ServiceLocator.locate(LogService.class);
        List<String> copyDocsList = VaultCollections.newList();
        String sourceVaultId = context.getRemoteVaultId();
        String connectionId = context.getConnectionId();
        String integrationPointApiName =
                message.getAttribute("integration_point", MessageAttributeValueType.STRING);

        StringBuilder logMessage = new StringBuilder();
        logMessage.append("Processing integrationPointApiName: ").append(integrationPointApiName);
        logService.info(logMessage.toString());

        String connectionName = "vsdk_doc_copy_connection";

        // Check for documents which haven't been copied
        // This uses a callback to the source vault for the document
        // then add them to the incoming message list for processing
        List<String> unprocessedDocIdsList = vSDKSparkHelper.getUnprocessedObjects(
                connectionName,
                "documents",
                integrationPointApiName,
                true);
        copyDocsList.addAll(unprocessedDocIdsList);

        // Copy the documents from the Source Vault
        vSDKDocCopy.process(connectionName,
                integrationPointApiName,
                unprocessedDocIdsList,
                true,
                true,
                true);
	}
}