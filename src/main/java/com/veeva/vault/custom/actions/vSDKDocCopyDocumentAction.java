package com.veeva.vault.custom.actions;

import com.veeva.vault.sdk.api.core.*;
import com.veeva.vault.sdk.api.action.DocumentAction;
import com.veeva.vault.sdk.api.action.DocumentActionContext;
import com.veeva.vault.sdk.api.action.DocumentActionInfo;
import com.veeva.vault.sdk.api.document.*;
import com.veeva.vault.custom.udc.vSDKSparkHelper;

import java.util.List;

/**
 * This class demonstrates how to use a Document User Action to send a document's information to a Spark Messaging Queue.
 * For this use case, the "vSDK: Create Vault to Vault Document Copy" user action is run on a "vSDK Document".
 * The user action will perform the following:
 * 
 * 		- Iterate over the documents and create one Spark Message per document
 *      - The message will get added to the "vsdk_doc_copy_out_queue__c" outbound queue
 *      - A pending entry is placed in the integration transactions record of the source vault, to mark it for copy
 *      - The message will be sent to the target vault to notify it that a document is ready for copy
 * 
 */

@DocumentActionInfo(label="SDK: Create Vault to Vault Document Copy")
public class vSDKDocCopyDocumentAction implements DocumentAction {

	public boolean isExecutable(DocumentActionContext documentActionContext) {
		return true;
	}

	public void execute(DocumentActionContext documentActionContext) {

    	DocumentService documentService = ServiceLocator.locate((DocumentService.class));
    	LogService logService = ServiceLocator.locate(LogService.class);
    	
		List vaultIds = VaultCollections.newList();
		String event = "Send Doc for Copy";
		String integrationName = "Doc Copies";
		String targetIntegrationPointAPIName = "receive_document_copy__c";
		String queueName = "vsdk_doc_copy_out_queue__c";
		String object = "documents";

		if (vSDKSparkHelper.isIntegrationActive(integrationName)) {
			logService.info("Integration '" + integrationName + "' is active.");

			for (DocumentVersion documentVersion : documentActionContext.getDocumentVersions()) {

				if (vaultIds.size() < 500) {
					vaultIds.add(documentVersion.getValue("global_id__sys", ValueType.STRING));
				} else {
					vSDKSparkHelper.moveMessagesToQueue(queueName,
							object,
							targetIntegrationPointAPIName,
							event,
							vaultIds);
					vaultIds.clear();
				}
			}

			if (vaultIds.size() > 0) {
				vSDKSparkHelper.moveMessagesToQueue(queueName,
						object,
						targetIntegrationPointAPIName,
						event,
						vaultIds);
			}
		}
    }

}