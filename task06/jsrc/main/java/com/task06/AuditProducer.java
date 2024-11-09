package com.task06;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.util.*;
import java.util.stream.Collectors;
import java.util.UUID;
import java.time.Instant;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.lambda.runtime.events.DynamodbStreamRecord; // Added the correct import

@LambdaHandler(
		lambdaName = "audit_producer",
		roleName = "audit_producer-role",
		isPublishVersion = true,
		aliasName = "${lambdas_alias_name}",
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
public class AuditProducer implements RequestHandler<List<DynamodbStreamRecord>, Map<String, Object>> {

	private static final String AUDIT_TABLE_NAME = "Audit";  // The audit table name

	@Override
	public Map<String, Object> handleRequest(List<DynamodbStreamRecord> event, Context context) {
		// DynamoDB client setup
		AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClient.builder().build();
		DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);

		// Result map to return status
		Map<String, Object> resultMap = new HashMap<>();

		try {
			// Loop through the event records
			for (DynamodbStreamRecord record : event) {
				if (record.getEventName().equals("INSERT") || record.getEventName().equals("MODIFY")) {
					// Extract new and old data from the DynamoDB stream record
					Map<String, Object> newData = record.getDynamodb().getNewImage();
					Map<String, Object> oldData = record.getDynamodb().getOldImage();

					// Generate a unique id for the audit entry (UUID)
					String auditId = UUID.randomUUID().toString();

					// Create the modification time (current time)
					String modificationTime = Instant.now().toString();

					// Extract the itemKey (assumes the key is a string field 'key')
					String itemKey = (String) newData.get("key");

					// Build the audit record
					Map<String, Object> auditRecord = new HashMap<>();
					auditRecord.put("id", auditId);  // UUID
					auditRecord.put("itemKey", itemKey);
					auditRecord.put("modificationTime", modificationTime);

					// Create ValueMap for inserting into DynamoDB
					ValueMap valueMap = new ValueMap();
					valueMap.withString("id", auditId)
							.withString("itemKey", itemKey)
							.withString("modificationTime", modificationTime);

					// Handle "INSERT" case - new value will be logged
					if (record.getEventName().equals("INSERT")) {
						valueMap.withMap("newValue", newData);
					}
					// Handle "MODIFY" case - old and new values will be logged
					else if (record.getEventName().equals("MODIFY")) {
						valueMap.withString("updatedAttribute", "value");
						valueMap.withString("oldValue", (String) oldData.get("value"));
						valueMap.withString("newValue", (String) newData.get("value"));
					}

					// Insert the audit record into the DynamoDB "Audit" table
					Table auditTable = dynamoDB.getTable(AUDIT_TABLE_NAME);
					auditTable.putItem(new PutItemSpec().withItem(valueMap));
				}
			}

			resultMap.put("statusCode", 200);
			resultMap.put("body", "Successfully processed stream event and logged audit records.");
		} catch (Exception e) {
			resultMap.put("statusCode", 500);
			resultMap.put("body", "Error processing event: " + e.getMessage());
			context.getLogger().log("Error: " + e.getMessage());
		}

		return resultMap;
	}
}
