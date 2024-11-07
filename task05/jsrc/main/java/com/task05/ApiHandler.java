package com.task05;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(
		lambdaName = "api_handler",
		roleName = "api_handler-role",
		isPublishVersion = true,
		aliasName = "${lambdas_alias_name}",
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
public class ApiHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

	private final AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
	private static final String TABLE_NAME = "Events";

	@Override
	public Map<String, Object> handleRequest(Map<String, Object> request, Context context) {
		try {
			// Extract request parameters
			Integer principalId = (Integer) request.get("principalId");
			Map<String, String> content = (Map<String, String>) request.get("content");

			// Validate request data
			if (principalId == null || content == null) {
				return createErrorResponse("Invalid input");
			}

			// Create event data
			String eventId = UUID.randomUUID().toString();
			String createdAt = Instant.now().toString();

			// Prepare DynamoDB item
			Map<String, AttributeValue> item = new HashMap<>();
			item.put("id", new AttributeValue(eventId)); // Use only 'id' as the HASH key
			item.put("principalId", new AttributeValue().withN(principalId.toString()));
			item.put("createdAt", new AttributeValue(createdAt));
			item.put("body", new AttributeValue().withM(convertContentToAttributeMap(content)));

			// Put item in DynamoDB
			PutItemRequest putItemRequest = new PutItemRequest()
					.withTableName(TABLE_NAME)
					.withItem(item);
			dynamoDB.putItem(putItemRequest);

			// Create success response with statusCode 201
			return createSuccessResponse(eventId, principalId, createdAt, content);

		} catch (Exception e) {
			context.getLogger().log("Error: " + e.getMessage());
			return createErrorResponse("Internal Server Error");
		}
	}

	private Map<String, AttributeValue> convertContentToAttributeMap(Map<String, String> content) {
		Map<String, AttributeValue> attributeMap = new HashMap<>();
		content.forEach((key, value) -> attributeMap.put(key, new AttributeValue(value)));
		return attributeMap;
	}

	private Map<String, Object> createErrorResponse(String message) {
		Map<String, Object> response = new HashMap<>();
		response.put("statusCode", 400);
		response.put("error", message);
		return response;
	}

	private Map<String, Object> createSuccessResponse(String eventId, Integer principalId, String createdAt, Map<String, String> content) {
		Map<String, Object> responseBody = new HashMap<>();
		responseBody.put("id", eventId);
		responseBody.put("principalId", principalId);
		responseBody.put("createdAt", createdAt);
		responseBody.put("body", content);

		Map<String, Object> response = new HashMap<>();
		response.put("statusCode", 201);  // Explicitly set statusCode to 201 for success
		response.put("event", responseBody);
		return response;
	}
}
