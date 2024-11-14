package com.task08;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.util.HashMap;
import java.util.Map;

@LambdaHandler(
		lambdaName = "api_handler",
		roleName = "api_handler-role",
		isPublishVersion = true,
		aliasName = "${lambdas_alias_name}",
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
public class ApiHandler implements RequestHandler<Object, Map<String, Object>> {

	@Override
	public Map<String, Object> handleRequest(Object request, Context context) {
		System.out.println("Fetching weather forecast from Open-Meteo API...");

		// Initialize the WeatherForecastAPI
		WeatherForecastAPI weatherApi = new WeatherForecastAPI();
		Map<String, Object> resultMap = new HashMap<>();

		try {
			// Example coordinates for testing (use actual values or parameters)
			String latitude = "52.52"; // Example: Berlin
			String longitude = "13.405";

			// Fetch the weather forecast
			String forecastData = weatherApi.getWeatherForecast(latitude, longitude);

			// Populate the response map
			resultMap.put("statusCode", 200);
			resultMap.put("body", forecastData);
		} catch (Exception e) {
			System.err.println("Error fetching weather data: " + e.getMessage());
			resultMap.put("statusCode", 500);
			resultMap.put("body", "Failed to retrieve weather data");
		}

		return resultMap;
	}
}
