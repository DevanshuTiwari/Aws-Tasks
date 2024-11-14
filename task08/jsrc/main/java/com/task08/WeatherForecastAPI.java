package com.task08;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class WeatherForecastAPI {
    private static final String BASE_URL = "https://api.open-meteo.com/";

    public String getWeatherForecast(String latitude, String longitude) throws Exception {
        String url = BASE_URL + "v1/forecast?latitude=" + latitude + "&longitude=" + longitude + "&hourly=temperature_2m";
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }
}
