package com.task05;

import lombok.*;
import java.util.Map;
import java.util.HashMap;  // If you're using HashMap or any other Map implementation


@Builder
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class Response {
    private int statusCode;
    private Event event;
}
