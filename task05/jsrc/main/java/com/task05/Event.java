package com.task05;

import lombok.*;
import java.util.Map;
import java.util.HashMap;  // If you're using HashMap or any other Map implementation


@Builder
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class Event {
    private String id;
    private int principalId;
    private String createdAt;
    private Map<String, String> body;
}
