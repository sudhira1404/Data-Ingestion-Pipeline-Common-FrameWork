package com.target.kelsaapi.common.vo.pinterest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import org.springframework.lang.Nullable;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor(force = true)
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class PinterestAdAccountsResponse {
    private final List<Item> items;

    @Nullable
    private final String bookmark;

    @Getter
    @Setter
    @NoArgsConstructor(force = true)
    @AllArgsConstructor
    @Builder
    public static class Item {
        private final String id;
        private final String name;
        private final Owner owner;
        private final String country;
        private final String currency;
        private final List<String> permissions;
        @JsonProperty("created_time")
        private final long createdTime;
        @JsonProperty("updated_time")
        private final long updatedTime;
    }

    @Getter
    @Setter
    @NoArgsConstructor(force = true)
    @AllArgsConstructor
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Owner {
        private final String username;
        private final String id;
    }

}
