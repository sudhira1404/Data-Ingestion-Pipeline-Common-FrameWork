package com.target.kelsaapi.common.vo.google.response.admanager.delivery;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;

public class GamJsonSerializationExclusionStrategy implements ExclusionStrategy {
    private final Class<?> excludedThisClass;

    public GamJsonSerializationExclusionStrategy(Class<?> excludedThisClass) {
        this.excludedThisClass = excludedThisClass;
    }

    @Override
    public boolean shouldSkipField(FieldAttributes f) {
        String name = f.getName();
        return name.equals("__equalsCalc") || name.equals("__hashCodeCalc") || excludedThisClass.equals(f.getDeclaredClass());
    }

    @Override
    public boolean shouldSkipClass(Class<?> clazz) {
        return excludedThisClass.equals(clazz);
    }
}
