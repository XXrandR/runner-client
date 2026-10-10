package com.maximus.runner.application.command;

import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import com.maximus.runner.*;
import java.util.Map;

public final class ResultValues {
    private ResultValues() { }
    public static Struct struct(Map<String, ?> values) {
        Struct.Builder result = Struct.newBuilder();
        values.forEach((k, v) -> result.putFields(k, value(v)));
        return result.build();
    }
    private static Value value(Object object) {
        Value.Builder builder = Value.newBuilder();
        if (object == null) return builder.setNullValue(NullValue.NULL_VALUE).build();
        if (object instanceof Boolean b) return builder.setBoolValue(b).build();
        if (object instanceof Number n) return builder.setNumberValue(n.doubleValue()).build();
        if (object instanceof Map<?, ?> map) {
            Struct.Builder fields = Struct.newBuilder();
            map.forEach((k, v) -> fields.putFields(String.valueOf(k), value(v)));
            return builder.setStructValue(fields).build();
        }
        if (object instanceof Iterable<?> items) {
            ListValue.Builder list = ListValue.newBuilder();
            items.forEach(item -> list.addValues(value(item)));
            return builder.setListValue(list).build();
        }
        return builder.setStringValue(String.valueOf(object)).build();
    }
    public static CommandResult.Builder generic(Map<String, ?> values) {
        return CommandResult.newBuilder().setGeneric(GenericResponse.newBuilder().setResult(struct(values)));
    }
    public static String json(MessageOrBuilder value) {
        try { return JsonFormat.printer().omittingInsignificantWhitespace().print(value); }
        catch (InvalidProtocolBufferException exception) { throw new IllegalStateException(exception); }
    }
}
