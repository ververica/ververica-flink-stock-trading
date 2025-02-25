package com.ververica.serdes;

import com.google.gson.Gson;
import com.ververica.models.MarketEvent;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

public class MarketEventSerdes extends AbstractDeserializationSchema<MarketEvent> {
    private Gson gson;

    @Override
    public void open(InitializationContext context) throws Exception {
        gson = new Gson();
        super.open(context);
    }

    @Override
    public MarketEvent deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), MarketEvent.class);
    }
}
