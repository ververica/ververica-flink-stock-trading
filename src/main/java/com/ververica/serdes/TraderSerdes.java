package com.ververica.serdes;

import com.google.gson.Gson;
import com.ververica.models.Trader;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

public class TraderSerdes extends AbstractDeserializationSchema<Trader> {
    private Gson gson;

    @Override
    public void open(InitializationContext context) throws Exception {
        gson = new Gson();
        super.open(context);
    }

    @Override
    public Trader deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), Trader.class);
    }
}
