package org.zirbes.kinesis.adapter

import com.squareup.okhttp.OkHttpClient

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory
import com.amazonaws.services.kinesis.model.Record

import groovy.transform.CompileStatic

@CompileStatic
class KinesisProcessorFactory implements IRecordProcessorFactory {

    protected final EndpointConfiguration endpointConfig
    protected final OkHttpClient client = new OkHttpClient()

    KinesisProcessorFactory(EndpointConfiguration endpointConfig) {
        this.endpointConfig = endpointConfig
    }

    @Override
    IRecordProcessor createProcessor() {
        return new KinesisDataProcessor(endpointConfig, client)
    }
}
