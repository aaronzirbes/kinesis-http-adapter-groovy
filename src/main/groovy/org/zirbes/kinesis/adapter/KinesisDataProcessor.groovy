package org.zirbes.kinesis.adapter

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record

import com.squareup.okhttp.MediaType
import com.squareup.okhttp.OkHttpClient
import com.squareup.okhttp.Request
import com.squareup.okhttp.RequestBody
import com.squareup.okhttp.Response

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import rx.Observable
import rx.Subscriber

@CompileStatic
@Slf4j
class KinesisDataProcessor implements IRecordProcessor {

    String shardId

    protected final HttpForwarder forwarder

    KinesisDataProcessor(EndpointConfiguration endpointConfig, OkHttpClient client) {
        forwarder = new HttpForwarder(endpointConfig, client)
    }

    @Override
    void initialize(String shardId) {
        this.shardId = shardId
    }

    @Override
    void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        Observable.create{ Subscriber<HttpResult> subscriber ->
            Thread.start {
                records.each{ record ->
                    if (subscriber.unsubscribed) { return }
                    subscriber.onNext(forwarder.post(record))

                }
                if (!subscriber.unsubscribed) { subscriber.onCompleted() }
            }
            return subscriber
        }.subscribe{ HttpResult result ->
            if (result.success) {
                checkpointer.checkpoint(result.record)
            }
        }
    }

    @Override
    void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        log.warn "Shutting down. ${reason}"
        checkpointer.checkpoint()
    }
}
