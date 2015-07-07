package org.zirbes.kinesis.adapter

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.amazonaws.services.kinesis.model.Record

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

@CompileStatic
@Slf4j
class KinesisHttpForwarder {

    // Kinesis
    static final String REGION = 'us-east-1'
    static final String STREAM = 'firehose'
    static final String APPLICATION_NAME = 'kinesis-http-adapter'

    // HTTP Endpoint
    static final String MEDIA_TYPE = 'application/json; charset=utf-8'
    static final String METHOD = 'POST'
    static final String URL = 'http://localhost:8080/data'
    static final Set<Integer> OK_ERRORS = [ 400, 408, 422 ] as Set

    AWSCredentialsProvider credProvider = new ProfileCredentialsProvider()

    KinesisHttpForwarder() { }

    void start() {
        EndpointConfiguration endpointConfig = new EndpointConfiguration(METHOD, URL, MEDIA_TYPE, OK_ERRORS)

        IRecordProcessorFactory recordProcessorFactory = new KinesisProcessorFactory(endpointConfig)
        Worker worker = new Worker(recordProcessorFactory, configureConsumer())
        worker.run()

    }

    protected KinesisClientLibConfiguration configureConsumer() {
        return new KinesisClientLibConfiguration(APPLICATION_NAME, STREAM, credProvider, workerId)
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
                .withRegionName(REGION)
    }

    protected String getWorkerId() {
        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
    }

}
