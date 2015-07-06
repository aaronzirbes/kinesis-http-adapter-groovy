package org.zirbes.kinesis.adapter

import com.amazonaws.services.kinesis.model.Record
import com.squareup.okhttp.MediaType
import com.squareup.okhttp.OkHttpClient
import com.squareup.okhttp.Request
import com.squareup.okhttp.RequestBody
import com.squareup.okhttp.Response

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

@CompileStatic
@Slf4j
class HttpForwarder {

    protected final EndpointConfiguration endpointConfig
    protected final OkHttpClient client

    HttpForwarder(EndpointConfiguration endpointConfig, OkHttpClient client) {
        this.endpointConfig = endpointConfig
        this.client = client
    }

    HttpResult post(Record record) {
        HttpResult result = new HttpResult(record)

        final MediaType mediaType = MediaType.parse(endpointConfig.mediaType)
        byte[] data = record.data.array()

        String dataText = new String(data,'UTF-8')
        log.info "HTTP ${endpointConfig.method} ${endpointConfig.url} data: ${dataText}"

        RequestBody body = RequestBody.create(mediaType, data)
        Request request = new Request.Builder().url(endpointConfig.url)
                                               .method(endpointConfig.method, body)
                                               .build()
        try {
            Response response = client.newCall(request).execute()
            if (response.isSuccessful() || response.code() in endpointConfig.okErrors) {
                log.info "HTTP ${response.request()?.method()} ${response.request()?.url()} : " +
                         "STATUS ${response.code()}, returned ${response.body()?.string()}"
                return result.succeded()
            }
            log.error "HTTP ${endpointConfig.method} ${endpointConfig.url} data: ${dataText}"
            log.error "HTTP ${response.request()?.method()} ${response.request()?.url()} : " +
                      "STATUS ${response.code()}, returned ${response.body()?.string()}"

        } catch (java.net.ConnectException |
                 java.net.HttpRetryException |
                 java.net.MalformedURLException |
                 java.net.NoRouteToHostException |
                 java.net.PortUnreachableException |
                 java.net.ProtocolException |
                 java.net.SocketException |
                 java.net.SocketTimeoutException |
                 java.net.UnknownHostException |
                 java.net.UnknownServiceException |
                 java.net.URISyntaxException ex) {
            log.error "${ex.message}: HTTP ${endpointConfig.method} ${endpointConfig.url}"
        }


        return result.failed()

    }

}
