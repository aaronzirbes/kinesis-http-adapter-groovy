package org.zirbes.kinesis.adapter

class EndpointConfiguration {

    final String method = 'POST'
    final String url
    final Set<Integer> okErrors = [400, 408, 422] as Set
    final String mediaType = 'application/json; charset=utf-8'

    EndpointConfiguration(String method,
                          String url,
                          String mediaType,
                          Set<Integer> okErrors) {
        this.method = method
        this.url = url
        this.mediaType = mediaType
        this.okErrors = okErrors
    }

}
