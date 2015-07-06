package org.zirbes.kinesis.adapter

class KinesisAdapterApp {

    static void main(String[] argv) {
        KinesisHttpForwarder kinesisHttpForwarder = new KinesisHttpForwarder()
        kinesisHttpForwarder.start()
    }
}
