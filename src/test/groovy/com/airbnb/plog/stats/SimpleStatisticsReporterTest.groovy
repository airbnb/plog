package com.airbnb.plog.stats

import com.airbnb.plog.Utils
import com.airbnb.plog.fragmentation.Defragmenter
import com.typesafe.config.ConfigFactory
import groovy.json.JsonSlurper
import kafka.javaapi.producer.Producer
import kafka.producer.ProducerConfig

class SimpleStatisticsReporterTest extends GroovyTestCase {
    private final static slurper = new JsonSlurper()
    private final static defragConfig = ConfigFactory.load().getConfig('plog.defrag')
    private final static clientId = 'SimpleStatisticsReporterTestClientId'
    static {
        final props = new Properties()
        props.put('client.id', clientId)
        props.put('metadata.broker.list', Utils.clientAddr.toString())
        new Producer(new ProducerConfig(props))
    }

    void testMinimalIsValidJSON() {
        testValidJSON(new SimpleStatisticsReporter(null))
    }

    void testIsValidJSONWithDefrag() {
        final stats = new SimpleStatisticsReporter(null)
        stats.withDefrag(new Defragmenter(stats, defragConfig))
        testValidJSON(stats)
    }

    void testIsValidJSONWithKafka() {
        final stats = new SimpleStatisticsReporter(clientId)
        testValidJSON(stats)
    }

    void testIsValidJSONWithDefragAndKafka() {
        final stats = new SimpleStatisticsReporter(clientId)
        stats.withDefrag(new Defragmenter(stats, defragConfig))
        testValidJSON(stats)
    }

    private void testValidJSON(SimpleStatisticsReporter stats) {
        final parsed = slurper.parseText(stats.toJSON())
        assert parsed instanceof Map
    }
}
