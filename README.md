# Graylog Elastic Beats Input Plugin

Graylog input plugin for Elastic beats shipper. Support for filebeat, packetbeat and topbeat.
Tested with beats platform 1.0.0 GA.

For more info on Elastic Beats platform visit [Beats product page] (https://www.elastic.co/products/beats)

How To
------

* Download the plugin [jar] (https://github.com/sivasamyk/graylog-beats-plugin/releases/download/v1.0.0/graylog-input-beats-1.0.0.jar) and copy the jar to plugin directory in Graylog server installation

* Restart Graylog Server

* Launch new input of type "Beats" 

* Configure logstash output in the beats YML configuration file and start the beats shipper.