package com.marina.simpletest.kafka.tutorial1;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsumerJmeter {


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "group-chalala");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        String topic = "first_topic";
        consumer.subscribe(Arrays.asList(topic));
        System.out.println("Subscribed to topic " + topic);
        int i = 0;

        long t = System.currentTimeMillis();
        long end = t + 5000;
        FileOutputStream f;
        PrintStream p;
        try {
            f = new FileOutputStream("//home//marina//data-marina.csv", true);
            p = new PrintStream(f);


            while (System.currentTimeMillis() < end) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    p.println("offset = " + record.offset() + " value = " + record.value());
                    System.out.println("OIIIIIIIIIIIIIIIIIIIIIIIIIIIII");
                    System.out.println("offset = " + record.offset() + " value = " + record.value());
                }
                consumer.commitSync();
            }
            consumer.close();
            p.close();
            f.close();
        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    /*
    <?xml version="1.0" encoding="UTF-8"?>
<jmeterTestPlan version="1.2" properties="5.0" jmeter="5.0 r1840935">
  <hashTree>
    <TestPlan guiclass="TestPlanGui" testclass="TestPlan" testname="Test Plan" enabled="true">
      <stringProp name="TestPlan.comments"></stringProp>
      <boolProp name="TestPlan.functional_mode">false</boolProp>
      <boolProp name="TestPlan.serialize_threadgroups">false</boolProp>
      <elementProp name="TestPlan.user_defined_variables" elementType="Arguments" guiclass="ArgumentsPanel" testclass="Arguments" testname="User Defined Variables" enabled="true">
        <collectionProp name="Arguments.arguments"/>
      </elementProp>
      <stringProp name="TestPlan.user_define_classpath"></stringProp>
    </TestPlan>
    <hashTree>
      <ThreadGroup guiclass="ThreadGroupGui" testclass="ThreadGroup" testname="Thread Group" enabled="true">
        <stringProp name="ThreadGroup.on_sample_error">continue</stringProp>
        <elementProp name="ThreadGroup.main_controller" elementType="LoopController" guiclass="LoopControlPanel" testclass="LoopController" testname="Loop Controller" enabled="true">
          <boolProp name="LoopController.continue_forever">false</boolProp>
          <intProp name="LoopController.loops">-1</intProp>
        </elementProp>
        <stringProp name="ThreadGroup.num_threads">4</stringProp>
        <stringProp name="ThreadGroup.ramp_time">1</stringProp>
        <longProp name="ThreadGroup.start_time">1487173362000</longProp>
        <longProp name="ThreadGroup.end_time">1487173362000</longProp>
        <boolProp name="ThreadGroup.scheduler">true</boolProp>
        <stringProp name="ThreadGroup.duration">60</stringProp>
        <stringProp name="ThreadGroup.delay"></stringProp>
      </ThreadGroup>
      <hashTree>
        <JavaSampler guiclass="JavaTestSamplerGui" testclass="JavaSampler" testname="PlainText" enabled="true">
          <elementProp name="arguments" elementType="Arguments" guiclass="ArgumentsPanel" testclass="Arguments" enabled="true">
            <collectionProp name="Arguments.arguments">
              <elementProp name="bootstrap.servers" elementType="Argument">
                <stringProp name="Argument.name">bootstrap.servers</stringProp>
                <stringProp name="Argument.value">localhost:9092</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="zookeeper.servers" elementType="Argument">
                <stringProp name="Argument.name">zookeeper.servers</stringProp>
                <stringProp name="Argument.value">localhost:2181</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="kafka.topic.name" elementType="Argument">
                <stringProp name="Argument.name">kafka.topic.name</stringProp>
                <stringProp name="Argument.value">first_topic</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="key.serializer" elementType="Argument">
                <stringProp name="Argument.name">key.serializer</stringProp>
                <stringProp name="Argument.value">org.apache.kafka.common.serialization.StringSerializer</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="value.serializer" elementType="Argument">
                <stringProp name="Argument.name">value.serializer</stringProp>
                <stringProp name="Argument.value">org.apache.kafka.common.serialization.StringSerializer</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="compression.type" elementType="Argument">
                <stringProp name="Argument.name">compression.type</stringProp>
                <stringProp name="Argument.value">none</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="batch.size" elementType="Argument">
                <stringProp name="Argument.name">batch.size</stringProp>
                <stringProp name="Argument.value">16384</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="linger.ms" elementType="Argument">
                <stringProp name="Argument.name">linger.ms</stringProp>
                <stringProp name="Argument.value">0</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="buffer.memory" elementType="Argument">
                <stringProp name="Argument.name">buffer.memory</stringProp>
                <stringProp name="Argument.value">33554432</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="acks" elementType="Argument">
                <stringProp name="Argument.name">acks</stringProp>
                <stringProp name="Argument.value">1</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="send.buffer.bytes" elementType="Argument">
                <stringProp name="Argument.name">send.buffer.bytes</stringProp>
                <stringProp name="Argument.value">131072</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="receive.buffer.bytes" elementType="Argument">
                <stringProp name="Argument.name">receive.buffer.bytes</stringProp>
                <stringProp name="Argument.value">32768</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="security.protocol" elementType="Argument">
                <stringProp name="Argument.name">security.protocol</stringProp>
                <stringProp name="Argument.value">PLAINTEXT</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="message.placeholder.key" elementType="Argument">
                <stringProp name="Argument.name">message.placeholder.key</stringProp>
                <stringProp name="Argument.value">MESSAGE</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="kerberos.auth.enabled" elementType="Argument">
                <stringProp name="Argument.name">kerberos.auth.enabled</stringProp>
                <stringProp name="Argument.value">NO</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="java.security.auth.login.config" elementType="Argument">
                <stringProp name="Argument.name">java.security.auth.login.config</stringProp>
                <stringProp name="Argument.value">&lt;JAAS File Location&gt;</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="java.security.krb5.conf" elementType="Argument">
                <stringProp name="Argument.name">java.security.krb5.conf</stringProp>
                <stringProp name="Argument.value">&lt;krb5.conf location&gt;</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="sasl.kerberos.service.name" elementType="Argument">
                <stringProp name="Argument.name">sasl.kerberos.service.name</stringProp>
                <stringProp name="Argument.value">kafka</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
            </collectionProp>
          </elementProp>
          <stringProp name="classname">com.gslab.pepper.sampler.PepperBoxKafkaSampler</stringProp>
        </JavaSampler>
        <hashTree/>
        <com.gslab.pepper.config.plaintext.PlainTextConfigElement guiclass="TestBeanGUI" testclass="com.gslab.pepper.config.plaintext.PlainTextConfigElement" testname="Kafka PlainText Load Generator Config" enabled="true">
          <stringProp name="placeHolder">MESSAGE</stringProp>
          <stringProp name="configClass">com.gslab.pepper.loadgen.impl.PlaintTextLoadGenerator</stringProp>
          <stringProp name="jsonSchema">{
	&quot;messageId&quot;:{{SEQUENCE(&quot;messageId&quot;, 1, 1)}},
	&quot;messageBody&quot;:&quot;{{RANDOM_ALPHA_NUMERIC(&quot;abcedefghijklmnopqrwxyzABCDEFGHIJKLMNOPQRWXYZ&quot;, 100)}}&quot;,
	&quot;messageCategory&quot;:&quot;{{RANDOM_STRING(&quot;Finance&quot;, &quot;Insurance&quot;, &quot;Healthcare&quot;, &quot;Shares&quot;)}}&quot;,
	&quot;messageStatus&quot;:&quot;{{RANDOM_STRING(&quot;Accepted&quot;,&quot;Pending&quot;,&quot;Processing&quot;,&quot;Rejected&quot;)}}&quot;,
	&quot;messageTime&quot;:{{TIMESTAMP()}}
}</stringProp>
        </com.gslab.pepper.config.plaintext.PlainTextConfigElement>
        <hashTree/>
        <JavaSampler guiclass="JavaTestSamplerGui" testclass="JavaSampler" testname="Serialized" enabled="false">
          <elementProp name="arguments" elementType="Arguments" guiclass="ArgumentsPanel" testclass="Arguments" enabled="true">
            <collectionProp name="Arguments.arguments">
              <elementProp name="bootstrap.servers" elementType="Argument">
                <stringProp name="Argument.name">bootstrap.servers</stringProp>
                <stringProp name="Argument.value">&lt;Broker List&gt;</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="zookeeper.servers" elementType="Argument">
                <stringProp name="Argument.name">zookeeper.servers</stringProp>
                <stringProp name="Argument.value">&lt;Zookeeper List&gt;</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="kafka.topic.name" elementType="Argument">
                <stringProp name="Argument.name">kafka.topic.name</stringProp>
                <stringProp name="Argument.value">&lt;Topic&gt;</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="key.serializer" elementType="Argument">
                <stringProp name="Argument.name">key.serializer</stringProp>
                <stringProp name="Argument.value">org.apache.kafka.common.serialization.StringSerializer</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="value.serializer" elementType="Argument">
                <stringProp name="Argument.name">value.serializer</stringProp>
                <stringProp name="Argument.value">org.apache.kafka.common.serialization.StringSerializer</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="compression.type" elementType="Argument">
                <stringProp name="Argument.name">compression.type</stringProp>
                <stringProp name="Argument.value">none</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="batch.size" elementType="Argument">
                <stringProp name="Argument.name">batch.size</stringProp>
                <stringProp name="Argument.value">16384</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="linger.ms" elementType="Argument">
                <stringProp name="Argument.name">linger.ms</stringProp>
                <stringProp name="Argument.value">0</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="buffer.memory" elementType="Argument">
                <stringProp name="Argument.name">buffer.memory</stringProp>
                <stringProp name="Argument.value">33554432</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="acks" elementType="Argument">
                <stringProp name="Argument.name">acks</stringProp>
                <stringProp name="Argument.value">1</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="send.buffer.bytes" elementType="Argument">
                <stringProp name="Argument.name">send.buffer.bytes</stringProp>
                <stringProp name="Argument.value">131072</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="receive.buffer.bytes" elementType="Argument">
                <stringProp name="Argument.name">receive.buffer.bytes</stringProp>
                <stringProp name="Argument.value">32768</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="security.protocol" elementType="Argument">
                <stringProp name="Argument.name">security.protocol</stringProp>
                <stringProp name="Argument.value">PLAINTEXT</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="message.placeholder.key" elementType="Argument">
                <stringProp name="Argument.name">message.placeholder.key</stringProp>
                <stringProp name="Argument.value">MESSAGE</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="kerberos.auth.enabled" elementType="Argument">
                <stringProp name="Argument.name">kerberos.auth.enabled</stringProp>
                <stringProp name="Argument.value">NO</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="java.security.auth.login.config" elementType="Argument">
                <stringProp name="Argument.name">java.security.auth.login.config</stringProp>
                <stringProp name="Argument.value">&lt;JAAS File Location&gt;</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="java.security.krb5.conf" elementType="Argument">
                <stringProp name="Argument.name">java.security.krb5.conf</stringProp>
                <stringProp name="Argument.value">&lt;krb5.conf location&gt;</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
              <elementProp name="sasl.kerberos.service.name" elementType="Argument">
                <stringProp name="Argument.name">sasl.kerberos.service.name</stringProp>
                <stringProp name="Argument.value">kafka</stringProp>
                <stringProp name="Argument.metadata">=</stringProp>
              </elementProp>
            </collectionProp>
          </elementProp>
          <stringProp name="classname">com.gslab.pepper.sampler.PepperBoxKafkaSampler</stringProp>
        </JavaSampler>
        <hashTree/>
        <com.gslab.pepper.config.serialized.SerializedConfigElement guiclass="TestBeanGUI" testclass="com.gslab.pepper.config.serialized.SerializedConfigElement" testname="Kafka Serialized Load Generator Config" enabled="false">
          <stringProp name="className">com.gslab.pepper.Message</stringProp>
          <stringProp name="messagePlaceholder">MESSAGE</stringProp>
          <collectionProp name="objProperties">
            <elementProp name="" elementType="com.gslab.pepper.model.FieldExpressionMapping">
              <stringProp name="fieldName">messageId</stringProp>
              <stringProp name="fieldExpression">SEQUENCE(&quot;messageId&quot;, 1, 1)</stringProp>
            </elementProp>
            <elementProp name="" elementType="com.gslab.pepper.model.FieldExpressionMapping">
              <stringProp name="fieldName">messageBody</stringProp>
              <stringProp name="fieldExpression">RANDOM_ALPHA_NUMERIC(&quot;abcedefghijklmnopqrwxyzABCDEFGHIJKLMNOPQRWXYZ&quot;, 100)</stringProp>
            </elementProp>
            <elementProp name="" elementType="com.gslab.pepper.model.FieldExpressionMapping">
              <stringProp name="fieldName">messageStatus</stringProp>
              <stringProp name="fieldExpression">RANDOM_STRING(&quot;Accepted&quot;,&quot;Pending&quot;,&quot;Processing&quot;,&quot;Rejected&quot;)</stringProp>
            </elementProp>
            <elementProp name="" elementType="com.gslab.pepper.model.FieldExpressionMapping">
              <stringProp name="fieldName">messageCategory</stringProp>
              <stringProp name="fieldExpression">RANDOM_STRING(&quot;Finance&quot;, &quot;Insurance&quot;, &quot;Healthcare&quot;, &quot;Shares&quot;)</stringProp>
            </elementProp>
            <elementProp name="" elementType="com.gslab.pepper.model.FieldExpressionMapping">
              <stringProp name="fieldName">messageTime</stringProp>
              <stringProp name="fieldExpression">TIMESTAMP()</stringProp>
            </elementProp>
          </collectionProp>
          <stringProp name="placeHolder">MESSAGE1</stringProp>
        </com.gslab.pepper.config.serialized.SerializedConfigElement>
        <hashTree/>
        <ResultCollector guiclass="SummaryReport" testclass="ResultCollector" testname="Summary Report" enabled="true">
          <boolProp name="ResultCollector.error_logging">false</boolProp>
          <objProp>
            <name>saveConfig</name>
            <value class="SampleSaveConfiguration">
              <time>true</time>
              <latency>true</latency>
              <timestamp>true</timestamp>
              <success>true</success>
              <label>true</label>
              <code>true</code>
              <message>true</message>
              <threadName>true</threadName>
              <dataType>true</dataType>
              <encoding>false</encoding>
              <assertions>true</assertions>
              <subresults>true</subresults>
              <responseData>false</responseData>
              <samplerData>false</samplerData>
              <xml>false</xml>
              <fieldNames>true</fieldNames>
              <responseHeaders>false</responseHeaders>
              <requestHeaders>false</requestHeaders>
              <responseDataOnError>false</responseDataOnError>
              <saveAssertionResultsFailureMessage>true</saveAssertionResultsFailureMessage>
              <assertionsResultsToSave>0</assertionsResultsToSave>
              <bytes>true</bytes>
              <threadCounts>true</threadCounts>
              <idleTime>true</idleTime>
            </value>
          </objProp>
          <stringProp name="filename"></stringProp>
        </ResultCollector>
        <hashTree/>
        <ResultCollector guiclass="ViewResultsFullVisualizer" testclass="ResultCollector" testname="View Results Tree" enabled="true">
          <boolProp name="ResultCollector.error_logging">false</boolProp>
          <objProp>
            <name>saveConfig</name>
            <value class="SampleSaveConfiguration">
              <time>true</time>
              <latency>true</latency>
              <timestamp>true</timestamp>
              <success>true</success>
              <label>true</label>
              <code>true</code>
              <message>true</message>
              <threadName>true</threadName>
              <dataType>true</dataType>
              <encoding>false</encoding>
              <assertions>true</assertions>
              <subresults>true</subresults>
              <responseData>false</responseData>
              <samplerData>false</samplerData>
              <xml>false</xml>
              <fieldNames>true</fieldNames>
              <responseHeaders>false</responseHeaders>
              <requestHeaders>false</requestHeaders>
              <responseDataOnError>false</responseDataOnError>
              <saveAssertionResultsFailureMessage>true</saveAssertionResultsFailureMessage>
              <assertionsResultsToSave>0</assertionsResultsToSave>
              <bytes>true</bytes>
              <sentBytes>true</sentBytes>
              <url>true</url>
              <threadCounts>true</threadCounts>
              <idleTime>true</idleTime>
              <connectTime>true</connectTime>
            </value>
          </objProp>
          <stringProp name="filename"></stringProp>
        </ResultCollector>
        <hashTree/>
      </hashTree>
      <ThreadGroup guiclass="ThreadGroupGui" testclass="ThreadGroup" testname="Thread Group" enabled="true">
        <stringProp name="ThreadGroup.on_sample_error">continue</stringProp>
        <elementProp name="ThreadGroup.main_controller" elementType="LoopController" guiclass="LoopControlPanel" testclass="LoopController" testname="Loop Controller" enabled="true">
          <boolProp name="LoopController.continue_forever">false</boolProp>
          <stringProp name="LoopController.loops">1</stringProp>
        </elementProp>
        <stringProp name="ThreadGroup.num_threads">1</stringProp>
        <stringProp name="ThreadGroup.ramp_time">1</stringProp>
        <boolProp name="ThreadGroup.scheduler">false</boolProp>
        <stringProp name="ThreadGroup.duration"></stringProp>
        <stringProp name="ThreadGroup.delay"></stringProp>
      </ThreadGroup>
      <hashTree>
        <JSR223Sampler guiclass="TestBeanGUI" testclass="JSR223Sampler" testname="JSR223 Sampler" enabled="true">
          <stringProp name="cacheKey">true</stringProp>
          <stringProp name="filename"></stringProp>
          <stringProp name="parameters"></stringProp>
          <stringProp name="script">import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.io.FileOutputStream;
import java.io.PrintStream;


        Properties props = new Properties();
        props.put(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
        props.put(&quot;group.id&quot;, &quot;group-chalala&quot;);
        props.put(&quot;enable.auto.commit&quot;, &quot;true&quot;);
        props.put(&quot;auto.commit.interval.ms&quot;, &quot;1000&quot;);
        props.put(&quot;session.timeout.ms&quot;, &quot;30000&quot;);
        props.put(&quot;key.deserializer&quot;,
                &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
        props.put(&quot;value.deserializer&quot;,
                &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
        KafkaConsumer&lt;String, String&gt; consumer = new KafkaConsumer&lt;String, String&gt;(props);
        String topic = &quot;first_topic&quot;;
        consumer.subscribe(Arrays.asList(topic));
        System.out.println(&quot;Subscribed to topic &quot; + topic);
        int i = 0;

        long t = System.currentTimeMillis();
        long end = t + 5000;
        FileOutputStream f;
        PrintStream p;
        try {
            f = new FileOutputStream(&quot;//home//marina//data-marina.csv&quot;, true);
            p = new PrintStream(f);


            while (System.currentTimeMillis() &lt; end) {
                ConsumerRecords&lt;String, String&gt; records = consumer.poll(100);
                for (ConsumerRecord&lt;String, String&gt; record : records) {
                    p.println(&quot;offset = &quot; + record.offset() + &quot; value = &quot; + record.value());
                    System.out.println(&quot;OIIIIIIIIIIIIIIIIIIIIIIIIIIIII&quot;);
                    System.out.println(&quot;offset = &quot; + record.offset() + &quot; value = &quot; + record.value());
                }
                consumer.commitSync();
            }
            consumer.close();
            p.close();
            f.close();
        } catch (Exception e) {
            e.printStackTrace();

        }</stringProp>
          <stringProp name="scriptLanguage">groovy</stringProp>
        </JSR223Sampler>
        <hashTree/>
        <ResultCollector guiclass="ViewResultsFullVisualizer" testclass="ResultCollector" testname="View Results Tree" enabled="true">
          <boolProp name="ResultCollector.error_logging">false</boolProp>
          <objProp>
            <name>saveConfig</name>
            <value class="SampleSaveConfiguration">
              <time>true</time>
              <latency>true</latency>
              <timestamp>true</timestamp>
              <success>true</success>
              <label>true</label>
              <code>true</code>
              <message>true</message>
              <threadName>true</threadName>
              <dataType>true</dataType>
              <encoding>false</encoding>
              <assertions>true</assertions>
              <subresults>true</subresults>
              <responseData>false</responseData>
              <samplerData>false</samplerData>
              <xml>false</xml>
              <fieldNames>true</fieldNames>
              <responseHeaders>false</responseHeaders>
              <requestHeaders>false</requestHeaders>
              <responseDataOnError>false</responseDataOnError>
              <saveAssertionResultsFailureMessage>true</saveAssertionResultsFailureMessage>
              <assertionsResultsToSave>0</assertionsResultsToSave>
              <bytes>true</bytes>
              <sentBytes>true</sentBytes>
              <url>true</url>
              <threadCounts>true</threadCounts>
              <idleTime>true</idleTime>
              <connectTime>true</connectTime>
            </value>
          </objProp>
          <stringProp name="filename"></stringProp>
        </ResultCollector>
        <hashTree/>
      </hashTree>
    </hashTree>
  </hashTree>
</jmeterTestPlan>
    * */
}
