
package com.gslab.pepper.sampler;

import com.gslab.pepper.util.ProducerKeys;
import com.gslab.pepper.util.PropsKeys;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log.Logger;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * The PepperBoxKafkaSampler class custom java sampler for jmeter.
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class PepperBoxKafkaSampler extends AbstractJavaSamplerClient {

    private KafkaTemplate<String, Object> kafkaTemplate;

    // topic on which messages will be sent
    private String topic;

    //Message placeholder keys
    private String msg_key_placeHolder;
    private String msg_val_placeHolder;
    private boolean key_message_flag = false;

    private static final Logger log = LoggingManager.getLoggerForClass();

    /**
     * Set default parameters and their values
     *
     * @return
     */
    @Override
    public Arguments getDefaultParameters() {

        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerKeys.BOOTSTRAP_SERVERS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.KAFKA_TOPIC_CONFIG, ProducerKeys.KAFKA_TOPIC_CONFIG_DEFAULT);
        defaultParameters.addArgument(PropsKeys.MESSAGE_KEY_PLACEHOLDER_KEY, PropsKeys.MSG_KEY_PLACEHOLDER);
        defaultParameters.addArgument(PropsKeys.MESSAGE_VAL_PLACEHOLDER_KEY, PropsKeys.MSG_PLACEHOLDER);

        return defaultParameters;
    }

    /**
     * Gets invoked exactly once  before thread starts
     *
     * @param context
     */
    @Override
    public void setupTest(JavaSamplerContext context) {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, context.getParameter(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(ProducerConfig.RETRIES_CONFIG, 2);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 163840);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1); // 延迟时间ms
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000); // 超时配置
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        topic = context.getParameter(ProducerKeys.KAFKA_TOPIC_CONFIG);

        if ("YES".equals(context.getParameter(PropsKeys.KEYED_MESSAGE_KEY))) {
            key_message_flag= true;
            msg_key_placeHolder = context.getParameter(PropsKeys.MESSAGE_KEY_PLACEHOLDER_KEY);
        }
        msg_val_placeHolder = context.getParameter(PropsKeys.MESSAGE_VAL_PLACEHOLDER_KEY);

        ProducerFactory<String, Object> producerFactory = new DefaultKafkaProducerFactory<>(props);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);
    }


    /**
     * For each sample request this method is invoked and will return success/failure result
     *
     * @param context
     * @return
     */
    @Override
    public SampleResult runTest(JavaSamplerContext context) {

        SampleResult sampleResult = new SampleResult();
        sampleResult.sampleStart();
        Object message_val = JMeterContextService.getContext().getVariables().getObject(msg_val_placeHolder);
        ProducerRecord<String, Object> producerRecord;
        try {

            if (key_message_flag) {
                Object message_key = JMeterContextService.getContext().getVariables().getObject(msg_key_placeHolder);
                producerRecord = new ProducerRecord<String, Object>(topic, message_key.toString(), message_val);
            } else {
                producerRecord = new ProducerRecord<String, Object>(topic, message_val);
            }
            log.info("AAAAA producerRecord" + producerRecord.toString());

            kafkaTemplate.send(
                    topic,
                    UUID.randomUUID().toString(),
                    message_val);

            sampleResult.setResponseData(message_val.toString(), StandardCharsets.UTF_8.name());
            sampleResult.setSuccessful(true);
            sampleResult.sampleEnd();

        } catch (Exception e) {
            log.error("Failed to send message", e);
            sampleResult.setResponseData(e.getMessage(), StandardCharsets.UTF_8.name());
            sampleResult.setSuccessful(false);
            sampleResult.sampleEnd();

        }

        return sampleResult;
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        // producerFactory.stop();
    }
}
