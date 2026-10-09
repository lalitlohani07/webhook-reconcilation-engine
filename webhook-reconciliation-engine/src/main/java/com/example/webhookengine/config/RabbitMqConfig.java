package com.example.webhookengine.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMqConfig {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqConfig.class);

    @Value("${app.rabbitmq.exchange:webhook.events}")
    private String exchangeName;

    @Value("${app.rabbitmq.queue:webhook.processing}")
    private String queueName;

    @Value("${app.rabbitmq.routing-key:webhook.received}")
    private String routingKey;

    @Bean
    public DirectExchange webhookExchange() {
        return new DirectExchange(exchangeName, true, false);
    }

    @Bean
    public Queue webhookProcessingQueue() {
        return QueueBuilder.durable(queueName).build();
    }

    @Bean
    public Binding webhookBinding(Queue webhookProcessingQueue, DirectExchange webhookExchange) {
        return BindingBuilder.bind(webhookProcessingQueue)
            .to(webhookExchange)
            .with(routingKey);
    }

    @Bean
    public MessageConverter jackson2JsonMessageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory, MessageConverter messageConverter) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter);
        template.setMandatory(true);

        template.setConfirmCallback((correlationData, ack, cause) -> {
            if (!ack) {
                log.warn("RabbitMQ publisher confirmation negative (NACK): cause={}", cause);
            } else {
                log.debug("RabbitMQ publisher confirmation ACK received for: {}", correlationData);
            }
        });

        template.setReturnsCallback(returned -> {
            log.error("RabbitMQ message returned (unroutable): message={}, replyCode={}, replyText={}, exchange={}, routingKey={}",
                returned.getMessage(), returned.getReplyCode(), returned.getReplyText(), returned.getExchange(), returned.getRoutingKey());
        });

        return template;
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getRoutingKey() {
        return routingKey;
    }
}
