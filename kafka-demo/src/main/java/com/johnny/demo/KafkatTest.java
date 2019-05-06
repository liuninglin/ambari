package com.johnny.demo;

public class KafkatTest {

    public static void main(String[] args) {
        System.setProperty("java.security.krb5.conf",  KafkatTest.class.getClassLoader().getResource("krb5.conf").getPath());
        System.setProperty("java.security.auth.login.config", KafkatTest.class.getClassLoader().getResource("kafka-jaas.conf").getPath());
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("sun.security.krb5.debug", "true");

        new KafkaProducerCreator(KafkaProperties.TOPIC).start();

        new KafkaConsumerCreator(KafkaProperties.TOPIC).start();
    }
}
