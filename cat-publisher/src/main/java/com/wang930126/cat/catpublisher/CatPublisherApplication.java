package com.wang930126.cat.catpublisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.wang930126.cat.catpublisher.mapper")
public class CatPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(CatPublisherApplication.class, args);
    }

}
