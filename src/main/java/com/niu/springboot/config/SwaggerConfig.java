package com.niu.springboot.config;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.ApiSelectorBuilder;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * Swagger 配置类
 *
 * @author [nza]
 * @version 1.0 [2021/06/03 16:00]
 * @createTime [2021/06/03 16:00]
 */
@Configuration
@EnableSwagger2
public class SwaggerConfig {
    /**
     * swagger 配置类对象
     *
     * @return {@link Docket}
     * @author nza
     * @createTime 2021/5/27 22:37
     */
    @Bean
    public Docket createRestApi() {

        Docket docket = new Docket(DocumentationType.SWAGGER_2);
        ApiInfoBuilder builder = new ApiInfoBuilder();
        // 设置 swagger 标题
        builder.title("Kafka Demo API");
        ApiInfo apiInfo = builder.build();
        docket.apiInfo(apiInfo);

        // 设置那些类出现在 swagger 里面
        ApiSelectorBuilder selectorBuilder = docket.select();
        // 类路径
        selectorBuilder.paths(PathSelectors.any());
        // 注解
        selectorBuilder.apis(RequestHandlerSelectors.withClassAnnotation(Api.class));
        selectorBuilder.apis(RequestHandlerSelectors.withMethodAnnotation(ApiOperation.class));
        docket = selectorBuilder.build();

        return docket;
    }
}
