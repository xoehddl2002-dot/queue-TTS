package com.aitts.queuetts.gateway.api

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
open class GatewayApiApplication

fun main(args: Array<String>) {
    runApplication<GatewayApiApplication>(*args)
}
