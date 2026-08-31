package com.aitts.queuetts.gateway.api.controller

import com.aitts.queuetts.gateway.api.service.GatewayService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@Tag(name = "Gateway", description = "gateway API: health, styles.")
@RestController
class GatewayController(
    private val gatewayService: GatewayService,
) {
    @Operation(summary = "Health check", description = "Return gateway liveness and lightweight runtime counts.")
    @GetMapping("/api/health")
    fun health(): ResponseEntity<*> {
        val result=gatewayService.health()
        val error = result.leftOrNull()
        return if (error == null) ResponseEntity.ok(result.getOrNull())
        else ResponseEntity.status(error.status).body(error)
    }


    @Operation(
        summary = "styles list",
        description = "Return the available voice styles through the worker queue. " +
            "Omit 'model' to merge every engine pool; pass it to query one (e.g. supertonic, qwen).",
    )
    @GetMapping("/api/styles")
    fun styles(@RequestParam(required = false) model: String?): ResponseEntity<*> {
        val result=gatewayService.styles(model)
        val error = result.leftOrNull()
        return if (error == null) ResponseEntity.ok(result.getOrNull())
        else ResponseEntity.status(error.status).body(error)
    }

}
