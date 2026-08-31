package com.aitts.queuetts.gateway.api.dto

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty

// GatewayController(/api/health, /api/styles) 의 응답 DTO.

data class HealthResponse(
    val ok: Boolean,
    val status: String,
    val gateway: String,
    @field:JsonProperty("active_jobs")
    val activeJobs: Int,
    @field:JsonProperty("jobs_total")
    val jobsTotal: Int,
    val workers: List<WorkerResponse>,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class StyleInfo(
    val name: String,
    val kind: String,
    val path: String? = null,
    /** 이 보이스를 제공하는 합성 엔진(모델). 엔진마다 voice 목록이 달라 구분이 필요하다. */
    val model: String? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class StyleCatalogResponse(
    val styles: List<StyleInfo>,
    // worker 가 styles 와 함께 보고한 batchSize. worker 가 없거나 미보고면 null.
    // 여러 모델을 조회한 경우 기본 모델 값이며, 모델별 값은 [batchSizes] 에 있다.
    val batchSize: Int? = null,
    /** 모델별 batchSize. 엔진마다 배치 크기가 달라 하나로 뭉뚱그릴 수 없다. */
    val batchSizes: Map<String, Int>? = null,
    /** 조회한 모델들. */
    val models: List<String>? = null,
    /**
     * 응답하지 못한 모델과 그 이유. 한 풀이 죽어도 나머지 보이스는 그대로 돌려주되,
     * 목록이 불완전하다는 사실은 숨기지 않는다.
     */
    val errors: Map<String, String>? = null,
)
