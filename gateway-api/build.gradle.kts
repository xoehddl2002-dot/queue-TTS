plugins {
    id("org.springframework.boot") version "3.5.15"
    id("io.spring.dependency-management") version "1.1.7"
    kotlin("jvm") version "2.2.21"
    kotlin("plugin.spring") version "2.2.21"
}

group = "com.aitts.queuetts"
version = "0.0.1-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

val profile = if (project.hasProperty("profile"))
    project.property("profile").toString() else "local"

println("build profile : $profile")

sourceSets{
    main{
        java{
            srcDirs(
                listOf("src/main/kotlin")
            )
        }
        resources{
            setSrcDirs(
                listOf(
                    "src/main/resources",
                    "src/main/resources-env/$profile",
                )
            )
        }
    }
}

kotlin {
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict")
    }
}

configurations {
    compileOnly {
        extendsFrom(configurations.annotationProcessor.get())
    }
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-aop")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
    // API Key 인증(SecurityConfig)·CORS 화이트리스트·기본 보안 헤더.
    implementation("org.springframework.boot:spring-boot-starter-security")
    // Swagger UI(/swagger-ui.html) + OpenAPI 3 문서(/v3/api-docs)를 자동 생성한다.
    implementation("org.springdoc:springdoc-openapi-starter-webmvc-ui:2.8.9")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    // 서비스 계층은 예외 대신 Either<GatewayError, T> 로 실패를 반환한다.
    implementation("io.arrow-kt:arrow-core:2.1.2")
    // speaker 등록은 worker/GPU와 무관하게 오디오 형식·길이를 검증해야 한다.
    implementation("net.jthink:jaudiotagger:3.0.1")
    implementation("org.springframework.data:spring-data-relational")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.postgresql:postgresql")
    // 스키마 마이그레이션. 적용 이력이 flyway_schema_history 에 남으므로 "적용하고 파일을 지웠다"가
    // 구조적으로 불가능해진다. Flyway 10 부터 DB 별 지원이 별도 모듈로 빠져 postgresql 을 함께 받는다.
    implementation("org.flywaydb:flyway-core")
    runtimeOnly("org.flywaydb:flyway-database-postgresql")

    developmentOnly("org.springframework.boot:spring-boot-devtools")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.security:spring-security-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test> {
    useJUnitPlatform()
}

// 게이트웨이 잡 큐(스케줄러)에 잡을 쌓아두고 큐/실행순서/동시실행을 검증하는 부하 테스트 러너.
// 환경(local/dev)을 번갈아 가며 실행할 수 있다:
//   ./gradlew loadTestJobs -Penv=local
//   ./gradlew loadTestJobs -Penv=dev --args="--count 50 --submit-concurrency 4 --strict-order"
// base URL 은 변수로 바꿀 수 있다: -PlocalUrl=... / -PdevUrl=... 또는 환경변수
//   QUEUETTS_GATEWAY_LOCAL_URL / QUEUETTS_GATEWAY_DEV_URL (둘 다 기본 http://127.0.0.1:8080)
tasks.register<JavaExec>("loadTestJobs") {
    group = "verification"
    description = "게이트웨이 잡 큐에 잡을 다량 제출해 큐 동작/실행 순서/동시 실행 수를 검증한다."
    mainClass.set("com.aitts.queuetts.gateway.api.loadtest.LoadTestJobsKt")
    classpath = sourceSets["test"].runtimeClasspath
    standardInput = System.`in`

    // -Penv 로 기본 환경을 정한다. 환경변수로 전달하므로 사용자의 --args 와 충돌하지 않는다.
    // (Gradle 의 --args 는 빌드 스크립트의 args 를 덮어쓰기 때문에 env 를 args 로 넘기지 않는다.)
    // --args 안에서 --env 를 주면 그쪽이 우선한다.
    val env = (project.findProperty("env") as String?) ?: "local"
    environment("QUEUETTS_GATEWAY_ENV", env)

    // base URL 을 Gradle 속성으로도 덮어쓸 수 있도록 러너가 읽는 환경변수로 전달한다.
    (project.findProperty("localUrl") as String?)?.let { environment("QUEUETTS_GATEWAY_LOCAL_URL", it) }
    (project.findProperty("devUrl") as String?)?.let { environment("QUEUETTS_GATEWAY_DEV_URL", it) }
}
