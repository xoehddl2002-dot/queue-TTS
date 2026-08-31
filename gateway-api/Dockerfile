# syntax=docker/dockerfile:1

# ---- Build stage ----
FROM eclipse-temurin:21-jdk AS build

# 빌드 시점에 포함할 환경 프로파일 (resources-env/<IMAGE_PROFILE>/application.yml 선택)
# Jenkinsfile 의 IMAGE_PROFILE 을 --build-arg 로 전달, 없으면 dev
ARG PROFILE=dev

WORKDIR /app

# Gradle 래퍼와 빌드 스크립트를 먼저 복사해 의존성 캐시를 활용한다.
COPY gradlew gradlew.bat settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle ./gradle

RUN chmod +x gradlew && ./gradlew --no-daemon dependencies > /dev/null 2>&1 || true

# 소스 복사 후 실행 가능한 bootJar 생성 (-Pprofile 로 환경 설정 선택, 테스트는 제외)
COPY src ./src
RUN ./gradlew --no-daemon clean bootJar -x test -Pprofile=${PROFILE}

# ---- Runtime stage ----
FROM eclipse-temurin:21-jre

ENV TZ=Asia/Seoul \
    SERVER_PORT=8080

WORKDIR /app

RUN apt-get update -o Acquire::Retries=3 \
    && apt-get install -y --no-install-recommends -o Acquire::Retries=3 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# bootJar 산출물 ( *-plain.jar 는 제외 )
COPY --from=build /app/build/libs/ai-tts-queuetts-gateway-api-0.0.1-SNAPSHOT.jar app.jar

# 작업 산출물 디렉터리 (application.yml 의 job.artifact-dir)
RUN mkdir -p /app/job_artifacts /app/logs

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
