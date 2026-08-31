-- Gateway 스키마의 기준선(baseline).
--
-- 이미 이 스키마가 올라가 있는 DB(dev/prod)는 `baseline-on-migrate` 로 V1 을 "적용된 것"으로
-- 표시하고 건너뛴다. 빈 DB 만 이 파일을 실제로 실행한다.
--
-- **이 파일은 다시 편집하지 않는다.** 스키마를 바꿀 때는 V2, V3... 를 새로 추가한다.
-- 이미 적용된 마이그레이션을 고치면 checksum 이 어긋나 다음 기동이 실패한다.

CREATE TABLE public.tts_job_generation_history (
                                                   job_id text NOT NULL,
                                                   state text NOT NULL,
                                                   priority text DEFAULT 'normal'::text NOT NULL,
                                                   -- job 을 접수시킨 API Key 호출자. 처리한 워커(worker_id)가 아니라 요청한 쪽이다.
                                                   -- 인증이 꺼진 환경(queuetts.security.enabled=false)에서 만들어진 job 은 NULL.
                                                   caller_id text NULL,
                                                   -- 그 API Key 의 권한. state/priority 와 같은 소문자 표기로 저장한다 ('admin' / 'client').
                                                   caller_role text NULL,
                                                   worker_id text NULL,
                                                   batch_id text NULL,
                                                   payload jsonb DEFAULT '{}'::jsonb NOT NULL,
                                                   "result" jsonb NULL,
                                                   "error" jsonb NULL,
                                                   "text" text NULL,
                                                   voice text NULL,
                                                   lang text NULL,
                                                   speed float8 NULL,
                                                   steps int4 NULL,
                                                   response_format text NULL,
                                                   seed int4 NULL,
                                                   max_chunk_length int4 NULL,
                                                   silence_duration float8 NULL,
                                                   artifact_path text NULL,
                                                   artifact_name text NULL,
                                                   artifact_media_type text NULL,
                                                   artifact_size int8 NULL,
                                                   download_url text NULL,
                                                   created_at timestamptz NOT NULL,
                                                   started_at timestamptz NULL,
                                                   finished_at timestamptz NULL,
                                                   CONSTRAINT tts_job_generation_history_pkey PRIMARY KEY (job_id)
);
CREATE INDEX idx_tts_job_generation_history_created_at ON public.tts_job_generation_history USING btree (created_at DESC);
CREATE INDEX idx_tts_job_generation_history_state_created_at ON public.tts_job_generation_history USING btree (state, created_at DESC);


-- 클론 보이스(speaker) 레지스트리. 테이블 이름은 tts_style 그대로 둔다 (rename 이 아니라 마이그레이션). 원본은 Gateway 가 소유하고 worker 는 파생 캐시다.
-- 자세한 설계는 docs/speakers-registry.md 참고.
CREATE TABLE public.tts_style (
                                  "name" text NOT NULL,
                                  model text NOT NULL,
                                  "language" text NULL,
                                  description text NULL,
                                  "mode" text DEFAULT 'icl'::text NOT NULL,
                                  ref_text text NULL,
                                  audio bytea NOT NULL,
                                  audio_sha256 text NOT NULL,
                                  -- sha256(audio_sha256 + mode + ref_text). worker prompt 캐시의 무효화 키다.
                                  -- 오디오만 보면 ref_text 오타 수정 같은 변경을 놓친다.
                                  reference_digest text NOT NULL,
                                  audio_format text NOT NULL,
                                  sample_rate int4 NULL,
                                  duration_s float8 NULL,
                                  default_params jsonb DEFAULT '{}'::jsonb NOT NULL,
                                  created_by text NULL,
                                  created_at timestamptz DEFAULT now() NOT NULL,
                                  updated_at timestamptz DEFAULT now() NOT NULL,
                                  -- 참조(오디오/ref_text/mode)가 마지막으로 바뀐 시각. 버전 이력을 두지 않으므로
                                  -- "언제부터 목소리가 달라졌나" 를 되짚을 근거가 이것뿐이다.
                                  reference_updated_at timestamptz DEFAULT now() NOT NULL,
                                  -- 공개 보이스 키가 곧 기본키다. name 은 풀 안에서만 유일하므로 model 과 묶는다.
                                  CONSTRAINT tts_style_pkey PRIMARY KEY (model, "name"),
                                  CONSTRAINT tts_style_mode_check CHECK ("mode" IN ('icl', 'x_vector')),
                                  -- icl 은 참조 텍스트를 조건으로 쓴다. 없으면 worker 가 거절하므로 DB 에서 막는다.
                                  CONSTRAINT tts_style_ref_text_check CHECK ("mode" <> 'icl' OR ref_text IS NOT NULL)
);
-- 목록 조회는 풀 단위로 최신순 정렬만 한다. 예전에는 status 로 archived 를 걸러 내느라 그 컬럼이
-- 인덱스 앞에 있었다.
CREATE INDEX idx_tts_style_model_created_at ON public.tts_style USING btree (model, created_at DESC);


CREATE TABLE public.tts_sample_comparison (
                                              sample_key text NOT NULL,
                                              "text" text NOT NULL,
                                              legacy_service text DEFAULT 'typecast'::text NOT NULL,
                                              legacy_audio_path text NOT NULL,
                                              legacy_audio_format text DEFAULT 'mp3'::text NOT NULL,
                                              notes text NULL,
                                              created_at timestamptz DEFAULT now() NOT NULL,
                                              updated_at timestamptz DEFAULT now() NOT NULL,
                                              current_service text DEFAULT 'queuetts'::text NOT NULL,
                                              current_model text NULL,
                                              current_voice text NULL,
                                              current_lang text NULL,
                                              current_speed float8 NULL,
                                              current_steps int4 NULL,
                                              current_response_format text NULL,
                                              current_seed int8 NULL,
                                              current_section_size text NULL,
                                              current_max_chunk_length int4 NULL,
                                              current_silence_duration float8 NULL,
                                              current_audio_path text NULL,
                                              current_audio_format text NULL,
                                              current_duration_s float8 NULL,
                                              current_sample_rate int4 NULL,
                                              current_result_info text NULL,
                                              current_generated_at timestamptz NULL,
                                              CONSTRAINT tts_sample_comparison_pkey PRIMARY KEY (sample_key)
);
CREATE INDEX tts_sample_comparison_updated_at_idx ON public.tts_sample_comparison USING btree (updated_at DESC);
