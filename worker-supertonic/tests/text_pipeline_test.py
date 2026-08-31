"""합성 전 텍스트 처리와 파라미터 정리 테스트.

모델도 ONNX 도 필요 없다 — `backend` 는 ONNX 를 함수 안에서 지연 로드하므로 import 만으로는
런타임을 건드리지 않는다. 여기서 보는 것은 **읽히는 문장이 무엇이 되는가**와
**잘못된 파라미터가 어떻게 정리되는가** 뿐이다.

숫자 읽기는 조용히 틀리는 종류의 버그다 — 합성은 성공하고 소리만 이상해지므로 테스트가 아니면
회귀를 알아채기 어렵다.
"""
from __future__ import annotations

from _harness import check, equals, raises, report

import backend as b


def test_sino_numbers() -> None:
    print("[1] 한자어 수 읽기")
    equals("0", b._read_sino_under_10000(0), "영")
    equals("10 은 '일십' 이 아니라 '십'", b._read_sino_under_10000(10), "십")
    equals("11", b._read_sino_under_10000(11), "십일")
    equals("100 은 '일백' 이 아니라 '백'", b._read_sino_under_10000(100), "백")
    equals("110", b._read_sino_under_10000(110), "백십")
    equals("1234", b._read_sino_under_10000(1234), "천이백삼십사")
    equals("9999", b._read_sino_under_10000(9999), "구천구백구십구")

    # 만 이상은 일부러 그대로 둔다 — 뒤에 오는 text_processing/g2pk 가 '일만오천' 처럼
    # 더 자연스럽게 읽는다. 여기서 읽어 버리면 '만오천' 이 되어 오히려 나빠진다.
    equals("9999 까지만 읽는다", b._read_sino_integer(9999), "구천구백구십구")
    equals("10000 은 숫자로 남긴다", b._read_sino_integer(10000), "10000")


def test_hour_and_month_readings() -> None:
    print("\n[2] 시각 · 월 고유 읽기")
    # 시각은 고유어다. '삼 시' 가 아니라 '세 시'.
    equals("1시", b._read_hour(1), "한")
    equals("3시", b._read_hour(3), "세")
    equals("12시", b._read_hour(12), "열두")
    equals("13시는 한자어로", b._read_hour(13), "십삼")

    # 6월/10월은 표기가 바뀐다. 여기가 틀리면 '육월', '십월' 로 읽힌다.
    equals("6월 → 유월", b._read_month(6), "유")
    equals("10월 → 시월", b._read_month(10), "시")
    equals("12월", b._read_month(12), "십이")


def test_date_and_time_normalization() -> None:
    print("\n[3] 날짜 · 시각 정규화")
    equals("ISO 날짜", b.normalize_time_date_numbers_for_tts("2026-08-27"), "이천이십육년 팔월 이십칠일")
    equals("시:분", b.normalize_time_date_numbers_for_tts("3시 30분"), "세 시 삼십 분")
    equals("반", b.normalize_time_date_numbers_for_tts("10시 반"), "열 시 반")
    equals("월-일", b.normalize_time_date_numbers_for_tts("6월 10일"), "유월 십일")


def test_phone_numbers() -> None:
    print("\n[4] 전화번호 자릿수 읽기")
    # 전화번호는 수가 아니라 자릿수로 읽어야 한다. '공일공' 이지 '십' 이 아니다.
    equals("휴대폰", b.read_phone_numbers_as_digits("01012345678"), "공일공 일이삼사 오육칠팔")
    equals("서울 국번", b.read_phone_numbers_as_digits("0212345678"), "공이 일이삼사 오육칠팔")
    equals("대표번호", b.read_phone_numbers_as_digits("15881588"), "일오팔팔 일오팔팔")

    equals("02 는 2자리로 분리", b._split_phone_number_digits("0212345678"), ["02", "1234", "5678"])
    equals("02 + 7자리", b._split_phone_number_digits("021234567"), ["02", "123", "4567"])
    equals("15xx 는 4+4", b._split_phone_number_digits("15881588"), ["1588", "1588"])

    equals("하이픈 전화번호", b.read_remaining_hyphenated_numbers_as_phone_digits("010-1234-5678"),
           "공일공 일이삼사 오육칠팔")
    # 날짜를 전화번호로 오인하면 "이공이육 공팔 이칠" 이 된다. 반드시 걸러야 한다.
    check("날짜는 전화번호가 아니다", not b._is_hyphenated_phone_token("2026-08-27"))


def test_terminal_punctuation() -> None:
    print("\n[5] 종결 부호 보정")
    # 종결 부호가 없으면 모델이 문장 끝을 흘려 읽는다.
    equals("없으면 마침표 추가", b.ensure_terminal_punctuation("안녕하세요"), "안녕하세요.")
    equals("있으면 그대로", b.ensure_terminal_punctuation("안녕하세요."), "안녕하세요.")
    equals("물음표도 종결", b.ensure_terminal_punctuation("정말?  "), "정말?")
    equals("빈 문자열", b.ensure_terminal_punctuation("   "), "")


def test_chunking() -> None:
    print("\n[6] 청크 분할")
    # chunk_text_fn 은 주입 인자다. 엔진 청커 대신 '자르지 않는' 가짜를 넣어 보정 로직만 본다.
    whole = lambda text, limit: [text]  # noqa: E731

    equals("문장 분리", b._split_sentences_for_tts("안녕하세요. 반갑습니다! 잘 지내죠? 네"),
           ["안녕하세요.", "반갑습니다!", "잘 지내죠?", "네"])
    equals("상한 이내는 그대로", b._split_oversized_chunk("짧은 문장", 200), ["짧은 문장"])

    # 상한을 넘으면 반드시 쪼갠다. 엔진 청커가 놓쳐도 여기서 잡는 마지막 그물이다.
    lengths = [len(chunk) for chunk in b._split_oversized_chunk("가" * 250, 100)]
    equals("250자를 100 상한으로", lengths, [100, 100, 50])
    check("모든 조각이 상한 이하", all(length <= 100 for length in lengths))

    # 끊을 곳이 있으면 구두점/공백에서 끊는다. 단어 중간에서 자르면 발음이 뭉개진다.
    first = b._split_oversized_chunk("가" * 60 + ", " + "나" * 60, 100)[0]
    equals("구두점에서 끊는다", first[-1], ",")

    equals("빈 청크는 버린다", b.chunk_text_for_tts("  ", 200, whole), [])
    equals("기본은 주입된 청커", b.chunk_text_for_tts("안녕하세요. 반갑습니다.", 200, whole),
           ["안녕하세요. 반갑습니다."])
    # 느린 한국어에서는 문장 단위로 끊어야 호흡이 자연스럽다.
    equals("문장 유지 모드", b.chunk_text_for_tts("안녕하세요. 반갑습니다.", 200, whole, keep_sentence_chunks=True),
           ["안녕하세요.", "반갑습니다."])

    check("느린 한국어만 문장 유지", b.prefer_sentence_chunks("ko", 0.9))
    check("보통 속도는 아니다", not b.prefer_sentence_chunks("ko", 1.05))
    check("영어는 아니다", not b.prefer_sentence_chunks("en", 0.8))


def test_param_cleaning() -> None:
    print("\n[7] 파라미터 정리")
    # 큐로 직접 XADD 하면 Gateway 검증을 우회하므로, worker 도 자기 입력을 정리해야 한다.
    equals("speed 기본값", b.clean_speed(None), b.SPEED_DEFAULT)
    equals("speed 하한 clamp", b.clean_speed(0.1), b.SPEED_MIN)
    equals("speed 상한 clamp", b.clean_speed(9.9), b.SPEED_MAX)
    equals("speed 문자열은 기본값", b.clean_speed("bad"), b.SPEED_DEFAULT)
    equals("speed NaN 은 기본값", b.clean_speed(float("nan")), b.SPEED_DEFAULT)

    equals("steps 기본값", b.clean_steps(None), b.STEPS_DEFAULT)
    equals("steps 하한 clamp", b.clean_steps(1), b.STEPS_MIN)
    equals("steps 상한 clamp", b.clean_steps(99), b.STEPS_MAX)

    equals("seed 없음", b.clean_seed(None), None)
    equals("seed -1 은 무작위", b.clean_seed(-1), None)
    equals("seed 값", b.clean_seed(7), 7)
    raises("seed 상한 초과는 오류", b.QueueTtsError, b.clean_seed, b.MAX_SEED + 1)

    equals("lang 없음", b.clean_lang(None), None)
    equals("lang auto 는 None", b.clean_lang("auto"), None)
    equals("lang 값", b.clean_lang("ko"), "ko")

    equals("format 기본값", b.clean_format(""), "wav")
    equals("format 대문자 허용", b.clean_format("WAV"), "wav")
    raises("모르는 format 은 오류", b.QueueTtsError, b.clean_format, "mp3")

    equals("max_chunk_length 기본값", b.clean_max_chunk_length(None), b.DEFAULT_MAX_CHUNK_LENGTH)
    equals("0 은 기본값", b.clean_max_chunk_length(0), b.DEFAULT_MAX_CHUNK_LENGTH)
    equals("정상값", b.clean_max_chunk_length(50), 50)
    raises("너무 작으면 오류", b.QueueTtsError, b.clean_max_chunk_length, b.MIN_CHUNK_LENGTH - 1)


def test_pipeline_end_to_end() -> None:
    print("\n[8] 파이프라인 종단")
    equals("전화번호", b.prepare_text_for_tts("문의는 0212345678 로 주세요"),
           "문의는 (공이 일이삼사 오육칠팔) 로 주세요.")
    equals("날짜", b.prepare_text_for_tts("2026-08-27 에 만나요"), "이천이십육년 팔월 이십칠일 에 만나요.")
    equals("시각", b.prepare_text_for_tts("3시 30분에 시작합니다"), "세시 삼십분에 시작합니다.")
    equals("종결 부호 보정", b.prepare_text_for_tts("안녕하세요"), "안녕하세요.")
    # 만 이상은 여기서 읽힌다 — _read_sino_integer 가 넘긴 몫을 뒷단이 받는다.
    equals("만 단위", b.prepare_text_for_tts("가격은 15000원입니다"), "가격은 일만오천원입니다.")


def main() -> None:
    test_sino_numbers()
    test_hour_and_month_readings()
    test_date_and_time_normalization()
    test_phone_numbers()
    test_terminal_punctuation()
    test_chunking()
    test_param_cleaning()
    test_pipeline_end_to_end()
    report()


if __name__ == "__main__":
    main()
