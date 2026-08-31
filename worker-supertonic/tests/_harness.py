"""테스트 공용 도구.

Qwen 워커의 테스트와 같은 방식이다 — pytest 를 쓰지 않고 스크립트로 직접 실행한다
(`python tests/<name>_test.py`). 워커 이미지에 테스트 전용 의존성을 넣지 않기 위해서다.
"""
from __future__ import annotations

import sys
from pathlib import Path

# 테스트를 어디서 실행하든 워커 모듈을 import 할 수 있게 한다.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

FAILURES: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    print(f"  {'PASS' if condition else 'FAIL'} {label}{': ' + detail if detail else ''}")
    if not condition:
        FAILURES.append(label)


def equals(label: str, actual, expected) -> None:
    """같지 않으면 기대/실제를 그대로 남긴다 — 실패 메시지만 보고 원인을 알 수 있게."""
    ok = actual == expected
    check(label, ok, "" if ok else f"expected {expected!r}, got {actual!r}")


def raises(label: str, exc_type, fn, *args, **kwargs) -> None:
    try:
        fn(*args, **kwargs)
    except exc_type:
        check(label, True)
        return
    except Exception as exc:  # noqa: BLE001
        check(label, False, f"expected {exc_type.__name__}, got {type(exc).__name__}: {exc}")
        return
    check(label, False, f"expected {exc_type.__name__}, but nothing was raised")


def report() -> None:
    print()
    if FAILURES:
        print(f"RESULT: {len(FAILURES)} FAILED -> {FAILURES}")
        raise SystemExit(1)
    print("RESULT: ALL PASSED")
