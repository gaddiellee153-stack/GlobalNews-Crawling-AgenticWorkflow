#!/usr/bin/env python3
"""
파이프라인 수리 스크립트
- 좀비 프로세스 제거
- SQLite WAL 정리
- DB 무결성 확인
- 분석 파이프라인 재개 준비
"""
import os
import signal
import sqlite3
import subprocess
import sys
import time

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "dedup.sqlite")
WAL_PATH = DB_PATH + "-wal"
SHM_PATH = DB_PATH + "-shm"


def print_step(n, msg):
    print(f"\n{'='*60}")
    print(f"  Step {n}: {msg}")
    print(f"{'='*60}")


def find_stale_processes():
    """어제 이전 날짜로 실행 중인 좀비 main.py 및 중복 프로세스 찾기"""
    result = subprocess.run(
        ["ps", "aux"], capture_output=True, text=True
    )
    stale = []
    current_date_pids = []
    for line in result.stdout.splitlines():
        if "main.py" not in line or "grep" in line or "repair" in line:
            continue
        parts = line.split()
        pid = int(parts[1])
        # 어제 이전 날짜 프로세스 = 좀비
        if "2026-03-21" in line or "2026-03-20" in line or "2026-03-19" in line:
            stale.append((pid, line.strip()))
        elif "2026-03-22" in line:
            current_date_pids.append((pid, line.strip()))

    # 같은 날짜에 2개 이상 프로세스 → 가장 오래된 것 제외하고 좀비 처리
    if len(current_date_pids) > 1:
        print(f"  ⚠ 같은 날짜 프로세스 {len(current_date_pids)}개 감지 — 중복 제거")
        # CPU 시간 기준 가장 활성인 것만 남기고 나머지 제거
        # PID가 큰 것이 최신 → 나머지 제거
        current_date_pids.sort(key=lambda x: x[0])
        for pid, desc in current_date_pids[:-1]:
            stale.append((pid, f"[중복] {desc}"))

    return stale


def kill_stale(pids):
    """좀비 프로세스 종료"""
    for pid, desc in pids:
        print(f"  Killing PID {pid}: {desc[:80]}...")
        try:
            os.kill(pid, signal.SIGTERM)
            time.sleep(2)
            # SIGTERM이 안 먹히면 SIGKILL
            try:
                os.kill(pid, 0)  # 프로세스 존재 확인
                print(f"  SIGTERM 무응답, SIGKILL 전송...")
                os.kill(pid, signal.SIGKILL)
                time.sleep(1)
            except ProcessLookupError:
                pass
            print(f"  ✓ PID {pid} 종료됨")
        except ProcessLookupError:
            print(f"  ✓ PID {pid} 이미 종료됨")
        except PermissionError:
            print(f"  ✗ PID {pid} 권한 부족")


def repair_database():
    """WAL 정리 + 무결성 확인"""
    wal_size = os.path.getsize(WAL_PATH) if os.path.exists(WAL_PATH) else 0
    print(f"  WAL 크기 (수리 전): {wal_size:,} bytes")

    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()

    # 무결성 확인
    cur.execute("PRAGMA integrity_check")
    integrity = cur.fetchone()[0]
    print(f"  무결성 검사: {integrity}")

    if integrity != "ok":
        print("  ✗ 데이터베이스 손상 감지! 백업 후 재생성 필요")
        conn.close()
        return False

    # WAL checkpoint (TRUNCATE = WAL 완전 비우기)
    cur.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    result = cur.fetchone()
    print(f"  WAL checkpoint: busy={result[0]}, log={result[1]}, checkpointed={result[2]}")

    if result[0] == 0:
        print("  ✓ WAL 정리 완료")
    else:
        print("  ⚠ WAL busy — 잠시 후 재시도...")
        time.sleep(3)
        cur.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        result = cur.fetchone()
        print(f"  재시도 결과: busy={result[0]}, log={result[1]}, checkpointed={result[2]}")

    # VACUUM으로 DB 최적화
    print("  VACUUM 실행 중...")
    cur.execute("VACUUM")
    print("  ✓ VACUUM 완료")

    conn.close()

    wal_size_after = os.path.getsize(WAL_PATH) if os.path.exists(WAL_PATH) else 0
    print(f"  WAL 크기 (수리 후): {wal_size_after:,} bytes")
    return True


def verify_readiness():
    """분석 파이프라인 실행 준비 확인"""
    # DB 접근 테스트
    conn = sqlite3.connect(DB_PATH, timeout=5)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM sqlite_master")
    tables = cur.fetchone()[0]
    conn.close()
    print(f"  DB 테이블 수: {tables}")
    print(f"  DB 접근: ✓ 정상")

    # 오늘 크롤링 데이터 확인
    raw_dir = os.path.join(os.path.dirname(DB_PATH), "raw", "2026-03-22")
    jsonl = os.path.join(raw_dir, "all_articles.jsonl")
    if os.path.exists(jsonl):
        with open(jsonl) as f:
            count = sum(1 for _ in f)
        size_mb = os.path.getsize(jsonl) / 1024 / 1024
        print(f"  크롤링 데이터: {count:,}개 기사 ({size_mb:.1f}MB)")
    else:
        print(f"  ⚠ 크롤링 데이터 없음: {jsonl}")

    return True


def main():
    print("\n" + "🔧" * 30)
    print("  글로벌 뉴스 크롤링 파이프라인 수리")
    print("🔧" * 30)

    # Step 1: 좀비 프로세스 탐지
    print_step(1, "좀비 프로세스 탐지")
    stale = find_stale_processes()
    if stale:
        print(f"  발견: {len(stale)}개 좀비 프로세스")
        for pid, desc in stale:
            print(f"  - PID {pid}: {desc[:100]}")
    else:
        print("  ✓ 좀비 프로세스 없음")

    # Step 2: 좀비 제거
    if stale:
        print_step(2, "좀비 프로세스 제거")
        kill_stale(stale)
        time.sleep(3)  # DB lock 해제 대기
    else:
        print_step(2, "좀비 프로세스 제거 (건너뜀)")

    # Step 3: DB 수리
    print_step(3, "SQLite 데이터베이스 수리")
    success = repair_database()
    if not success:
        print("  ✗ DB 수리 실패. 수동 개입 필요.")
        sys.exit(1)

    # Step 4: 준비 상태 확인
    print_step(4, "분석 파이프라인 준비 확인")
    verify_readiness()

    print("\n" + "=" * 60)
    print("  ✅ 수리 완료 — 분석 파이프라인 실행 준비됨")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
