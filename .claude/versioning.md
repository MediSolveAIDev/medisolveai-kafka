# 버전 관리 규칙

## 형식: vX.Y.Z

| 위치 | 변경 조건 | 커밋 prefix | 예시 |
|------|----------|------------|------|
| X (major) | 이벤트 스키마 breaking change, 라이브러리 구조 변경 | `version:` | `version: BaseEvent 필드 타입 변경` |
| Y (minor) | 서비스(도메인) 추가 | `feat:` | `feat: say 서비스 이벤트 추가` |
| Z (patch) | 버그픽스, 기능(메서드) 추가, 기타 업데이트 | `fix:` | `fix: Producer 재시도 로직 수정` |

## 버전 변경 없는 커밋

| prefix | 용도 | 예시 |
|--------|------|------|
| `docs:` | 문서 수정 | `docs: README 사용법 업데이트` |
| `test:` | 테스트 추가/수정 | `test: Consumer DLQ 테스트 추가` |
| `chore:` | 설정, CI 변경 | `chore: GitHub Actions 수정` |
| `refactor:` | 리팩토링 (동작 변경 없음) | `refactor: Producer 내부 구조 정리` |

## 버전 계산 규칙

- 마지막 릴리즈 태그 이후 커밋을 스캔
- `version:` 이 하나라도 있으면 → major 업 (X+1.0.0)
- `feat:` 이 하나라도 있으면 → minor 업 (X.Y+1.0)
- `fix:` 만 있으면 → patch 업 (X.Y.Z+1)
- 버전 변경 prefix가 없으면 → 릴리즈 불가 (CI가 중단)
- 우선순위: version > feat > fix

## 릴리즈 프로세스

1. 개발자가 prefix 규칙에 따라 커밋
2. main에 merge
3. GitHub Actions 탭에서 "Release" 워크플로우 수동 실행
4. CI가 자동으로:
   - 커밋 prefix 스캔 → 버전 계산
   - pytest 실행 → 실패 시 중단
   - pyproject.toml 버전 업데이트
   - git tag + GitHub Release 생성
   - PyPI 배포
