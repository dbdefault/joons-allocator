# JOONS Allocator 0.1.0

English: [README.md](README.md)

AI 에이전트를 위한 가벼운 작업 할당 및 작업자 조정 도구입니다.

> [JOONS 생태계](https://github.com/dbdefault/joons-ecosystem)(MCP 기반 AI 인프라 생태계)의 일부입니다.

![Preview](https://img.shields.io/badge/status-Preview-orange) [![CI](https://github.com/dbdefault/joons-allocator/actions/workflows/ci.yml/badge.svg)](https://github.com/dbdefault/joons-allocator/actions/workflows/ci.yml)

## 1. 소개

JOONS Allocator는 하나의 SQLite 파일을 사용하는 단일 컴퓨터용 작업 할당기입니다. `Allocator` 퍼사드(facade)는 작업 저장, 접수, 배정 및 운영자 제어를 결합합니다. 핵심 기능은 Python 표준 라이브러리만 사용합니다. 이 버전은 미리 보기 버전이며 API가 변경될 수 있습니다.

## 2. 해결하는 문제

에이전트에는 작업을 지속적으로 보관하고, 사용 가능한 작업자에게 할당하며, 이름이 지정된 자원의 중복 사용을 막고, 운영자의 결정을 기록하는 방법이 필요합니다. 이 할당기는 서버 없이 로컬에서 이러한 작업을 조정합니다.

## 3. 기능

- 하나의 SQLite 데이터베이스에서 제출과 임대(lease)를 트랜잭션(transaction)으로 처리합니다. 운영자 결정의 감사 기록을 남깁니다.
- 우선순위에 따라 정렬하며, 우선순위가 같으면 먼저 생성된 작업부터 정렬합니다.
- 작업자 한 명당 실행 중인 작업을 하나로 제한합니다. 실행 중인 작업은 이름이 지정된 자원을 독점적으로 사용합니다.
- 주체(principal)와 요청 ID를 기준으로 멱등성(idempotency)을 보장하는 제출을 지원합니다.
- 승인, 취소, 일시 중지, 재개, 재시도 및 명시적 복구를 지원합니다.
- 임대 시 작업자를 등록하고, 하트비트(heartbeat), 일시 중지 및 재개를 지원합니다.
- 명령줄 인터페이스(command-line interface)와 asyncio 퍼사드를 제공합니다.
- 선택적으로 JOONS Agent Guard 훅(hook)을 사용합니다. 핵심 기능에는 Guard가 필요하지 않습니다.

### 0.1.0에 포함되지 않는 기능

여러 컴퓨터 또는 네트워크 간 조정, 작업 시간 초과에 따른 자동 만료, 예약 또는 지연 작업, 작업 간 의존성, 작업자 한 명의 동시 실행 용량을 하나보다 크게 설정하는 기능, 웹 UI는 계획 항목이며 이번 버전의 범위에 포함되지 않습니다.

## 4. 아키텍처

`Allocator`는 데이터베이스 하나를 소유하고 `TaskStore`, `Admission`, `Dispatcher`, `Control`을 조합합니다. 자원 가용성은 임대 트랜잭션 안에서 확인합니다. 복구와 그 감사 기록은 같은 트랜잭션에서 커밋(commit)합니다. 자세한 내용은 [아키텍처](ARCHITECTURE.md)를 참조합니다.

## 5. 설치

소스 체크아웃(checkout)에서 설치합니다.

```sh
pip install .
```

Git 저장소에서 설치합니다.

```sh
pip install git+https://github.com/dbdefault/joons-allocator
```

선택 사항인 Guard 연동을 사용하려면 `guard` 추가 의존성을 설치합니다.

```sh
pip install ".[guard]"
```

## 6. 빠른 시작

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator

with TemporaryDirectory() as directory:
    with Allocator(Path(directory) / "tasks.sqlite") as allocator:
        submitted = allocator.submit({"operation": "add", "values": [1, 2]})
        task = allocator.lease("worker-1")
        if task is not None:
            allocator.complete("worker-1", task, '3')
            print(allocator.get(submitted.task_id))
```

## 7. 작업 제출과 할당

`submit`은 JSON으로 직렬화할 수 있는 딕셔너리(dict) 페이로드(payload)를 필수로 받습니다. 선택 인수로 `priority`, `resource`, `principal`, `request_id`, `require_approval`을 받습니다. 우선순위가 높은 작업을 먼저 임대합니다. 우선순위가 같으면 생성 시간순으로 정렬합니다. 실행 중인 작업이 자원을 보유하면 해당 자원이 필요한 작업은 건너뜁니다.

요청 ID를 사용하면 새 요청의 제출 결과는 `accepted`입니다. 주체, 요청 ID, 페이로드, 우선순위, 자원 및 승인 요구 사항이 같으면 원래 작업 ID와 함께 `duplicate`를 반환합니다. 같은 키에서 페이로드 또는 옵션이 다르면 `conflict`를 반환합니다. 이 다이제스트(digest) 변경 전에 생성된 기존 원장에는 새 요청 ID가 필요합니다. 승인이 필요한 작업은 운영자가 승인할 때까지 대기합니다.

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator

with TemporaryDirectory() as directory:
    with Allocator(Path(directory) / "tasks.sqlite") as allocator:
        submission = allocator.submit(
            {"operation": "render"}, priority=10, resource="renderer",
            principal="service", request_id="request-1", require_approval=True,
        )
        allocator.approve(submission.task_id, "operator")
        task = allocator.lease("worker-1")
        if task is not None:
            allocator.complete("worker-1", task, 'done')
```

## 8. 작업자

`lease(worker_id)`는 새 작업자를 자동으로 등록하고 작업 또는 `None`을 반환합니다. 일시 중지된 작업자나 이미 작업을 실행 중인 작업자는 다른 작업을 임대할 수 없습니다. `heartbeat`, `workers`, `pause_worker`, `resume_worker`로 작업자를 관리합니다. 결과는 텍스트 문자열입니다. 오래된 실행을 차단하려면 임대한 `Task`를 `complete` 또는 `fail`에 전달합니다. 또는 작업 ID와 함께 `lease_token=task.lease_token`을 전달합니다. ID만 전달하는 호출도 호환되지만 오래된 임대는 구별할 수 없습니다. 복구할 때 새 임대를 보호하려면 `lease_token=task.lease_token`을 전달합니다. CLI에서는 `--lease-token`을 사용합니다. 운영자는 실패한 작업을 `retry`로 재시도하거나 예상 작업자를 지정해 `recover`로 명시적으로 복구할 수 있습니다. 운영자의 결정은 작업 이력에 기록됩니다.

## 9. CLI 사용법

CLI는 `python -m joons_allocator` 또는 `joons-allocator`로 실행할 수 있습니다. 명령 앞에 데이터베이스를 지정합니다. `--json`은 선택 사항입니다.

```sh
python -m joons_allocator --db tasks.sqlite submit --payload '{"action":"render"}' --priority 10 --resource renderer --request-id request-1
python -m joons_allocator --db tasks.sqlite --json queue
python -m joons_allocator --db tasks.sqlite list --state awaiting_approval
python -m joons_allocator --db tasks.sqlite approve task-1 --by operator --yes
python -m joons_allocator --db tasks.sqlite history task-1
python -m joons_allocator --db tasks.sqlite recover task-1 --worker worker-1 --lease-token TOKEN --by operator --yes
```

CLI는 제출을 포함한 모든 명령에 기존 데이터베이스가 필요합니다. 조회 명령은 데이터베이스를 읽기 전용으로 엽니다. 운영자가 상태를 변경하려면 `--yes`가 필요합니다. 종료 코드 `0`은 성공을 뜻합니다. `1`은 대상을 찾지 못했거나 변경된 내용이 없거나 결정이 거부되었음을 뜻합니다. `2`는 사용법이 잘못되었거나 확인이 필요함을 뜻합니다.

## 10. 지원 환경

Windows, macOS, Linux의 Python 3.9–3.13을 지원합니다. 로컬 디스크가 있는 컴퓨터 한 대에서 할당기마다 SQLite 파일 하나를 사용합니다.

## 11. 제한 사항

- 이 버전은 미리 보기 버전이며 핵심 기능을 완성하고 있습니다.
- 조정 범위는 컴퓨터 한 대로 제한됩니다. 서버나 분산 모드는 없습니다.
- 작업은 자동으로 만료되지 않습니다. 복구는 운영자가 명시적으로 수행합니다.
- 작업자마다 실행 중인 작업을 위한 슬롯이 하나 있습니다. 예약, 의존성 및 웹 UI는 이번 릴리스의 범위에 포함되지 않습니다.
- 결과는 UTF-8로 인코딩할 수 있는 텍스트 1,048,576자로 제한됩니다. 사유는 4096자로 제한됩니다. NaN과 Infinity는 허용되지 않습니다.
- 선택적 Guard 훅의 실패는 로그에 기록되며 할당기 상태를 손상해서는 안 됩니다. 오류가 발생하면 외부 Guard의 정리 작업을 확인해야 할 수 있습니다.

## 12. JOONS Allocator와 JOONS Agent Guard 비교

| | JOONS Allocator | JOONS Agent Guard |
|---|---|---|
| 목적 | 로컬 작업 할당 및 작업자 조정 | 회로 및 자원 잠금과 같은 Guard 기본 기능 |
| 핵심 의존성 | Python 표준 라이브러리 | 할당기에서 선택 사항 |
| 자원 | 자체 데이터베이스에서 같은 자원을 사용하는 작업의 동시 실행을 방지합니다. | 훅을 통해 별도의 자원 잠금을 제공할 수 있습니다. |

두 도구를 함께 사용하려면 `guard` 추가 의존성을 설치합니다. `joons_allocator.integrations.guard`의 `GuardHooks(circuit=..., locks=...)`를 `hooks=`를 통해 `Dispatcher`에 전달합니다. 훅은 작업 임대와 정리 과정에서 전달받은 Guard 회로와 잠금을 사용합니다. 위 퍼사드 예제에는 Guard가 필요하지 않습니다.

## 13. 라이선스

Apache-2.0입니다.

## 14. 기여

[기여 안내](CONTRIBUTING.md)를 참조합니다.

