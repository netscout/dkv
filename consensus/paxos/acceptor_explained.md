# Acceptor 코드 종합 해설

Paxos 합의 알고리즘에서 Acceptor 역할과 Go 코드를 함께 설명한다.

---

## 배경: Paxos란?

Paxos는 분산 합의 알고리즘이다. 일부 노드가 장애를 일으켜도 여러 서버(노드)가 하나의 값에 합의할 수 있게 해준다. 세 가지 역할이 있다:

1. **Proposer** — 값을 제안하고 수락받으려 한다
2. **Acceptor** — 제안에 투표한다 (이 파일!)
3. **Learner** — 결정된 값을 학습한다

프로토콜은 두 단계로 이루어진다:

- **Phase 1 (Prepare/Promise)**: Proposer가 Acceptor에게 "내 제안보다 낮은 제안은 무시해줄래?"라고 요청한다. Acceptor는 "약속할게" 또는 "이미 더 높은 번호에 약속했어"로 응답한다.
- **Phase 2 (Accept/Accepted)**: Proposer가 과반수의 약속을 받으면 실제 값을 보낸다. Acceptor는 그 사이에 더 높은 약속을 하지 않았다면 수락한다.

이 구현은 **Multi-Paxos**를 사용한다 — 단일 값이 아니라 여러 "슬롯"(커맨드 로그)에 대해 Paxos를 실행한다.

---

## 핵심 개념: Ballot

**Ballot**은 제안 번호다 — 버전/우선순위 스탬프라고 생각하면 된다. proto 정의:

```protobuf
message Ballot {
    uint64 number = 1;
    uint32 node_id = 2;
}
```

- `number` — 단조 증가하는 카운터. 높을수록 새로운 제안.
- `node_id` — 동점 처리용. 두 노드가 같은 번호로 제안하면 node_id가 높은 쪽이 이긴다.

`ballot.go`의 비교 헬퍼:

```go
func BallotGreaterThan(a, b *pb.Ballot) bool {
    if a.Number != b.Number {
        return a.Number > b.Number
    }
    return a.NodeId > b.NodeId
}
```

이것은 모든 노드의 모든 제안에 대해 **전순서(total ordering)**를 확립한다.

---

## 구조체 정의

### `Acceptor` 구조체

```go
type Acceptor struct {
    mu     sync.Mutex
    nodeID uint32
    wal    *wal.WAL
    log    *slog.Logger

    promisedBallot *pb.Ballot
    slots map[uint64]*AcceptorSlot
}
```

**Go 문법:**
- `type Acceptor struct { ... }` — 구조체를 정의한다 (다른 언어의 클래스와 비슷하지만 상속은 없다).
- `mu sync.Mutex` — 상호 배제 잠금. 여러 고루틴(경량 스레드)이 동시에 메서드를 호출할 수 있으므로 뮤텍스가 경쟁 조건을 방지한다. `Lock()` / `Unlock()`으로 임계 구역을 감싼다.
- `*wal.WAL` — `*`는 WAL(Write-Ahead Log)에 대한 **포인터**다. 포인터는 `nil`이 될 수 있어서 테스트에서 영속성을 건너뛸 때 사용한다.
- `*slog.Logger` — Go의 구조화된 로깅 라이브러리 (Go 1.21에서 추가).

**Paxos 의미:**
- `promisedBallot` — 이 Acceptor가 **약속한 가장 높은 ballot**. 이보다 낮은 제안은 거절한다. "글로벌"이라 함은 모든 슬롯에 적용된다는 뜻 (Multi-Paxos 최적화).
- `slots` — 슬롯 번호에서 슬롯별 상태로의 맵. 각 슬롯은 독립적인 Paxos 인스턴스 (복제 로그의 한 항목).

### `AcceptorSlot` 구조체

```go
type AcceptorSlot struct {
    AcceptedBallot *pb.Ballot
    AcceptedValue  *pb.Command
}
```

각 슬롯이 추적하는 것:
- `AcceptedBallot` — 이 Acceptor가 이 슬롯에서 **수락한** 마지막 제안의 ballot
- `AcceptedValue` — 수락된 실제 커맨드 (PUT/GET/DEL)

---

## 생성자: `NewAcceptor`

```go
func NewAcceptor(nodeID uint32, w *wal.WAL, logger *slog.Logger) *Acceptor {
    return &Acceptor{
        nodeID:         nodeID,
        wal:            w,
        log:            logger,
        promisedBallot: &pb.Ballot{},
        slots:          make(map[uint64]*AcceptorSlot),
    }
}
```

**Go 문법:**
- `func NewAcceptor(...) *Acceptor` — Go에는 생성자가 없다. 관례적으로 `NewXxx` 함수가 생성자 역할을 한다. 새 Acceptor에 대한 **포인터**를 반환한다.
- `&Acceptor{...}` — `&`는 주소를 취한다 (포인터 생성). `{...}`는 이름 있는 필드를 가진 구조체 리터럴.
- `make(map[uint64]*AcceptorSlot)` — `make`는 맵을 초기화한다. Go에서 맵은 사용 전에 반드시 초기화해야 한다 (아니면 `nil`이라 쓰기 시 패닉 발생).
- `&pb.Ballot{}` — 제로 값 Ballot 생성 (number=0, nodeId=0). "아직 약속 없음"을 의미 — 어떤 실제 ballot이든 이보다 크다.

---

## 헬퍼: `getSlot`

```go
func (a *Acceptor) getSlot(slot uint64) *AcceptorSlot {
    s, ok := a.slots[slot]
    if !ok {
        s = &AcceptorSlot{
            AcceptedBallot: &pb.Ballot{},
        }
        a.slots[slot] = s
    }
    return s
}
```

**Go 문법:**
- `func (a *Acceptor) getSlot(...)` — `Acceptor`의 **메서드**. `(a *Acceptor)` 부분은 **리시버**라 부른다. 다른 언어의 `this`와 같다. `*`는 리시버가 포인터라는 뜻 (구조체를 수정할 수 있다).
- `s, ok := a.slots[slot]` — Go 맵 조회는 두 값을 반환한다: 값과 키 존재 여부 boolean. `:=`는 짧은 변수 선언 (타입 추론).
- `if !ok { ... }` — 슬롯이 없으면 제로 ballot으로 생성한다.

**목적:** 지연 초기화 — 슬롯은 필요할 때 생성된다.

---

## 영속성: `persist`

```go
func (a *Acceptor) persist(slot uint64, slotState *AcceptorSlot) error {
    if a.wal == nil {
        return nil
    }
    record := &pb.WALRecord{
        Type: pb.WALRecord_ACCEPTOR_STATE,
        AcceptorState: &pb.AcceptorStateRecord{
            Slot:           slot,
            PromisedBallot: slotState.AcceptedBallot,
            AcceptedBallot: slotState.AcceptedBallot,
            AcceptedValue:  slotState.AcceptedValue,
        },
    }
    data, err := proto.Marshal(record)
    if err != nil {
        return err
    }
    return a.wal.Append(data)
}
```

**Go 문법:**
- `error` 반환 타입 — Go는 예외 대신 명시적 에러 반환을 사용한다. 호출자가 `if err != nil`로 확인한다.
- `proto.Marshal(record)` — protobuf 구조체를 바이트로 직렬화한다.
- `pb.WALRecord_ACCEPTOR_STATE` — protobuf 정의의 열거형 값.

**Paxos 의미:**
이것은 **정확성에 핵심적**이다. Acceptor는 응답하기 **전에** 반드시 상태를 영속화해야 한다. 장애 후 재시작하면 무엇을 약속했는지 기억해야 한다. 이것 없이는 재시작 후 약속을 어길 수 있어 Paxos의 안전성 보장을 위반한다.

---

## Phase 1: `HandlePrepare`

Acceptor 관점에서 **Paxos Phase 1의 핵심**.

```go
func (a *Acceptor) HandlePrepare(_ context.Context, req *pb.PrepareRequest) (*pb.PrepareResponse, error) {
    a.mu.Lock()
    defer a.mu.Unlock()

    if BallotGreaterThan(a.promisedBallot, req.Ballot) {
        return &pb.PrepareResponse{
            Ok:             false,
            PromisedBallot: BallotClone(a.promisedBallot),
        }, nil
    }

    a.promisedBallot = BallotClone(req.Ballot)

    resp := &pb.PrepareResponse{
        Ok:              true,
        AcceptedEntries: make([]*pb.AcceptedEntry, 0),
    }
    for slotNum, slotState := range a.slots {
        if slotState.AcceptedValue != nil {
            resp.AcceptedEntries = append(resp.AcceptedEntries, &pb.AcceptedEntry{
                Slot:    slotNum,
                Ballot:  BallotClone(slotState.AcceptedBallot),
                Command: slotState.AcceptedValue,
            })
        }
    }

    if a.wal != nil {
        s := a.getSlot(0)
        if err := a.persist(0, s); err != nil {
            return nil, fmt.Errorf("persist acceptor state: %w", err)
        }
    }

    a.log.Debug("prepare: promised", "node", a.nodeID, "ballot", req.Ballot)
    return resp, nil
}
```

**단계별 설명:**

1. **뮤텍스 잠금** — `a.mu.Lock()`은 한 번에 하나의 고루틴만 요청을 처리하도록 보장한다. `defer a.mu.Unlock()`은 함수 반환 시 (에러 시에도) 잠금 해제를 보장한다. `defer`는 Go의 정리 스케줄링 방식.

2. **약속 확인** — 이 Acceptor가 이미 **더 높은** ballot을 약속했다면 요청을 거절한다. `Ok: false`를 반환하고 이미 약속한 ballot을 알려준다 (Proposer가 더 높은 번호로 재시도할 수 있도록).

3. **새 약속** — `promisedBallot`을 들어온 ballot으로 업데이트한다. 이제부터 이 Acceptor는 더 낮은 ballot의 제안을 거절한다. `BallotClone`은 공유 가변 상태를 피하기 위해 복사본을 만든다.

4. **이전에 수락한 값 수집** — 핵심 Multi-Paxos 세부사항. Acceptor는 **이미 값을 수락한 모든 슬롯**을 Proposer에게 알려준다. (아래 "왜 수락된 값을 수집하는가?" 섹션에서 상세 설명)

5. **응답 전 영속화** — 응답 보내기 전에 WAL에 상태를 기록한다. `fmt.Errorf("...: %w", err)`는 에러에 컨텍스트를 감싼다 (`%w` 동사는 에러 언래핑을 가능하게 한다).

6. **응답 반환** — `Ok: true`와 모든 수락된 항목.

**비유:** 투표라고 생각하면 된다. Proposer가 "나는 후보 #5야, 나를 지지해줄래?"라고 말한다. Acceptor는:
- "아니, 이미 후보 #7에게 서약했어" (거절)
- "좋아, 지지할게. 그런데 참고로, 이전 후보 아래에서 이런 것들에 이미 투표했어" (약속 + 이력)

---

## 왜 HandlePrepare에서 수락된 값을 수집하는가?

### 핵심 문제: 결정된 값을 잃지 않기

3개 노드: A, B, C. 과반수는 2.

#### 시나리오: 수락된 값 수집이 없다면

```
시점 1: Proposer-1 (ballot #1)이 슬롯 3에 "PUT foo=bar"를 제안
         → Acceptor-A 수락 ✓
         → Acceptor-B 수락 ✓  (과반수! 값이 사실상 결정됨)
         → Acceptor-C는 수신 못함 (네트워크 지연)

시점 2: Proposer-1이 Commit 메시지를 보내기 전에 장애 발생.
         아무도 값이 결정된 것을 모름.

시점 3: Proposer-2가 ballot #2로 새 리더가 됨.
         슬롯 3에 자기 커맨드 "DEL foo"를 사용하고 싶어함.
```

**Proposer-2가 그냥 "DEL foo"를 슬롯 3에 제안하면, 이미 과반수에 의해 결정된 "PUT foo=bar"를 덮어쓰게 된다.** 이것은 Paxos의 근본적인 안전성 보장을 깨뜨린다: **한번 결정된 값은 절대 변경될 수 없다.**

#### 수락된 값 수집이 이를 어떻게 방지하는가

```
시점 3: Proposer-2가 모든 Acceptor에게 Prepare(ballot #2)를 전송.

         Acceptor-A 응답: "OK, ballot #2를 약속할게.
                           참고로, 슬롯 3에서 ballot #1 아래 'PUT foo=bar'를 수락했어"
         Acceptor-B 응답: "OK, ballot #2를 약속할게.
                           참고로, 슬롯 3에서 ballot #1 아래 'PUT foo=bar'를 수락했어"
         Acceptor-C 응답: "OK, ballot #2를 약속할게.
                           수락한 것 없어."

시점 4: Proposer-2가 슬롯 3에 이미 수락된 값이 있음을 확인.
         슬롯 3에 "PUT foo=bar"를 재제안해야 한다 (자기 "DEL foo"가 아니라).
         응답 중 가장 높은 ballot의 값을 선택한다.

         → "DEL foo"는 다른 슬롯을 사용할 수 있다.
```

**결정된 값이 보존된다.** 이것이 핵심이다.

### Proposer가 따라야 하는 규칙

Prepare 응답을 받은 후, 각 슬롯에 대해:

1. **어떤** Acceptor라도 해당 슬롯에 수락된 값을 보고했다면 → Proposer는 **가장 높은 ballot** 아래에서 수락된 값을 **반드시** 재제안해야 한다
2. **아무** Acceptor도 해당 슬롯에 값을 보고하지 않았다면 → Proposer는 원하는 것을 자유롭게 제안할 수 있다

이것이 Acceptor가 각 슬롯에 대해 `Ballot`과 `Command`를 모두 반환하는 이유다:

```go
for slotNum, slotState := range a.slots {
    if slotState.AcceptedValue != nil {
        resp.AcceptedEntries = append(resp.AcceptedEntries, &pb.AcceptedEntry{
            Slot:    slotNum,
            Ballot:  BallotClone(slotState.AcceptedBallot),
            Command: slotState.AcceptedValue,
        })
    }
}
```

Proposer는 여러 Acceptor가 같은 슬롯에 다른 값을 보고할 때 **어떤** 수락된 값을 사용할지 결정하기 위해 `Ballot`이 필요하다 (가장 높은 ballot의 것을 선택).

### 왜 "가장 높은 ballot"이 작동하는가?

더 까다로운 시나리오 — Acceptor들이 불일치:

```
슬롯 3:
  Acceptor-A가 ballot #1 아래 "PUT foo=bar"를 수락
  Acceptor-B가 ballot #3 아래 "PUT foo=baz"를 수락
  Acceptor-C는 아무것도 수락 안 함
```

Ballot #3이 #1보다 높다. ballot #3 아래의 값은 **나중에** 제안되었고 같은 과정을 거쳤다. 그래서:
- 자유롭게 선택되었거나 (ballot #3 실행 시 이전 수락된 값이 없었음), 또는
- 더 이전에 결정된 값에서 전달된 것

어느 쪽이든, 가장 높은 ballot의 값이 실제 결정된 값에 가장 가깝거나 같다. 항상 가장 높은 것을 선택함으로써 리더 교체 시에도 결정된 값을 보존하는 관리 체인을 형성한다.

### 왜 하나가 아니라 모든 슬롯인가?

이것이 **Multi-Paxos** 측면이다. 기본 Paxos에서는 슬롯이 하나뿐이다 (합의할 값 하나). Multi-Paxos에서는 로그에 많은 슬롯이 있고, 새 리더가 **여러 슬롯**의 결정을 놓쳤을 수 있다. 그래서 Acceptor는 알고 있는 모든 것을 전달한다:

```
"내가 수락한 것들:
  슬롯 3 → PUT foo=bar (ballot #1)
  슬롯 5 → DEL baz     (ballot #1)
  슬롯 7 → PUT x=y     (ballot #4)"
```

새 Proposer는 모든 Acceptor의 정보를 종합하여 어떤 슬롯에 이미 결정된 (또는 진행 중인) 값이 있는지 파악할 수 있다.

---

## Phase 2: `HandleAccept`

```go
func (a *Acceptor) HandleAccept(_ context.Context, req *pb.AcceptRequest) (*pb.AcceptResponse, error) {
    a.mu.Lock()
    defer a.mu.Unlock()

    if BallotGreaterThan(a.promisedBallot, req.Ballot) {
        return &pb.AcceptResponse{
            Ok:             false,
            PromisedBallot: BallotClone(a.promisedBallot),
        }, nil
    }

    a.promisedBallot = BallotClone(req.Ballot)
    slotState := a.getSlot(req.Slot)
    slotState.AcceptedBallot = BallotClone(req.Ballot)
    slotState.AcceptedValue = req.Command

    if err := a.persist(req.Slot, slotState); err != nil {
        return nil, fmt.Errorf("persist acceptor state: %w", err)
    }

    a.log.Debug("accept: accepted", "node", a.nodeID, "slot", req.Slot, "ballot", req.Ballot)
    return &pb.AcceptResponse{Ok: true}, nil
}
```

**단계별:**

1. **같은 약속 확인** — Phase 1 이후 더 높은 ballot이 약속되었다면 거절.

2. **수락된 값 기록**:
   - 글로벌 `promisedBallot` 업데이트
   - 특정 `slot`에 ballot과 커맨드 저장

3. **영속화 후 응답** — Phase 1과 같은 패턴.

**비유:** Proposer가 "좋아, 슬롯 #3에 ballot #5 아래 PUT(key=foo, value=bar) 커맨드를 수락해줘"라고 말한다. Acceptor는 "알겠어, 기록했어" 또는 "미안, 그 사이에 ballot #7을 약속했어"로 응답한다.

---

## 복구: `RestoreSlot`

```go
func (a *Acceptor) RestoreSlot(rec *pb.AcceptorStateRecord) {
    a.mu.Lock()
    defer a.mu.Unlock()

    if rec.PromisedBallot != nil && BallotGreaterThan(rec.PromisedBallot, a.promisedBallot) {
        a.promisedBallot = rec.PromisedBallot
    }

    slotState := a.getSlot(rec.Slot)
    if rec.AcceptedBallot != nil {
        slotState.AcceptedBallot = rec.AcceptedBallot
    }
    slotState.AcceptedValue = rec.AcceptedValue
}
```

**장애 후 시작 시** 호출된다. WAL 레코드를 재생하여 Acceptor의 상태를 재구축한다.

핵심 세부사항: 레코드의 ballot이 현재 것보다 **높을 때만** `promisedBallot`을 업데이트한다. 여러 WAL 레코드가 재생될 때 가장 높은 약속이 이기도록 처리한다.

---

## 요약: 전체 흐름

```
Proposer                     Acceptor
   |                            |
   |--- PrepareRequest ------->|  "ballot #5를 약속해줄래?"
   |                            |  확인: #5 >= 내 약속?
   |                            |  예 → 약속 업데이트, 수락 이력 반환
   |<-- PrepareResponse -------|  아니오 → 거절, 현재 약속 반환
   |                            |
   |--- AcceptRequest -------->|  "슬롯 3에 ballot #5로 CMD 수락해줘"
   |                            |  확인: #5 >= 내 약속?
   |                            |  예 → (ballot, 값) 기록, 영속화, OK
   |<-- AcceptResponse --------|  아니오 → 거절
   |                            |
```

Acceptor는 근본적으로 **문지기**다 — 한번 ballot을 지지하겠다고 약속하면 절대 뒤로 가지 않도록 보장한다. 이 단일 불변성이 Paxos를 안전하게 만드는 것이다.
