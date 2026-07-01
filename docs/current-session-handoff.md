# Current Session Handoff

## Goal

- Mode: Producer handoff for Claude Code / next operator.
- Current user goal: finish Eric Chang's Kafka presentation/blog topic.
- Topic: `溝通時的版本選擇：為什麼要用這個設計、使用者看到版本錯誤時可以怎麼處理、發展至今的版本截斷`.
- Desired next outcome: continue from the blog draft, review accuracy, then distill it into presentation slides.

## Project State

- Repo/worktree: `/Users/unknowntpo/repo/unknowntpo/kafka/palantir-decomposition`
- Parent repo area: `/Users/unknowntpo/repo/unknowntpo/kafka`
- Branch: `codex/palantir-decomposition`
- Upstream status: branch is behind `origin/trunk` by 121 commits.
- Dirty status from `git status --short --branch`:

```text
## codex/palantir-decomposition...origin/trunk [behind 121]
 M clients/src/test/java/org/apache/kafka/clients/NodeApiVersionsTest.java
 M metadata/src/test/java/org/apache/kafka/controller/ClusterControlManagerTest.java
?? docs/current-session-handoff.md
?? study/
```

Treat existing dirty/untracked files as intentional user/study artifacts. Do not revert them.

Important files:

- Blog draft: `study/version-control/kafka-version-negotiation-blog-draft.md`
- Working notes: `study/version-control/kafka-version-negotiation-blog-notes.md`
- Lab compose file: `study/version-control/lab/tour0/docker-compose.yml`
- Client playground tests: `clients/src/test/java/org/apache/kafka/clients/NodeApiVersionsTest.java`
- Feature/MV playground test: `metadata/src/test/java/org/apache/kafka/controller/ClusterControlManagerTest.java`
- This handoff: `docs/current-session-handoff.md`

## What Changed

Completed:

- Created a full blog draft for Eric's topic.
- Added progressive-disclosure structure:
  - three version layers: release version, `metadata.version` / feature version, wire protocol API version
  - client-broker API version negotiation
  - client-side exception workflow
  - protocol version truncation
  - broker-broker internal RPC and `metadata.version`
  - fenced broker / feature update Chia 7712 question
  - user-facing debug decision tree
  - slide outline and evidence checklist
- Added a new `版本截斷` section at `study/version-control/kafka-version-negotiation-blog-draft.md`.
- Updated notes with the version-truncation conclusion.
- Added small playground tests:
  - `NodeApiVersionsTest.testPlaygroundClientChoosesHighestCommonProduceVersion`
  - `NodeApiVersionsTest.testPlaygroundClientAbortsWhenProduceVersionsDoNotOverlap`
  - `ClusterControlManagerTest.testFencedBrokerRegistrationStillBlocksFeatureUpdate`

In progress:

- Blog draft is structurally complete, but still needs editorial review before publishing.
- Presentation slides have not been created yet; only the extractable slide outline exists in the draft.

Deferred:

- Deep dive into long-term compatibility policy beyond the currently added Kafka 4.0 protocol truncation evidence.
- Turning the draft into actual slides.

## Key Technical Conclusions

- Kafka version discussion must be split into three layers:
  - release version: binary/distribution version
  - `metadata.version` / feature version: cluster-wide finalized capability boundary
  - wire protocol API version: per-request request/response schema version
- Client-broker protocol selection uses the highest common API version range.
- If there is no overlap, Java clients can raise `UnsupportedVersionException` before sending the request over the socket.
- Broker-broker/internal request versions are not fully explained by client-style ApiVersions negotiation; some paths are constrained by finalized `metadata.version`.
- Version truncation is real: modern Kafka does not necessarily support old protocol versions forever.
  - Example evidence: `FetchRequest.json` has `validVersions: 4-18`; lab output shows `Fetch(1): 4 to 18`.
  - Kafka 4.0 upgrade docs state old protocol API versions were removed.
- Fenced broker conclusion:
  - `fenced=true` does not remove broker registration.
  - Feature update validation still sees registered broker `supportedFeatures`.
  - A fenced but still registered broker can block a feature/MV upgrade if its supported range does not include the target.
  - Unregister/decommission removes the registration.

## Evidence Anchors

Client-broker negotiation:

- `docs/design/protocol.md:108`
- `clients/src/main/java/org/apache/kafka/clients/NodeApiVersions.java:149`
- `clients/src/main/java/org/apache/kafka/clients/NetworkClient.java:591`
- `clients/src/main/java/org/apache/kafka/clients/NetworkClient.java:597`
- `clients/src/main/java/org/apache/kafka/clients/NetworkClient.java:940`
- `clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java:595`
- `clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerNetworkClient.java:614`

Version truncation:

- `docs/getting-started/upgrade.md:229`
- `docs/design/protocol.md:115`
- `clients/src/main/resources/common/message/FetchRequest.json:61`
- `clients/src/main/resources/common/message/ListOffsetsRequest.json:45`
- `clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java:287`

`metadata.version` and broker-broker/internal behavior:

- `docs/getting-started/zk2kraft.md:71`
- `core/src/main/scala/kafka/server/RemoteLeaderEndPoint.scala:215`
- `server-common/src/main/java/org/apache/kafka/server/common/MetadataVersion.java:273`
- `server-common/src/main/java/org/apache/kafka/server/common/MetadataVersion.java:289`
- `metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java:462`
- `metadata/src/main/java/org/apache/kafka/controller/PartitionChangeBuilder.java:464`

Fenced broker / feature update:

- `metadata/src/main/java/org/apache/kafka/controller/FeatureControlManager.java:321`
- `metadata/src/main/java/org/apache/kafka/controller/FeatureControlManager.java:334`
- `metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java:836`
- `metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java:1762`
- `metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java:595`

## Runtime / Environment

Kafka lab compose file:

```text
study/version-control/lab/tour0/docker-compose.yml
```

Known lab state from earlier in the session:

- Kafka image: `apache/kafka:4.1.0`
- Controllers: `controller-1`, `controller-2`, `controller-3`
- Brokers: `broker-1`, `broker-2`, `broker-3`
- Redpanda Console: `http://localhost:18080`
- Host broker ports:
  - `localhost:29192`
  - `localhost:39192`
  - `localhost:49192`

Useful lab commands:

```bash
docker compose -f study/version-control/lab/tour0/docker-compose.yml ps
docker compose -f study/version-control/lab/tour0/docker-compose.yml exec broker-1 \
  /opt/kafka/bin/kafka-features.sh --bootstrap-server broker-1:19092 describe
docker compose -f study/version-control/lab/tour0/docker-compose.yml exec broker-1 \
  /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server broker-1:19092
```

Docker access may require approval/elevated execution depending on the tool environment.

## Verification

Passed earlier:

```bash
./gradlew :clients:test \
  --tests org.apache.kafka.clients.NodeApiVersionsTest.testPlaygroundClientChoosesHighestCommonProduceVersion \
  --tests org.apache.kafka.clients.NodeApiVersionsTest.testPlaygroundClientAbortsWhenProduceVersionsDoNotOverlap
```

Earlier observed result:

```text
BUILD SUCCESSFUL
```

Passed earlier:

```bash
./gradlew :metadata:test \
  --tests org.apache.kafka.controller.ClusterControlManagerTest.testFencedBrokerRegistrationStillBlocksFeatureUpdate
```

Earlier observed result:

```text
BUILD SUCCESSFUL
```

Known warning:

- In Codex sandbox, Gradle wrapper initially failed because it could not write `~/.gradle/...zip.lck`.
- Re-running with permission to access Gradle cache succeeded.

Not run after the latest handoff update:

- Full test suite.
- Blog markdown linting.
- Slide generation.

## Security Notes

- No secrets were needed or included.
- No credentials, tokens, cookies, or private keys are in this handoff.
- Commands that may require approval:
  - Docker commands that access the daemon.
  - Gradle commands if the environment blocks access to `~/.gradle`.

## Next Steps

1. First action: open `study/version-control/kafka-version-negotiation-blog-draft.md` and review the new `版本截斷` section for wording and accuracy.
2. Second action: do a claim-by-claim review of the evidence checklist at the bottom of the draft.
3. Third action: convert the `可抽成 slide 的精華` section into Eric's slide outline.
4. Optional follow-up: rerun the two targeted Gradle tests if code/test evidence must be freshly verified.

## Unknowns

- Whether Eric wants the final output as blog only, slides only, or both.
- Whether to keep the playground tests as permanent repo changes or treat them as local study/demo tests only.
- Whether the lab containers are still running at handoff time; check with the compose `ps` command above.
