#!/usr/bin/env python3
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import MSO_AUTO_SIZE
import os

HERE = os.path.dirname(os.path.abspath(__file__))
ICO = os.path.join(HERE, "icons") + os.sep

ACCENT = RGBColor(0x18, 0x5F, 0xA5)
DARK   = RGBColor(0x1E, 0x20, 0x24)
GRAY   = RGBColor(0x5f, 0x5e, 0x5a)
MUTED  = RGBColor(0x8a, 0x8a, 0x8a)
CODEBG = RGBColor(0xF3, 0xF1, 0xEC)   # subtle warm-grey panel (Claude minimalist)
CODEFG = RGBColor(0x2C, 0x2C, 0x2A)
HAIR   = RGBColor(0xE3, 0xE0, 0xD8)   # hairline border
SOLVEBG= RGBColor(0xE9, 0xF3, 0xDE)   # soft green tint
SOLVEFG= RGBColor(0x27, 0x50, 0x0A)
GREEN  = RGBColor(0x27, 0x50, 0x0A)

UP = "b7b1c0a83d856766390ee0c05e33b63711eee80e"   # apache/kafka trunk commit (upstream line numbers)
FK = "27342708500d978cb9b7477b4a870d4954ec13b1"   # unknowntpo/kafka commit (demo tests / lab)
P = {
 "protocol.md":"docs/design/protocol.md",
 "zk2kraft.md":"docs/getting-started/zk2kraft.md",
 "upgrade.md":"docs/getting-started/upgrade.md",
 "NodeApiVersions.java":"clients/src/main/java/org/apache/kafka/clients/NodeApiVersions.java",
 "NetworkClient.java":"clients/src/main/java/org/apache/kafka/clients/NetworkClient.java",
 "ApiKeys.java":"clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java",
 "FetchRequest.json":"clients/src/main/resources/common/message/FetchRequest.json",
 "FetchRequest.java":"clients/src/main/java/org/apache/kafka/common/requests/FetchRequest.java",
 "RequestContext.java":"clients/src/main/java/org/apache/kafka/common/requests/RequestContext.java",
 "SocketServer.scala":"core/src/main/scala/kafka/network/SocketServer.scala",
 "KafkaApis.scala":"core/src/main/scala/kafka/server/KafkaApis.scala",
 "TransactionMarkerChannelManager.scala":"core/src/main/scala/kafka/coordinator/transaction/TransactionMarkerChannelManager.scala",
 "OffsetsForLeaderEpochRequest.java":"clients/src/main/java/org/apache/kafka/common/requests/OffsetsForLeaderEpochRequest.java",
 "RequestResponseTest.java":"clients/src/test/java/org/apache/kafka/common/requests/RequestResponseTest.java",
 "MessageTest.java":"clients/src/test/java/org/apache/kafka/common/message/MessageTest.java",
 "Dockerfile":"tests/docker/Dockerfile",
 "version.py":"tests/kafkatest/version.py",
 "client_compat.py":"tests/kafkatest/tests/client/client_compatibility_features_test.py",
 "compat_newbroker.py":"tests/kafkatest/tests/core/compatibility_test_new_broker_test.py",
 "upgrade_test.py":"tests/kafkatest/tests/core/upgrade_test.py",
 "MetadataVersionTest.java":"server-common/src/test/java/org/apache/kafka/server/common/MetadataVersionTest.java",
 "BrokerBlockingSender.scala":"core/src/main/scala/kafka/server/BrokerBlockingSender.scala",
 "MetadataVersion.java":"server-common/src/main/java/org/apache/kafka/server/common/MetadataVersion.java",
 "RemoteLeaderEndPoint.scala":"core/src/main/scala/kafka/server/RemoteLeaderEndPoint.scala",
 "FeatureControlManager.java":"metadata/src/main/java/org/apache/kafka/controller/FeatureControlManager.java",
 "ClusterControlManager.java":"metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java",
 "Feature.java":"server-common/src/main/java/org/apache/kafka/server/common/Feature.java",
 "NodeApiVersionsTest.java":"clients/src/test/java/org/apache/kafka/clients/NodeApiVersionsTest.java",
 "ClusterControlManagerTest.java":"metadata/src/test/java/org/apache/kafka/controller/ClusterControlManagerTest.java",
 "docker-compose.yml":"study/version-control/lab/tour0/docker-compose.yml",
}
KIP896="https://cwiki.apache.org/confluence/display/KAFKA/KIP-896%3A+Remove+old+client+protocol+API+versions+in+Kafka+4.0"
KIP35="https://cwiki.apache.org/confluence/display/KAFKA/KIP-35+-+Retrieving+protocol+version"
KIP584="https://cwiki.apache.org/confluence/display/KAFKA/KIP-584%3A+Versioning+scheme+for+features"
PGURL="https://www.postgresql.org/docs/current/protocol-flow.html"
PGLOGREPL="https://www.postgresql.org/docs/current/logical-replication.html"
MONGO="https://www.mongodb.com/docs/manual/reference/command/hello/"

def A(name, line=None, disp=None):
    u = "https://github.com/apache/kafka/blob/%s/%s" % (UP, P[name])
    if line: u += "#L%d" % line
    return (disp or (name + (":%d" % line if line else "")), u, True)
def F(name, line=None, disp=None):
    u = "https://github.com/unknowntpo/kafka/blob/%s/%s" % (FK, P[name])
    if line: u += "#L%d" % line
    return (disp or (name + (":%d" % line if line else "")), u, True)
def X(text, url): return (text, url, False)
def T(text): return (text, None, False)

prs = Presentation()
prs.slide_width  = Inches(13.333)
prs.slide_height = Inches(7.5)
BLANK = prs.slide_layouts[6]
CW = 12.0

def tb(slide, x, y, w, h):
    b = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = b.text_frame; tf.word_wrap = True; tf.auto_size = MSO_AUTO_SIZE.NONE
    return b, tf

def run(p, text, size, color, bold=False, mono=False):
    r = p.add_run(); r.text = text
    r.font.size = Pt(size); r.font.color.rgb = color; r.font.bold = bold
    r.font.name = "Courier New" if mono else "Calibri"
    return r

def has_cjk(s):
    return any('　' <= c <= '鿿' or '＀' <= c <= '￯' for c in s)

def est_lines(text, cpl):
    n = 0
    for line in text.split("\n"):
        n += max(1, -(-len(line)//cpl))
    return n

def render_ref(s, tokens):
    _, tf = tb(s, 0.7, 7.0, CW, 0.42)
    p = tf.paragraphs[0]
    run(p, "REF   ", 12, MUTED, bold=True)
    for i, (text, url, mono) in enumerate(tokens):
        if i > 0: run(p, "   ·   ", 12, MUTED)
        r = run(p, text, 12, ACCENT if url else MUTED, mono=mono)
        if url:
            r.hyperlink.address = url
            r.font.underline = True

def add_content(kicker, title, blocks, ref=None):
    s = prs.slides.add_slide(BLANK)
    if kicker:
        _, tf = tb(s, 0.7, 0.5, CW, 0.4); run(tf.paragraphs[0], kicker, 13, ACCENT, bold=True)
    _, tf = tb(s, 0.7, 0.92, CW, 1.1); run(tf.paragraphs[0], title, 32, DARK, bold=True)
    y = 2.35
    for blk in blocks:
        kind, text = blk[0], blk[1]
        if kind == "code":
            h = est_lines(text, 58)*0.30 + 0.30
            box, tf = tb(s, 0.7, y, CW, h)
            box.fill.solid(); box.fill.fore_color.rgb = CODEBG
            box.line.color.rgb = HAIR; box.line.width = Pt(0.5)
            tf.margin_left = Inches(0.16); tf.margin_top = Inches(0.1); tf.margin_bottom = Inches(0.1)
            mono = not has_cjk(text)
            first = True
            for ln in text.split("\n"):
                p = tf.paragraphs[0] if first else tf.add_paragraph(); first = False
                run(p, ln if ln else " ", 13.5, CODEFG, mono=mono)
            y += h + 0.22
        elif kind == "solve":
            h = est_lines(text, 50)*0.34 + 0.28
            box, tf = tb(s, 0.7, y, CW, h)
            box.fill.solid(); box.fill.fore_color.rgb = SOLVEBG; box.line.fill.background()
            tf.margin_left = Inches(0.16); tf.margin_top = Inches(0.09)
            run(tf.paragraphs[0], text, 15, SOLVEFG)
            y += h + 0.20
        elif kind == "note":
            h = est_lines(text, 62)*0.32 + 0.16
            _, tf = tb(s, 0.7, y, CW, h); run(tf.paragraphs[0], text, 15, GRAY)
            y += h + 0.14
        else:
            icon = blk[2] if len(blk) > 2 else "point"
            h = est_lines(text, 42)*0.38 + 0.16
            s.shapes.add_picture(ICO + icon + ".png", Inches(0.72), Inches(y + 0.03), height=Inches(0.30))
            _, tf = tb(s, 1.18, y, CW - 0.5, h); run(tf.paragraphs[0], text, 16.5, DARK)
            y += h + 0.16
    if ref: render_ref(s, ref)
    return s

def add_quiz(tag, q, opts, correct, why, ref, reveal):
    # reveal=False → 只出題、蓋住答案；reveal=True → 公布正解（下一頁）
    s = prs.slides.add_slide(BLANK)
    label = "隨堂考 · " + tag + ("　（正解）" if reveal else "")
    _, tf = tb(s, 0.7, 0.5, CW, 0.4); run(tf.paragraphs[0], label, 13, ACCENT, bold=True)
    _, tf = tb(s, 0.7, 0.92, CW, 1.3); run(tf.paragraphs[0], q, 26, DARK, bold=True)
    y = 2.55
    letters = "ABCD"
    for k, o in enumerate(opts):
        h = est_lines(o, 52)*0.36 + 0.18
        box, tf = tb(s, 0.9, y, 11.5, h)
        ok = reveal and (k == correct)
        if ok:
            box.fill.solid(); box.fill.fore_color.rgb = SOLVEBG; box.line.fill.background()
            tf.margin_left = Inches(0.14); tf.margin_top = Inches(0.06)
        p = tf.paragraphs[0]
        run(p, letters[k] + ".  ", 16, GREEN if ok else DARK, bold=ok)
        run(p, o + ("   ✓" if ok else ""), 16, GREEN if ok else DARK, bold=ok)
        y += h + 0.14
    _, tf = tb(s, 0.7, y + 0.1, CW, 1.0)
    if reveal:
        p = tf.paragraphs[0]; run(p, "正解 " + letters[correct] + " — ", 14, GREEN, bold=True); run(p, why, 14, DARK)
    else:
        run(tf.paragraphs[0], "想一想 —— 下一頁公布答案", 14, MUTED)
    if ref and reveal: render_ref(s, ref)
    return s
def quiz_pair(i):
    add_quiz(*QUIZ[i], reveal=False)
    add_quiz(*QUIZ[i], reveal=True)

# 只留本場範圍內、有教育意義的 2 題（其餘屬兄弟場或過於簡單，已移除）
QUIZ = [
 ("核心", "partition replication（follower→leader）裡，Fetch 的版本怎麼決定？",
  ["兩 broker ApiVersions 協商取交集", "由 finalized metadata.version 決定", "永遠用最新版", "controller 每次指定"], 1,
  "fetchRequestVersion(MV)：MV 是集中共識，不做 per-connection 協商。",
  [A("RemoteLeaderEndPoint.scala",215), A("MetadataVersion.java",273)]),
 ("核心", "client 繞過協商、直接送出 broker 不支援的 API version，會怎樣？",
  ["一律回 UNSUPPORTED_VERSION 錯誤碼", "只有 ApiVersions 例外回錯誤碼+支援範圍；其他 API 直接關閉連線", "broker 自動降版處理", "忽略版本照常處理"], 1,
  "ApiVersions 是協商的第一支 request（此時雙方還不知道彼此版本），若它也關閉連線，client 永遠問不到支援範圍，故回 v0+錯誤碼讓 client 重新協商；其他 API 在 RequestContext 解析失敗、SocketServer 關閉連線。",
  [A("RequestContext.java",112), A("SocketServer.scala",781)]),
]
# ---- Title ----
s = prs.slides.add_slide(BLANK)
bar = s.shapes.add_shape(1, Inches(0.9), Inches(2.62), Inches(0.1), Inches(1.5))
bar.fill.solid(); bar.fill.fore_color.rgb = ACCENT; bar.line.fill.background()
_, tf = tb(s, 1.2, 1.5, 11.3, 0.5); run(tf.paragraphs[0], "幾萬個節點之上的版本控制 · 第三場", 15, ACCENT, bold=True)
_, tf = tb(s, 1.2, 2.6, 11.3, 1.4); run(tf.paragraphs[0], "溝通當下的版本選擇", 46, DARK, bold=True)
_, tf = tb(s, 1.2, 4.2, 11.3, 0.9); run(tf.paragraphs[0], "長壽的 client、滾動升級的 broker，怎麼喬出共同版本", 20, GRAY)
_, tf = tb(s, 1.2, 6.65, 11.3, 0.5); run(tf.paragraphs[0], "Eric Chang · 2026", 14, MUTED)

add_content("議程", "本場範圍", [
    ("bullet", "Part 1 — 為什麼不能「一個版本打天下」，版本又是怎麼選出來的", "point"),
    ("bullet", "Part 2 — 協商失敗時，會看到什麼訊息", "point"),
])

add_content("點題", "為什麼不能「一個版本打天下」？", [
    ("code", "直覺（想像中）：  client v4.1 ─────── broker v4.1   同版、一起升"),
    ("bullet", "現實一：client 是內嵌在應用裡的函式庫——IoT 裝置、車載系統、多年前的 batch job，隨應用長期運行、難更新（Kafka 曾為此保留每個 protocol 版本近九年）", "clock-hour-4"),
    ("bullet", "現實二：Kafka 要求 zero downtime → broker 只能一台一台滾動升級 → 過程必然新舊混版", "server-2"),
    ("solve", "兩個現實加起來：不能鎖全叢集同一版（又要不停機）——版本無法事先統一，那要怎麼選？"),
    ("note", "這其實就是 backward／forward compatibility：一般服務多靠『加欄位＋ URL 版號 /v1 /v2』相容；Kafka 特別在每支 API 各自宣告版本區間、連線當下協商挑一版（更像 TLS 選 cipher，而非固定版號）。"),
], [X("KIP-896",KIP896), A("protocol.md",94)])

add_content("Part 1 · 術語", "要談「怎麼選」，先分清楚「版本」是哪一層", [
    ("bullet", "上一頁的結論是「版本無法事先統一」；但在講機制前得先知道，日常講的『版本』其實混了三個東西", "arrows-split"),
    ("code", "release version   = 我裝了哪版 binary          (per-node)\nmetadata.version  = 管理員替全叢集設的功能檔位  (cluster-wide)\nwire API version  = 這條連線講第幾版            (per-connection)"),
    ("note", "finalize＝管理員手動宣告全叢集一致採用的 feature level（第一場《版本定義》的主題）。本場主角是 wire 版本。"),
], [T("kafka-features describe"), A("zk2kraft.md",71)])

add_content("Part 1 · 架構 (a)", "版本怎麼定：協商，還是由 finalized MV 決定", [
    ("bullet", "機制一 · 協商（KIP-35）：連線後送 ApiVersionsRequest、取交集最高——多數路徑都是", "arrows-split"),
    ("bullet", "機制二 · 由 finalized MV 事先決定（KIP-584）：只有 partition replication（follower→leader）這條不協商", "ban"),
    ("code", "協商           client↔broker · broker↔controller · Raft · broker↔broker txn markers\nfinalized MV   partition replication（follower→leader，replica fetcher）—— 唯一不協商"),
    ("note", "唯一不協商的是 partition replication（follower→leader）——這條流程發三支 RPC：OffsetsForLeaderEpoch 對齊、ListOffsets 定位、Fetch 抓資料，版本都由 MV／寫死；同是 broker↔broker 的 transaction markers 仍協商。"),
], [X("KIP-35",KIP35), X("KIP-584",KIP584), A("BrokerBlockingSender.scala",95,"replica fetch=false"), A("TransactionMarkerChannelManager.scala",99,"txn markers=true")])

add_content("Part 1 · 架構 (b)", "查詢之後，最終版本誰說了算？", [
    ("code", "協商                    取交集最高（client↔broker、broker↔controller、Raft、txn markers）\npartition replication   不協商——Fetch/ListOffsets 由 finalized MV、OffsetsForLeaderEpoch 寫死 v4"),
    ("bullet", "partition replication（follower→leader）連 ApiVersionsRequest 都不送（discoverBrokerVersions=false）：這條路上一整組 RPC 版本都不看對端能力，改由叢集事先決定", "ban"),
    ("solve", "為什麼交給 MV，不各連線協商？協商能讓 broker 互相『讀得懂』——但讀得懂不代表跨版本行為正確（如 KIP-903 replica-epoch fencing 要全叢集一致生效，才擋得住舊 epoch 的 follower 進 ISR）"),
], [A("MetadataVersion.java",273,"fetchRequestVersion"), A("BrokerBlockingSender.scala",95,"discoverBrokerVersions=false"), A("OffsetsForLeaderEpochRequest.java",65,"forFollower 寫死 v4"), A("RemoteLeaderEndPoint.scala",215)])

# §2a — 數線圖：架構下的一個舉例（cb 協商 vs bb 由 MV）
s2a = prs.slides.add_slide(BLANK)
_, tf = tb(s2a, 0.7, 0.5, CW, 0.4); run(tf.paragraphs[0], "Part 1 · 舉例", 13, ACCENT, bold=True)
_, tf = tb(s2a, 0.7, 0.9, CW, 0.7); run(tf.paragraphs[0], "以一支 Fetch 為例：consumer 協商、replica 由 MV", 29, DARK, bold=True)
_, tf = tb(s2a, 0.7, 1.62, CW, 0.4); run(tf.paragraphs[0], "同一支 Fetch 兩種身分：consumer fetch 走 client↔broker、replica fetch 走 broker↔broker", 14, GRAY)
s2a.shapes.add_picture(ICO + "s2a-rangeline.png", Inches(1.82), Inches(2.15), width=Inches(9.7))
render_ref(s2a, [A("FetchRequest.java",165,"forConsumer/forReplica"), A("MetadataVersion.java",273,"fetchRequestVersion"), A("FetchRequest.json",61,"validVersions")])

add_content("Part 1 · 對照", "同一道題，別家怎麼答——各付什麼代價", [
    ("bullet", "Kafka：per-API 協商＋partition replication 由 finalized MV 決定——單支 API 獨立演進、複製層線上滾動升級；代價＝協定面積最大", "arrows-split"),
    ("bullet", "MongoDB：全域一個 wire version＋FCV 治理叢集——實作簡單；代價＝粒度粗、無法單支 API 獨立演進", "database"),
    ("bullet", "PostgreSQL：協定 v3 極穩定；physical replication 靠 WAL、跨大版不相容 → HA replica 不能就地跨 major 滾，近零停機得改搭 logical replication（bolt-on）", "leaf"),
    ("solve", "沒有免費：Kafka 複製層內建線上滾動升級，PostgreSQL 得另搭 logical replication 才換得到"),
], [X("MongoDB hello / FCV",MONGO), X("PostgreSQL protocol",PGURL), X("PG logical replication",PGLOGREPL)])

add_content("Part 1 · 代價", "代價：每支 API 的 handler 都要逐版分岔", [
    ("code", "每支 API 的 broker handler 都逐版分岔，例如 Fetch：\n  if (version >= 13)  用 topic-id   else   用 topic-name\n  老版缺的欄位給預設 · 回應一律照「request 的版本」序列化"),
    ("bullet", "讀寫欄位、語意差異、預設值全逐版 if (version ≥ N)，散在每支 API 的 handler 裡——加一版就多一堆分支要維護", "arrows-split"),
    ("note", "另一條更大的隱形帳單是相容性測試（下一張細看）——每個版本、每個歷史 release 都得長期盯著。"),
    ("solve", "貴到 Kafka 自己在 KIP-896 承認「maintenance cost up, value down」，4.0 砍掉 2.1 以前的舊版本止血"),
], [A("KafkaApis.scala",568,"version()>=13"), A("RequestContext.java",137,"回應=request 版本"), A("FetchRequest.json",80,"預設值"), X("KIP-896",KIP896)])
add_content("Part 1 · 代價 · 測試", "「相容」到底要保證哪些事？", [
    ("bullet", "① 格式自洽：每個宣稱支援的 wire 版本，自己序列化都要來回讀寫無誤", "arrows-split"),
    ("bullet", "② 新 client → 舊 broker：client 肯協商降版、不假設對方有新功能", "point"),
    ("bullet", "③ 舊 client → 新 broker：broker 保留舊 wire 版本、繼續服務", "point"),
    ("bullet", "④ 滾動升級混版：新舊 broker 並存，inter-broker（finalized MV 決定）＋對 client 都不掉資料", "point"),
    ("solve", "這四條是本場最相關的相容承諾——每一條都得有測試長期盯著（tagged fields、第三方 client 等外圍成本另計）"),
], [A("RequestResponseTest.java",340,"①格式自洽"), A("client_compat.py",109,"②新→舊"), A("compat_newbroker.py",68,"③舊→新"), A("upgrade_test.py",167,"④滾動升級")])

# （SO-WHAT「為什麼越來越貴」測試 slide 已移除——太細節；保留 WHAT 四相容義務那張）

quiz_pair(0)  # 小測驗 1：replica fetch 版本誰決定

# ---- Part 2：失敗會有什麼訊息 ----
add_content("Part 2 · 失敗訊息", "通訊當下協商不出版本，會看到什麼", [
    ("code", "交集空                       → client 端 UnsupportedVersionException（送出前 abort）\nclient 繞過協商、硬送不支援版本 → broker 丟 UnsupportedVersionException、關閉連線\n（ApiVersions 例外：回 v0 + UNSUPPORTED_VERSION 錯誤碼）"),
    ("bullet", "多數情況 client 在送出前就本地中止，根本沒上網路", "ban"),
    ("note", "「硬送」指 client↔broker：自刻或有 bug 的 client 繞過協商；partition replication 的版本已由 finalized MV 事先決定，不會送出對方不懂的版本。"),
], [A("NodeApiVersions.java",149,"latestUsableVersion"), A("NetworkClient.java",591,"NetworkClient"), A("RequestContext.java",112,"RequestContext")])

add_content("Part 2 · 失敗訊息", "版本截斷：為什麼「升一點點」不夠", [
    ("bullet", "前一頁「交集空」最容易被忽略的根因——舊 wire 版本被整批移除", "alert-triangle"),
    ("code", "舊 wire API version 被整個移除，min 可 > 0\n  Fetch：4–18（v0–3 在 4.0 移除）"),
    ("solve", "解法：升任一端到 4.0 前，先確認另一端 ≥ 2.1（upgrade guide 明訂雙向要求），否則協商結果沒有可用版本"),
    ("note", "取捨：0.8.0（2013）起九年保留每個版本——每個舊版本都是活的程式碼與測試面；4.0 用「斷 2018 年前的 client」換「刪九年的碼」，只有 major 版本邊界付得起。"),
    ("note", "銜接（非本場）：finalize / 升降當下的錯誤（INVALID_UPDATE_VERSION 等）屬「運行時的版本升降」那場，本場不展開。"),
], [A("upgrade.md",229), A("FetchRequest.json",61,"FetchRequest.json validVersions")])
quiz_pair(1)  # 小測驗 2：自刻不支援版本會怎樣

add_content("Recap", "回到開場那個問題", [
    ("code", "想像中：client v4.1 ─── broker v4.1\n實際上：同一顆 4.1 broker，同時講 Fetch v11（對老 consumer）和 v17（對 replica）"),
    ("bullet", "Part 1：不能「一個版本打天下」（client 長壽、broker 滾動升級）；多數路徑靠協商，唯一不協商的是 partition replication（follower→leader）——上面 Fetch/ListOffsets 等由 finalized MV 決定（其餘 broker↔broker 仍協商）", "point"),
    ("bullet", "Part 2：協商不出版本 → UnsupportedVersionException（多在送出前中止）；finalize／升降的錯誤交給運行時那場", "point"),
    ("solve", "立場：per-API 細粒度協商＋replication 集中治理，是為「client 生態極度分散」量身的取捨——對 Kafka 划算，但不是通用解"),
])

out = os.path.join(HERE, "..", "kafka-version-negotiation.pptx")
prs.save(out)
print("slides:", len(prs.slides._sldIdLst), "->", out)
