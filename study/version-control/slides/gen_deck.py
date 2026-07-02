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
PGURL="https://www.postgresql.org/docs/current/protocol-flow.html"
MONGO="https://www.mongodb.com/docs/manual/reference/command/hello/"
KIP482="https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields"
PBUF="https://protobuf.dev/programming-guides/encoding/"

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

QUIZ = [
 ("暖身", "可以「一個版本打天下」嗎？",
  ["可以，大家同一版一起升", "不行——client 長壽 + 要不停機，得溝通當下協商", "可以，只要都 4.x", "看 broker 數量"], 1,
  "client 嵌在各 app、超長壽，混版是常態；要不停機升級就得每條連線各自選版本。",
  [A("protocol.md",94), X("KIP-896",KIP896)]),
 ("暖身", "「版本截斷」是 wire 還是 metadata.version 的事？",
  ["wire protocol", "metadata.version", "兩個都有，是兩條獨立的軸", "Kafka 不砍舊版"], 2,
  "wire 每 API 的 min 會升（Fetch 4–18）；MV 也有 MINIMUM（≥ 3.3-IV3）。兩條獨立軸。",
  [A("FetchRequest.json",61), A("MetadataVersion.java",147,"MetadataVersion.MINIMUM_VERSION")]),
 ("核心", "replica fetch（broker↔broker）的 Fetch 版本怎麼決定？",
  ["兩 broker ApiVersions 協商取交集", "由 finalized metadata.version 決定", "永遠用最新版", "controller 每次指定"], 1,
  "fetchRequestVersion(MV)：MV 是集中共識，不做 per-connection 協商。",
  [A("RemoteLeaderEndPoint.scala",215), A("MetadataVersion.java",273)]),
 ("核心", "自刻 client 送了不支援的 API version，會怎樣？",
  ["一律回 UNSUPPORTED_VERSION", "只有 ApiVersions 例外回錯誤+範圍；其他 API 關線", "broker 自動降版", "忽略版本照跑"], 1,
  "ApiVersions 是 bootstrap 逃生口；其他 API → broker 丟 UnsupportedVersionException → 斷線。",
  [A("RequestContext.java",112)]),
 ("核心", "metadata.version 是全 cluster 同一個值嗎？",
  ["finalized 全 cluster 一個；supported 範圍 per-node 可不同", "每個 broker 各自一個", "每條連線一個", "controller/broker 各一個"], 0,
  "finalized MV 是 metadata log 一筆 record、複製給所有節點。supported [min,max] 才 per-binary。",
  [T("kafka-features describe")]),
 ("核心", "finalize 某 feature 到某 level，它「不能超過」MV？",
  ["對，feature ≤ MV", "錯——是 dependency：該 level 可能要求 MV ≥ 門檻（下限）", "feature 跟 MV 無關", "只有 controller 決定，與 MV 無關"], 1,
  "Feature.validateVersion：feature level 可依賴 MV ≥ 某值，是下限、不是數值 ≤。",
  [A("Feature.java",163)]),
 ("核心", "fenced 但沒 unregister 的舊 broker，會擋升級嗎？",
  ["不會，fenced 就不算數", "會——檢查所有 registration，不 filter fenced", "只有 controller fenced 才擋", "看還有幾台活著"], 1,
  "brokerSupportedFeatures() 迭代所有 registration、不看 fenced；fence ≠ unregister。（嘉平考點）",
  [A("FeatureControlManager.java",334), A("ClusterControlManager.java",836,"ClusterControlManager.java:836/595")]),
]
# ---- Title ----
s = prs.slides.add_slide(BLANK)
bar = s.shapes.add_shape(1, Inches(0.9), Inches(2.62), Inches(0.1), Inches(1.5))
bar.fill.solid(); bar.fill.fore_color.rgb = ACCENT; bar.line.fill.background()
_, tf = tb(s, 1.2, 1.5, 11.3, 0.5); run(tf.paragraphs[0], "幾萬個節點之上的版本控制 · 第三場", 15, ACCENT, bold=True)
_, tf = tb(s, 1.2, 2.6, 11.3, 1.4); run(tf.paragraphs[0], "溝通當下的版本選擇", 46, DARK, bold=True)
_, tf = tb(s, 1.2, 4.2, 11.3, 0.9); run(tf.paragraphs[0], "長壽的 client、滾動升級的 broker，怎麼喬出共同版本", 20, GRAY)
_, tf = tb(s, 1.2, 6.65, 11.3, 0.5); run(tf.paragraphs[0], "Eric Chang · 2026", 14, MUTED)

add_content("本場大綱", "兩段，加 3 題隨堂考穿插", [
    ("bullet", "Part 1 — 版本為何要在連線當下協商、又是怎麼選的", "point"),
    ("bullet", "Part 2 — 協商失敗時，會看到什麼訊息", "point"),
    ("note", "結尾會用一頁 Recap 把兩段串回開場那個問題。"),
])

add_content("點題", "為什麼不「一個版本打天下」？", [
    ("bullet", "初學者常有的直覺：client 與 broker 使用同一個版本，升級時一併更新", "bulb"),
    ("code", "想像中：  client v4.1 ─────── broker v4.1"),
    ("bullet", "但只要叢集達到一定規模，這個假設就不再成立", "help-circle"),
], [A("protocol.md",94)])

add_content("Part 1 · 地圖", "叢集裡有三種通訊，而且版本天生對不齊", [
    ("code", "client ─▶ broker        讀寫資料、查 metadata…\nbroker ─▶ controller    註冊、心跳、AlterPartition、轉發 admin…\nbroker ◀▶ broker        複製（follower 抓 leader）"),
    ("note", "每列只列代表性的幾種、非窮舉；重點是「就這三條線」。"),
    ("bullet", "client 內嵌在各 app、長期不更新（Kafka 曾為舊 client 保留每個 protocol 版本近九年，4.0／KIP-896 才把下限提到 2.1）", "clock-hour-4"),
    ("bullet", "broker 逐台滾動升級，過程必然新舊並存；要不停機就不能要求全叢集同版", "server-2"),
    ("solve", "所以版本只能在連線當下協商——後面每個機制都掛在上面這三條線"),
], [X("KIP-896",KIP896), A("protocol.md",94), X("KIP-35",KIP35)])

add_content("Part 1 · 三層 scope", "一個版本號不夠：三種 scope 互斥", [
    ("code", "release version   = 我裝了哪版 binary   (per-node)\nmetadata.version  = 叢集一致的能力世代   (cluster-wide, finalize)\nwire API version  = 這條連線講第幾版     (per-connection)"),
    ("bullet", "三層不會同步變動、scope 也不同，多數版本問題的根源都在於此", "arrows-split"),
    ("solve", "單一版本號無法同時表達三種 scope，只能拆成三層各自管"),
], [T("kafka-features describe"), A("zk2kraft.md",71)])

add_content("Part 1 · 選版機制", "回到三個角色：各自怎麼選版", [
    ("bullet", "底層都先做 ApiVersions 握手（共用同一顆 NetworkClient）；決定版本的是 request Builder 留多少空間", "arrows-split"),
    ("code", "client ↔ broker       協商，取交集最高\nbroker ↔ controller   也協商（broker 當 client：heartbeat / registration）\nbroker ↔ broker 複製   Fetch 由 MV 決定、ListOffsets 以 MV 為上限"),
    ("solve", "MV 只影響複製用的 Fetch / ListOffsets；client、controller 那兩條都是協商"),
], [A("NodeApiVersions.java",149,"latestUsableVersion"), A("MetadataVersion.java",273,"fetchRequestVersion"), A("MetadataVersion.java",31,"javadoc")])

# §2a — 數線圖：架構下的一個舉例（cb 協商 vs bb 由 MV）
s2a = prs.slides.add_slide(BLANK)
_, tf = tb(s2a, 0.7, 0.5, CW, 0.4); run(tf.paragraphs[0], "Part 1 · 舉例", 13, ACCENT, bold=True)
_, tf = tb(s2a, 0.7, 0.9, CW, 0.7); run(tf.paragraphs[0], "以一支 Fetch 為例：consumer 協商、replica 由 MV", 29, DARK, bold=True)
_, tf = tb(s2a, 0.7, 1.62, CW, 0.4); run(tf.paragraphs[0], "同一支 Fetch 兩種身分：consumer fetch 走 client↔broker、replica fetch 走 broker↔broker", 14, GRAY)
s2a.shapes.add_picture(ICO + "s2a-rangeline.png", Inches(1.82), Inches(2.15), width=Inches(9.7))
render_ref(s2a, [A("FetchRequest.java",165,"forConsumer/forReplica"), A("MetadataVersion.java",273,"fetchRequestVersion"), A("FetchRequest.json",61,"validVersions")])
quiz_pair(0)  # 小測驗 1：能一版打天下嗎
quiz_pair(2)  # 小測驗 2：replica fetch 版本誰決定

# ---- Part 2：失敗會有什麼訊息 ----
add_content("Part 2 · 失敗訊息", "通訊當下協商不出版本，會看到什麼", [
    ("code", "交集空        → client 端 UnsupportedVersionException（送出前 abort）\n繞過協商自刻  → broker 丟 UnsupportedVersionException + 關線\n（ApiVersions 例外：回 v0 + UNSUPPORTED_VERSION 錯誤碼）"),
    ("bullet", "多數情況 client 在送出前就本地中止，根本沒上網路", "ban"),
], [A("NodeApiVersions.java",149,"latestUsableVersion"), A("NetworkClient.java",591,"NetworkClient"), A("RequestContext.java",112,"RequestContext")])

add_content("Part 2 · 失敗訊息", "版本截斷：為什麼「升一點點」不夠", [
    ("code", "舊 wire API version 被整個移除，min 可 > 0\n  Fetch：4–18（v0–3 在 4.0 移除）"),
    ("solve", "解法：升級前確認兩端都跨過版本下限（升 4.0 需至少 2.1），否則協商結果沒有可用版本"),
    ("note", "銜接（非本場）：finalize / 升降當下的錯誤（INVALID_UPDATE_VERSION 等）屬「運行時的版本升降」那場，本場不展開。"),
], [A("upgrade.md",229), A("FetchRequest.json",61,"FetchRequest.json validVersions")])
quiz_pair(3)  # 小測驗 3：自刻不支援版本會怎樣

add_content("Recap", "回到開場那個問題", [
    ("bullet", "三種通訊：client↔broker、broker↔controller、broker↔broker——版本機制都掛在這三條線", "point"),
    ("bullet", "Part 1：不能「一個版本打天下」（client 長壽、broker 滾動升級）；多數路徑靠協商，只有複製面由 finalized MV 決定", "point"),
    ("bullet", "Part 2：協商不出版本 → UnsupportedVersionException（多在送出前中止）；finalize／升降的錯誤交給運行時那場", "point"),
])

out = os.path.join(HERE, "..", "kafka-version-negotiation.pptx")
prs.save(out)
print("slides:", len(prs.slides._sldIdLst), "->", out)
