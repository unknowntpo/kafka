# Where to Find Tags in Azul Mission Control

## The Confusion: Attributes vs Tags

### What You're Seeing

When you click on a metric in Azul Mission Control, you see **attributes**:

```
Attributes:
  commit-id: baec23112c    ← This is a METRIC VALUE
  state: RUNNING           ← This is a METRIC VALUE
  version: 4.2.0-SNAPSHOT  ← This is a METRIC VALUE
```

**These are NOT tags!** These are the actual metric values.

## Where Tags Actually Are

### Tags are in the ObjectName!

Look for the **ObjectName** or **MBean Info** section:

```
ObjectName: kafka.streams:type=stream-metrics,
            application-id=test-metrics-app,  ← TAG!
            client-id=test-metrics-...,       ← TAG!
            process-id=...                    ← TAG!
```

## How to See Tags in Azul Mission Control

### Method 1: Look at the Tree Structure

In the left MBean tree, the full path shows the tags:

```
kafka.streams
└── stream-metrics
    └── type=stream-metrics,application-id=test-metrics-app,client-id=...,process-id=...
        ├── commit-id        ← Metric
        ├── state            ← Metric
        └── version          ← Metric
```

The long line with commas (`type=...,application-id=...,client-id=...,process-id=...`) contains the **TAGS**.

### Method 2: Check the Details/Info Panel

After clicking a metric, look for these sections in the right panel:

1. **Info** tab or **Details** section
2. Find: **"ObjectName:"** or **"MBean Name:"**
3. The text after the colon shows: `kafka.streams:type=...,application-id=...,client-id=...,process-id=...`

The key-value pairs after the colon (`:`) are the **tags**!

## Visual Guide

```
┌─────────────────────────────────────────────────────────────────┐
│ Azul Mission Control - MBean Browser                            │
├───────────────────┬─────────────────────────────────────────────┤
│ MBean Tree        │ Selected: commit-id                         │
│                   │                                             │
│ ▼ kafka.streams   │ ┌─ Info Tab ────────────────────────────┐ │
│   ▼ stream-metrics│ │                                        │ │
│     ▼ type=stream-│ │ ObjectName:                            │ │
│       metrics,    │ │   kafka.streams:type=stream-metrics,   │ │
│       application-│ │   application-id=test-metrics-app, ←TAG│ │
│       id=test-... │ │   client-id=test-metrics-app-...,  ←TAG│ │
│       ↑           │ │   process-id=bf586c12-...          ←TAG│ │
│       │           │ │                                        │ │
│     THESE         │ └────────────────────────────────────────┘ │
│     ARE           │                                             │
│     TAGS!         │ ┌─ Attributes Tab ──────────────────────┐ │
│                   │ │                                        │ │
│     ▼ commit-id   │ │ Name          Value                   │ │
│     • state       │ │ ───────────────────────────────────── │ │
│     • version     │ │ commit-id     baec23112c   ←METRIC    │ │
│       ↑           │ │                                        │ │
│       │           │ └────────────────────────────────────────┘ │
│     THESE ARE     │                                             │
│     METRICS!      │                                             │
└───────────────────┴─────────────────────────────────────────────┘
```

## Key Differences

| Feature | Location | Example |
|---------|----------|---------|
| **Tags** | ObjectName (after the colon `:`) | `application-id=test-metrics-app` |
| **Metrics** | Attributes panel | `commit-id: baec23112c` |

### Tags (Labels/Properties)
- Part of the **MBean identity**
- In the **ObjectName**
- Format: `key=value` pairs separated by commas
- Used to **categorize** metrics
- Example: `application-id=test-metrics-app`

### Metrics (Attributes/Values)
- The **actual measurements**
- In the **Attributes** panel
- Format: Name → Value
- What you're **monitoring**
- Example: `state: RUNNING`

## What You're Verifying

For this PR, you need to verify that the **ObjectName** contains:

```
application-id=test-metrics-app
```

This should appear in **EVERY** metric under `kafka.streams → stream-metrics`.

## Step-by-Step in Azul Mission Control

1. **Expand the tree**:
   ```
   kafka.streams → stream-metrics → (click the long name with commas)
   ```

2. **Read the node name** in the tree:
   - Should contain: `application-id=test-metrics-app`

3. **Click any metric** (e.g., `state` or `commit-id`)

4. **Find the Info/Details tab** in the right panel

5. **Look for "ObjectName:"**
   - Should show: `kafka.streams:type=stream-metrics,application-id=test-metrics-app,...`

6. **Verify** all three tags are present:
   - ✓ `application-id=test-metrics-app`
   - ✓ `client-id=test-metrics-app-...`
   - ✓ `process-id=...`

## Screenshot to Take

To document the verification, take a screenshot showing:

1. The **MBean tree** with the full node name expanded
2. The **ObjectName** in the Info/Details panel
3. Make sure `application-id=test-metrics-app` is **visible** in the ObjectName

Example text to highlight:
```
kafka.streams:type=stream-metrics,
              application-id=test-metrics-app,  ← Highlight this!
              client-id=test-metrics-app-bf586c12-bf8b-4a0e-bf4a-98ef8abf00ac,
              process-id=bf586c12-bf8b-4a0e-bf4a-98ef8abf00ac
```

## Common Mistake

❌ **Wrong**: Looking at the Attributes tab for tags
```
Attributes:
  application-id: test-metrics-app   ← This is also a metric! (confusing!)
```

✓ **Correct**: Looking at the ObjectName
```
ObjectName: kafka.streams:type=stream-metrics,application-id=test-metrics-app,...
```

Note: There IS a metric called `application-id` (confusing!), but the **TAG** `application-id` is in the ObjectName, not in attributes.

## Summary

- **Tags** = Properties in the ObjectName = `application-id=test-metrics-app`
- **Metrics** = Attributes/Values = `commit-id: baec23112c`
- **What to verify**: ObjectName contains `application-id=test-metrics-app`
