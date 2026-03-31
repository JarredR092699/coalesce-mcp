---
name: coalesce-pipeline-builder
description: "Build data transformation pipelines in Coalesce from source tables. Use when: create a pipeline, build a stage node, create a dimension, build a transformation, set up a data model, create a view for analysts, add a CASE WHEN, transform columns, build from a source table, create nodes in Coalesce. Triggers: build a pipeline, create a stage, create a dimension, create a fact, add transformation, build from source table, model this data, create nodes, pipeline from table."
allowed-tools:
  - mcp__coalesce__list_workspace_nodes
  - mcp__coalesce__get_workspace_node
  - mcp__coalesce__create_workspace_node
  - mcp__coalesce__set_node
  - mcp__coalesce__patch_node_field
  - mcp__coalesce__start_run
  - mcp__coalesce__get_run_status
  - mcp__coalesce__get_run_results
---

# Coalesce Pipeline Builder

Build a data transformation pipeline in Coalesce from a source table — creating Stage, Dimension, Fact, or View nodes with SQL transforms, business keys, JOIN conditions, and column logic.

## Workflow

```
Step 1: Collect requirements (workspace ID + source table + pipeline spec)
         ↓
Step 2: Orient — list workspace nodes, inspect an existing node for patterns
         ↓
Step 3: Create the Stage node
         ↓
Step 4: Apply column transforms to the Stage node
         ↓
Step 5: Create downstream nodes (Dimension, Fact, or View)
         ↓
Step 6: Configure downstream nodes (business keys, JOIN conditions, etc.)
         ↓
Step 7 (optional): Run and verify the pipeline
```

---

### Step 1: Collect Requirements

**Goal:** Gather everything needed before touching the API.

**Always ask for the workspace ID first.** It cannot be inferred programmatically.

> "To build the pipeline I'll need your Coalesce workspace ID. You can find it in the URL when you're in the workspace: `.../workspaces/{ID}/build/...`"

**Capture from the user's request:**
- **Source table** — fully qualified name (e.g. `DATABASE.SCHEMA.TABLE_NAME`)
- **Pipeline shape** — which node types to create and in what order (e.g. Source → Stage → Dimension)
- **Transformations** — any column-level SQL logic: CASE WHEN, CAST, UPPER/TRIM, computed columns
- **Business key** — which column uniquely identifies a record (required for Dimension nodes)
- **Node naming convention** — infer from existing nodes (e.g. `STG_`, `DIM_`, `FACT_`) — confirm if unclear

If the user's request is ambiguous about node types, default to: **Source → Stage → Dimension**. This is the most common pattern for HR, customer, and entity data.

**⚠️ STOP if workspace ID is not provided.** Ask before proceeding.

---

### Step 2: Orient — Inspect the Workspace

**Goal:** Find the source node, understand naming conventions, and see how existing nodes are structured so you can match the workspace's patterns.

**2a. List workspace nodes:**

Call `mcp__coalesce__list_workspace_nodes` with the `workspace_id`.

Look for:
- The **source node** matching the user's table name — note its `node_id`
- **Naming conventions** — prefixes like `STG_`, `DIM_`, `FACT_` on existing nodes
- **Existing patterns** — what node types are in use

**If the source node does not exist:**
> "I don't see a source node for `{TABLE_NAME}` in workspace `{workspace_id}`. You may need to add the source in Coalesce first before building on top of it. Let me know once it's added and I'll proceed."
**⚠️ STOP — do not create Stage nodes without a source node to connect them to.**

**2b. Inspect an existing Stage node (if any):**

Pick one Stage node from the list and call `mcp__coalesce__get_workspace_node`. This reveals:
- The column structure and how transforms are applied
- The `locationName` (schema) used in this workspace — you'll need this when creating new nodes
- The `sourceMapping` shape for JOIN patterns (note: the raw API uses `sourceMapping`, not `storageMapping`)

**2c. Inspect an existing Dimension node (if creating a Dimension):**

Same as above — pick one Dimension node to see how business keys and SCD columns are structured.

You now have enough context to proceed.

---

### Step 3: Create the Stage Node

**Goal:** Create a Stage node sourced from the source node.

Call `mcp__coalesce__create_workspace_node`:
- `workspace_id` — from Step 1
- `node_type` — `"Stage"`
- `predecessor_node_ids` — `[source_node_id]` from Step 2

The API returns the created node including its generated `node_id`. Save this as `stage_node_id`.

**Naming:** Coalesce assigns a default name. The user may want to rename it — you can do this via `patch_node_field` on `"name"` or via `set_node`. Match the naming convention observed in Step 2 (e.g. `STG_EMPLOYEE_HR_DATA_25`).

**Note the column indices** from the created node response — you'll need them for Step 4. Columns are 0-indexed in `metadata.columns[]`.

---

### Step 4: Apply Column Transforms to the Stage Node

**Goal:** Apply all SQL transforms the user requested to the Stage node's columns.

**For each transformation, call `mcp__coalesce__patch_node_field`:**

| Transform type | field_path | new_value example |
|---|---|---|
| CASE WHEN | `metadata.columns[N].sources[0].transform` | `CASE WHEN "TABLE"."COL" = 'X' THEN 'Y' ELSE "TABLE"."COL" END` |
| CAST | `metadata.columns[N].sources[0].transform` | `CAST("TABLE"."HIRE_DATE" AS DATE)` |
| UPPER/TRIM | `metadata.columns[N].sources[0].transform` | `UPPER(TRIM("TABLE"."STATUS"))` |
| WHERE clause | `config.whereClause` | `STATUS != 'TEST'` |
| Pre/Post SQL | `config.preSQL` / `config.postSQL` | Any SQL string |

**Finding the right column index N:**
- After `create_workspace_node` returns, inspect `metadata.columns[]` in the response
- Count from 0 — e.g. `EMPLOYEE_ID` at index 0, `FIRST_NAME` at index 1, `HIRE_DATE` at index 6
- If you're unsure of the index, call `mcp__coalesce__get_workspace_node` to confirm before patching

**Important — transform SQL syntax:**
- Reference source columns with the **table alias** used in the node: `"TABLE_NAME"."COLUMN_NAME"` (double-quoted identifiers)
- The table alias is typically the source table name without schema prefix
- Match the quoting style seen in existing nodes (from Step 2b inspection)

**Run patches in parallel when possible** — multiple independent column transforms can be fired simultaneously for efficiency.

**After applying all transforms, call `mcp__coalesce__get_workspace_node`** to verify the Stage node looks correct before proceeding to downstream nodes. Check that:
- The CASE WHEN / CAST / other transforms are present on the right columns
- The column names and types look right

**⚠️ Verify before continuing.** If the user questions whether a transform was applied, always fetch and show the current node state — don't rely on memory of what was patched.

---

### Step 5: Create Downstream Nodes

**Goal:** Create the Dimension, Fact, or View node(s) downstream of the Stage.

Call `mcp__coalesce__create_workspace_node`:
- `workspace_id` — same as before
- `node_type` — `"Dimension"`, `"Fact"`, or `"View"` (or custom type if the user specifies)
- `predecessor_node_ids` — `[stage_node_id]` from Step 3

Save the returned `node_id` as `dim_node_id` (or `fact_node_id`, etc.).

**Node type guidance:**

| User asks for... | Node type to use | Notes |
|---|---|---|
| "a view for analysts" | `"Dimension"` (default) or check if `"View"` exists | Coalesce's Dimension type includes SCD2 tracking — often more useful for reporting than a plain view |
| "a dimension" | `"Dimension"` | Standard SCD2 dimension with surrogate key |
| "a fact table" | `"Fact"` | Append-only fact table |
| "a plain view" | Check workspace for a `"View"` node type | Not all workspaces have a plain View type — check existing nodes first |

**If the user asked for a "view" but only Dimension is available**, explain:
> "Coalesce created a Dimension node — this gives analysts SCD2 history tracking (version, current flag, start/end dates) which is more useful for HR/entity reporting than a plain view. If you'd prefer a plain view materialization, let me know."

---

### Step 5b (Star Schema): Build All Dimensions Before the Fact

**⚠️ If the user asked for a star schema (one Fact + multiple Dimensions), follow this section instead of proceeding directly to Step 6.**

A star schema requires that **every dimension is fully built and configured before the Fact node is created.** The Fact node must list the fact Stage node plus all **Dimension nodes** as predecessors so it can reference their surrogate keys.

**The key principle:** Each Dimension node auto-generates a surrogate key column (e.g. `DIM_CUSTOMER_KEY`). The Fact table must carry a foreign key column for each dimension that matches this surrogate key — this is how the tables join at query time.

**Star schema build order:**

```
For each entity (customer, product, date, etc.):
  1. Create a Source → Stage → Dimension chain (Steps 3–6a for that entity)
  2. Note the Dimension's surrogate key column name (e.g. DIM_CUSTOMER_KEY)
  3. Note the matching natural key column on the fact source (e.g. CUSTOMER_ID)

After ALL dimensions are built:
  4. Create the Fact Stage node (sourced from the fact source table)
  5. Create the Fact node with predecessor_node_ids = [fact_stage_id, dim1_node_id, dim2_node_id, ...]
  6. ⚠️ Audit the Fact node for duplicate column names — drop or rename any duplicates before continuing
  7. Add surrogate foreign key columns to the Fact node for each dimension
  8. Configure JOIN conditions on the Fact node to link each dimension
```

**5b-i. Identify the surrogate key name for each dimension:**

After creating each Dimension node, call `mcp__coalesce__get_workspace_node` and inspect `metadata.columns[0]` — this is always the auto-generated surrogate key. Its name follows the pattern `{DIM_NODE_NAME}_KEY` (e.g. `DIM_CUSTOMER_KEY`).

Record a mapping for each dimension:
```
DIM_CUSTOMER  → surrogate key: DIM_CUSTOMER_KEY  ← matched by: CUSTOMER_ID (on fact source)
DIM_PRODUCT   → surrogate key: DIM_PRODUCT_KEY   ← matched by: PRODUCT_ID (on fact source)
DIM_DATE      → surrogate key: DIM_DATE_KEY       ← matched by: ORDER_DATE (on fact source)
```

**5b-ii. Create the Fact node with the fact Stage plus all Dimension nodes as predecessors:**

When creating the Fact node, include the Fact Stage AND every **Dimension node** (not Stage) as predecessors:

```
predecessor_node_ids: [fact_stage_id, dim_customer_node_id, dim_product_node_id, dim_date_node_id]
```

This wires the Fact node to pull columns from the fact's own stage (for measures/natural keys) and from each Dimension node (for surrogate keys).

**5b-iii. Deduplicate columns on the Fact node:**

**⚠️ CRITICAL — resolve duplicate column names before configuring the Fact node.**

When the Fact node has multiple predecessors (a Stage + multiple Dimension nodes), Coalesce pulls all columns from every predecessor into the Fact node by default. This will create **duplicate column names** — for example, both `STG_ORDERS` and `DIM_CUSTOMERS` will each have a `CUSTOMER_ID` column.

**After creating the Fact node, immediately call `mcp__coalesce__get_workspace_node` and audit the full `metadata.columns[]` list for duplicates.**

For every duplicate column name found, you must do one of the following:

**Option A — Drop it from the Fact (preferred for natural/business keys that already exist on the fact stage):**
- The Fact node should only carry measures and surrogate foreign keys — do not carry natural key columns from dimension nodes
- Remove the dimension's natural key column from the Fact by setting `metadata.columns[N].exclude` to `true` (or equivalent Coalesce exclusion mechanism)
- If Coalesce does not support exclusion via patch, rename it instead (Option B)

**Option B — Rename with a table suffix to make it unambiguous:**
- `field_path`: `"metadata.columns[N].name"` → `"{ORIGINAL_NAME}_{TABLE_SUFFIX}"`
- Example: a second `CUSTOMER_ID` coming from `DIM_CUSTOMERS` → rename to `CUSTOMER_ID_DIM`

**The Fact node's column set should contain only:**
1. Surrogate foreign keys (one per dimension, e.g. `DIM_CUSTOMER_KEY`, `DIM_PRODUCT_KEY`)
2. Measures from the fact stage (e.g. `QUANTITY`, `UNIT_PRICE`, `DISCOUNT_AMOUNT`, `GROSS_REVENUE`)
3. Degenerate dimensions from the fact stage (e.g. `ORDER_ID`, `ORDER_DATE`)
4. **No descriptive attributes from dimension tables** — those belong in the dimension tables and are accessed via JOIN at query time

**Add foreign key columns to the Fact node:**

The Fact node needs one foreign key column per dimension. These columns hold the surrogate key value from each dimension so records can be joined.

For each dimension, call `mcp__coalesce__patch_node_field` to add a foreign key column to the Fact node's `metadata.columns[]`. The column's transform must reference the dimension's surrogate key using the dimension node's alias:

Example — adding a customer foreign key column:
- `field_path`: `"metadata.columns[N].name"` → `"DIM_CUSTOMER_KEY"`
- `field_path`: `"metadata.columns[N].sources[0].transform"` → `"DIM_CUSTOMER"."DIM_CUSTOMER_KEY"`

**The transform must reference the dimension's surrogate key column using the dimension node's alias** — not the raw natural key from the fact source. The JOIN (configured next) is what resolves dimension records.

**5b-iv. Configure JOIN conditions on the Fact node:**

For each dimension predecessor, set the JOIN ON condition in `sourceMapping`. This tells Coalesce how to join the dimension to the fact.

**IMPORTANT — correct `patch_node_field` path:** The raw API uses `sourceMapping` (not `storageMapping`), and `join` is an object with a `joinCondition` property. The correct field path is:

- `field_path`: `"metadata.sourceMapping[N].join.joinCondition"`
- `new_value`: `"STG_ORDERS"."CUSTOMER_ID" = "DIM_CUSTOMER"."CUSTOMER_ID"`

Inspect the Fact node first (`mcp__coalesce__get_workspace_node`) to determine the correct `sourceMapping` index `[N]` for each dimension predecessor. Each predecessor has its own entry in the `sourceMapping` array.

Where:
- Left side = the natural key column on the fact stage (e.g. `"STG_ORDERS"."CUSTOMER_ID"`)
- Right side = the matching natural/business key column on the dimension node (e.g. `"DIM_CUSTOMER"."CUSTOMER_ID"`)

**Example — complete star schema wiring for a sales fact:**

```
DIM_CUSTOMER joined on: "STG_ORDERS"."CUSTOMER_ID" = "DIM_CUSTOMER"."CUSTOMER_ID"
DIM_PRODUCT  joined on: "STG_ORDERS"."PRODUCT_ID"  = "DIM_PRODUCT"."PRODUCT_ID"
DIM_DATE     joined on: "STG_ORDERS"."ORDER_DATE"   = "DIM_DATE"."DATE_KEY"
```

**⚠️ Common mistake:** Creating the Fact node before the dimensions are built, or setting `predecessor_node_ids` to only the fact stage. The Fact node must list all **Dimension nodes** as predecessors — otherwise the foreign key columns have no source to draw from and the JOINs will fail.

---

### Step 6: Configure Downstream Nodes

**Goal:** Set the business key and any other required configuration on the downstream node.

**6a. Set the business key (required for Dimension nodes):**

The business key is the column that uniquely identifies a record (e.g. `EMPLOYEE_ID`, `CUSTOMER_ID`, `ORDER_ID`).

- Fetch the downstream node: `mcp__coalesce__get_workspace_node`
- Find the business key column's index in `metadata.columns[]`
  - Note: index 0 is typically the auto-generated surrogate key — the natural key column is usually at index 1
- Call `mcp__coalesce__patch_node_field`:
  - `field_path`: `"metadata.columns[N].isBusinessKey"`
  - `new_value`: `"true"`

**6b. Set JOIN conditions (if applicable):**

If the downstream node joins multiple sources, configure the JOIN condition:
- `field_path`: `"metadata.sourceMapping[N].join.joinCondition"`
- `new_value`: the JOIN ON condition SQL

**6c. Add WHERE clause (if applicable):**
- `field_path`: `"config.whereClause"`
- `new_value`: the WHERE condition SQL

---

### Step 7 (Optional): Run and Verify

**Goal:** Deploy and test the pipeline to confirm it executes cleanly.

**⚠️ STOP:** Ask the user if they want to run the pipeline before triggering anything.

> "The pipeline is built. Would you like me to kick off a run to test it?"

**If yes:**

1. Call `mcp__coalesce__start_run` with the `environment_id` (ask user if not already known)
2. Note the `run_id` from the response
3. Poll `mcp__coalesce__get_run_status` with `run_id` until `runStatus` is `"complete"` or `"failed"`
   - Inform the user of progress as you poll
4. Call `mcp__coalesce__get_run_results` to confirm all nodes passed

**If the run fails:**
> "The run failed. I can investigate the failure — would you like me to diagnose it?"
Then follow the Coalesce Job Failure Investigation skill workflow.

---

## Pipeline Summary

At the end of the workflow, always present a structured summary:

**Single-entity pipeline:**
```
Pipeline: {SOURCE_TABLE_BASE_NAME}

  {SOURCE_NODE_NAME} (Source)
      → {STAGE_NODE_NAME} (Stage)
          → {DIM/FACT_NODE_NAME} (Dimension/Fact)

Stage node — {STAGE_NODE_NAME}
• {COLUMN}: {transform description}
• {COLUMN}: {transform description}
• All other columns pass through from the source

Dimension node — {DIM_NODE_NAME}
• Sources from the Stage node
• {COLUMN} set as the business key
• Auto-generated surrogate key: {DIM_NAME}_KEY
• SCD tracking columns: SYSTEM_VERSION, SYSTEM_CURRENT_FLAG, SYSTEM_START_DATE, SYSTEM_END_DATE
```

**Star schema pipeline:**
```
Star Schema: {SUBJECT_AREA}

Dimensions:
  {SRC_CUSTOMER} (Source) → STG_CUSTOMER (Stage) → DIM_CUSTOMER (Dimension)
    • Business key: CUSTOMER_ID
    • Surrogate key: DIM_CUSTOMER_KEY

  {SRC_PRODUCT} (Source) → STG_PRODUCT (Stage) → DIM_PRODUCT (Dimension)
    • Business key: PRODUCT_ID
    • Surrogate key: DIM_PRODUCT_KEY

  {SRC_DATE} (Source) → STG_DATE (Stage) → DIM_DATE (Dimension)
    • Business key: DATE_KEY
    • Surrogate key: DIM_DATE_KEY

Fact:
  {SRC_ORDERS} (Source)
      → STG_ORDERS (Stage)
          → FACT_ORDERS (Fact)
              predecessors: STG_ORDERS + DIM_CUSTOMER + DIM_PRODUCT + DIM_DATE

Fact foreign keys (how dimensions connect):
  DIM_CUSTOMER_KEY ← JOIN ON STG_ORDERS.CUSTOMER_ID = DIM_CUSTOMER.DIM_CUSTOMER_KEY
  DIM_PRODUCT_KEY  ← JOIN ON STG_ORDERS.PRODUCT_ID  = DIM_PRODUCT.DIM_PRODUCT_KEY
  DIM_DATE_KEY     ← JOIN ON STG_ORDERS.ORDER_DATE   = DIM_DATE.DIM_DATE_KEY
```

---

## Tools Reference

### mcp__coalesce__list_workspace_nodes
**When:** Orient — find the source node and understand existing workspace patterns.
**Params:** `workspace_id` (required)

### mcp__coalesce__get_workspace_node
**When:** Inspect an existing node's structure before creating similar ones, or verify a node after patching.
**Params:** `workspace_id` (required), `node_id` (required)

### mcp__coalesce__create_workspace_node
**When:** Create Stage, Dimension, Fact, or View nodes.
**Params:** `workspace_id` (required), `node_type` (required — `"Stage"`, `"Dimension"`, `"Fact"`, or custom), `predecessor_node_ids` (required — use `[]` for source nodes, `[parent_id]` for downstream)
**Returns:** Created node object including generated `node_id`, `name`, and `metadata.columns[]`

### mcp__coalesce__patch_node_field
**When:** Apply a targeted SQL transform, set a business key flag, update a WHERE clause, or any single-field change. Preferred over `set_node` for targeted updates — handles fetch-modify-replace internally.
**Params:** `workspace_id`, `node_id`, `field_path` (dot/bracket path), `new_value`
**Returns:** `{success, node_name, field_path, old_value, new_value}`

### mcp__coalesce__set_node
**When:** Full node replacement — use only when `patch_node_field` is insufficient (e.g. restructuring multiple fields at once). Always call `get_workspace_node` first to fetch the current config.
**Params:** `workspace_id`, `node_id`, `node_config_json` (complete node object as JSON string)

### mcp__coalesce__start_run
**When:** Trigger a full environment run to test the pipeline.
**Params:** `environment_id` (required), `job_id` (optional), `parallelism` (optional)
**Returns:** `{success, run_id}`

### mcp__coalesce__get_run_status
**When:** Poll run progress after calling `start_run`.
**Params:** `run_id` (required)

### mcp__coalesce__get_run_results
**When:** Confirm all nodes passed after a run completes.
**Params:** `run_id` (required)

---

## Common Mistakes to Avoid

| Mistake | What to do instead |
|---|---|
| Creating nodes without a source node | Always verify the source node exists in Step 2 before creating |
| Wrong column index in `field_path` | Call `get_workspace_node` to confirm indices before patching |
| Using wrong table alias in transform SQL | Inspect an existing Stage node in Step 2b — match its quoting and alias style |
| Creating a Dimension without setting a business key | Always set `isBusinessKey` on the natural key column (Step 6a) |
| Proceeding without workspace ID | Ask the user — it cannot be inferred from any API response |
| Assuming a transform was applied without verifying | Always call `get_workspace_node` after patching to confirm before moving on |
| Triggering a run without asking | Always ask the user before calling `start_run` |
| **Star schema: creating Fact before all Dimensions** | Build and configure every Dimension first — the Fact node needs their surrogate keys to exist |
| **Star schema: Fact predecessor_node_ids missing dimensions** | Always include all **Dimension node IDs** (not Stage IDs) in the Fact node's predecessor list, not just the Fact Stage |
| **Star schema: using `metadata.storageMapping[N].join` for JOIN path** | Use `metadata.sourceMapping[N].join.joinCondition` — the raw API key is `sourceMapping` and `join` is an object with a `joinCondition` property |
| **Star schema: using natural keys as FK columns on Fact** | Fact foreign key columns must reference the Dimension's surrogate key (e.g. `DIM_CUSTOMER_KEY`), not the raw natural key |
| **Star schema: JOIN references wrong side of the key** | LEFT side of JOIN = natural key on fact stage, RIGHT side = surrogate key on dimension |
| **Star schema: duplicate column names on Fact node** | After creating the Fact node, audit all columns for duplicates (e.g. `CUSTOMER_ID` from both stage and dimension). Either drop the dimension's copy or rename it with a `_DIM` suffix. Never leave two columns with the same name — Coalesce will error at compile time. |
| **Star schema: carrying descriptive attributes on Fact** | Fact nodes should only contain surrogate FKs, measures, and degenerate dimensions. Do not bring `FIRST_NAME`, `PRODUCT_NAME`, etc. onto the Fact — those live on the dimension tables. |

---

## Stopping Points

- **Step 1:** If workspace ID is not provided, stop and ask
- **Step 2:** If source node is not found, stop and inform the user
- **Step 4:** After patching transforms, verify with `get_workspace_node` before proceeding
- **Step 4:** If the user questions whether a transform was applied, fetch and show current node state
- **Step 7:** Ask the user before triggering any run
