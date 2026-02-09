// index.js (CommonJS)
const express = require("express");
const cors = require("cors");
const { BigQuery } = require("@google-cloud/bigquery");

const app = express();
app.use(express.json());

// CORS (dev + prod)
const ALLOWED_ORIGINS = [
  "http://localhost:5173",
  "http://127.0.0.1:5173",
  // "https://seu-front.com" // quando publicar
];

app.use(cors({
  origin: ALLOWED_ORIGINS,
  methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"],
}));

// garante preflight
app.options("*", cors());

// BigQuery client (Cloud Run SA)
const PROJECT_ID =
  process.env.BQ_PROJECT ||
  process.env.GOOGLE_CLOUD_PROJECT ||
  "looker-viz-484818";

const bq = new BigQuery({ projectId: PROJECT_ID });

// Helpers
function parseDays(v, fallback = 30) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(1, Math.min(365, Math.floor(n)));
}
function getClientId(v, fallback = "demo") {
  const s = String(v || "").trim();
  return s || fallback;
}

const PROJECT_ID = process.env.BQ_PROJECT || "looker-viz-484818";
const DATASET = process.env.BQ_DATASET || "ussouth1";
const LOCATION = process.env.BQ_LOCATION || "US";

const T_FACT_KPIS = `${PROJECT_ID}.${DATASET}.fact_kpis_daily`;
const T_FACT_FUNNEL = `${PROJECT_ID}.${DATASET}.fact_funnel_daily`;
const T_FACT_HEALTH = `${PROJECT_ID}.${DATASET}.fact_data_health_daily`;

// Root
app.get("/", (req, res) => res.status(200).json({ ok: true, message: "API online" }));

// Health
app.get("/__health", (req, res) => res.status(200).send("ok"));

// Echo
app.post("/echo", (req, res) => res.status(200).json({ received: req.body }));

// BigQuery test
app.get("/bq-test", async (req, res) => {
  try {
    const [job] = await bq.createQueryJob({ query: "SELECT 1 AS ok", location: LOCATION });
    const [rows] = await job.getQueryResults();
    res.status(200).json({ ok: true, rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

/**
 * KPIs agregados (totais + conversões) por janela de dias
 * GET /kpis?client_id=demo&days=30
 */
app.get("/kpis", async (req, res) => {
  const client_id = getClientId(req.query.client_id, "demo");
  const days = parseDays(req.query.days, 30);

  try {
    const query = `
      WITH base AS (
        SELECT *
        FROM \`${T_FACT_KPIS}\`
        WHERE client_id = @client_id
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
      )
      SELECT
        @client_id AS client_id,
        @days AS days,
        COUNT(*) AS rows,
        SUM(leads) AS leads,
        SUM(mql) AS mql,
        SUM(sql) AS sql,
        SUM(deals_total) AS deals_total,
        SUM(deals_won) AS deals_won,
        SUM(revenue) AS revenue,
        SAFE_DIVIDE(SUM(mql), NULLIF(SUM(leads), 0)) AS cr_lead_to_mql,
        SAFE_DIVIDE(SUM(sql), NULLIF(SUM(mql), 0)) AS cr_mql_to_sql,
        SAFE_DIVIDE(SUM(deals_won), NULLIF(SUM(sql), 0)) AS cr_sql_to_won
      FROM base
    `;

    const [job] = await bq.createQueryJob({
      query,
      location: LOCATION,
      params: { client_id, days },
    });
    const [rows] = await job.getQueryResults();

    res.json({ ok: true, data: rows[0] || { client_id, days } });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

/**
 * Série diária (pra gráficos)
 * GET /kpis/series?client_id=demo&days=30
 */
app.get("/kpis/series", async (req, res) => {
  const client_id = getClientId(req.query.client_id, "demo");
  const days = parseDays(req.query.days, 30);

  try {
    const query = `
      SELECT
        client_id,
        date,
        demand_index,
        leads,
        mql,
        sql,
        deals_total,
        deals_won,
        revenue,
        cr_lead_to_mql,
        cr_mql_to_sql,
        cr_sql_to_won
      FROM \`${T_FACT_KPIS}\`
      WHERE client_id = @client_id
        AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
      ORDER BY date ASC
    `;

    const [job] = await bq.createQueryJob({
      query,
      location: LOCATION,
      params: { client_id, days },
    });
    const [rows] = await job.getQueryResults();

    res.json({ ok: true, rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

/**
 * Funil agregado (pra HorizontalFunnel)
 * GET /funnel?client_id=demo&days=30
 */
app.get("/funnel", async (req, res) => {
  const client_id = getClientId(req.query.client_id, "demo");
  const days = parseDays(req.query.days, 30);

  try {
    // usando fact_funnel_daily (tem leads/mql/sql/deals_won e win_rate_over_sql)
    const query = `
      WITH base AS (
        SELECT *
        FROM \`${T_FACT_FUNNEL}\`
        WHERE client_id = @client_id
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
      )
      SELECT
        @client_id AS client_id,
        @days AS days,
        SUM(leads) AS leads,
        SUM(mql) AS mql,
        SUM(sql) AS sql,
        SUM(deals_won) AS deals_won,
        SUM(revenue) AS revenue,
        SAFE_DIVIDE(SUM(mql), NULLIF(SUM(leads), 0)) AS cr_lead_to_mql,
        SAFE_DIVIDE(SUM(sql), NULLIF(SUM(mql), 0)) AS cr_mql_to_sql,
        SAFE_DIVIDE(SUM(deals_won), NULLIF(SUM(sql), 0)) AS cr_sql_to_won
      FROM base
    `;

    const [job] = await bq.createQueryJob({
      query,
      location: LOCATION,
      params: { client_id, days },
    });
    const [rows] = await job.getQueryResults();
    const d = rows[0] || {};

    // payload já no formato "etapas" (fácil plugar no frontend)
    const funnel = [
      { key: "leads", label: "Leads", value: Number(d.leads || 0) },
      { key: "mql", label: "MQL", value: Number(d.mql || 0), rate: Number(d.cr_lead_to_mql || 0) },
      { key: "sql", label: "SQL", value: Number(d.sql || 0), rate: Number(d.cr_mql_to_sql || 0) },
      { key: "won", label: "Won", value: Number(d.deals_won || 0), rate: Number(d.cr_sql_to_won || 0) },
    ];

    res.json({ ok: true, data: { client_id, days, ...d }, funnel });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

/**
 * Data Health
 * GET /data-health?client_id=demo&days=30
 */
app.get("/data-health", async (req, res) => {
  const client_id = getClientId(req.query.client_id, "demo");
  const days = parseDays(req.query.days, 30);

  try {
    const query = `
      SELECT
        client_id,
        date,
        table_name,
        rows_count,
        last_ingested_at,
        pct_missing_client_id,
        pct_missing_utm_source,
        pct_missing_lead_id,
        pct_missing_created_at,
        pct_tracking_valid,
        pct_missing_event_ts,
        pct_missing_event_name,
        pct_missing_deal_id,
        pct_missing_closed_at
      FROM \`${T_FACT_HEALTH}\`
      WHERE client_id = @client_id
        AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
      ORDER BY date DESC, table_name ASC
    `;

    const [job] = await bq.createQueryJob({
      query,
      location: LOCATION,
      params: { client_id, days },
    });
    const [rows] = await job.getQueryResults();

    res.json({ ok: true, rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// 404
app.use((req, res) => res.status(404).json({ error: `Not found: ${req.method} ${req.path}` }));

// Listen
const port = process.env.PORT || 8080;
app.listen(port, "0.0.0.0", () => console.log(`Listening on ${port}`));
