// index.js — RevOps API (Cloud Run) + BigQuery + CORS OK
// ------------------------------------------------------------
// Endpoints principais:
//   GET  /                 -> { ok, message }
//   GET  /healthz          -> redirect 308 -> /healthz/
//   GET  /healthz/         -> { ok, version }
//   GET  /__health         -> "ok" (compat legado)
//   GET  /bq-test          -> teste BigQuery
//   GET  /kpis             -> agregado fact_kpis_daily
//   GET  /kpis/series      -> série fact_kpis_daily
//   GET  /funnel           -> agregado + série fact_funnel_daily
//   GET  /data-health      -> qualidade fact_data_health_daily
//
// CORS:
//   libera http://localhost:5173 e http://127.0.0.1:5173
//   responde OPTIONS (preflight) com 204
// ------------------------------------------------------------

const express = require("express");
const { BigQuery } = require("@google-cloud/bigquery");

const app = express();
app.use(express.json());

// =====================
// CORS (DEV + whitelist)
// =====================
const allowedOrigins = [
  "http://localhost:5173",
  "http://127.0.0.1:5173",
  // adicione seu domínio do front quando publicar:
  // "https://seu-dominio.com",
];

app.use((req, res, next) => {
  const origin = req.headers.origin;

  // Libera somente origens permitidas (quando há Origin)
  if (origin && allowedOrigins.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  }

  res.setHeader(
    "Access-Control-Allow-Methods",
    "GET,POST,PUT,PATCH,DELETE,OPTIONS"
  );
  res.setHeader(
    "Access-Control-Allow-Headers",
    "Content-Type, Authorization"
  );

  // Preflight
  if (req.method === "OPTIONS") return res.status(204).end();

  next();
});

// ---------------------------
// Versão (debug de deploy)
// ---------------------------
const VERSION = "cors-fix-v2";

// ------------------------------------------------------------
// Config padrão (MVP)
// ------------------------------------------------------------
const PROJECT_ID =
  process.env.BQ_PROJECT ||
  process.env.GOOGLE_CLOUD_PROJECT ||
  process.env.GCLOUD_PROJECT ||
  "looker-viz-484818";

const DATASET = process.env.BQ_DATASET || "ussouth1";

// Tabelas/views que você já tem no dataset
const T_FACT_KPIS = process.env.BQ_TABLE_KPIS || "fact_kpis_daily";
const T_FACT_FUNNEL = process.env.BQ_TABLE_FUNNEL || "fact_funnel_daily";
const T_FACT_HEALTH = process.env.BQ_TABLE_HEALTH || "fact_data_health_daily";

// BigQuery client
const bq = new BigQuery({ projectId: PROJECT_ID });
const BQ_LOCATION = process.env.BQ_LOCATION || "US";

// Helpers
function asInt(v, def) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : def;
}
function asStr(v, def) {
  const s = (v ?? "").toString().trim();
  return s || def;
}
function tableRef(projectId, dataset, table) {
  return `\`${projectId}.${dataset}.${table}\``;
}

// ------------------------------------------------------------
// Rotas
// ------------------------------------------------------------

// Root (use pra checar se o serviço está no ar)
app.get("/", (req, res) =>
  res.status(200).json({ ok: true, message: "API online", version: VERSION })
);

// Health legado
app.get("/__health", (req, res) => res.status(200).send("ok"));

// Health novo: sem barra -> redireciona pra com barra
app.get("/healthz", (req, res) => res.redirect(308, "/healthz/"));

// Health novo: com barra
app.get("/healthz/", (req, res) => {
  res.setHeader("X-REVOPS-VERSION", VERSION);
  res.status(200).json({ ok: true, version: VERSION });
});

// BigQuery connectivity test
app.get("/bq-test", async (req, res) => {
  try {
    const [job] = await bq.createQueryJob({
      query: "SELECT 1 AS ok",
      location: BQ_LOCATION,
    });
    const [rows] = await job.getQueryResults();
    res.status(200).json({
      ok: true,
      rows,
      projectId: PROJECT_ID,
      dataset: DATASET,
      location: BQ_LOCATION,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

/**
 * GET /kpis?client_id=demo&days=30
 * Retorna agregado simples (últimos N dias) da fact_kpis_daily
 */
app.get("/kpis", async (req, res) => {
  const client_id = asStr(req.query.client_id, "demo");
  const days = asInt(req.query.days, 30);

  try {
    const ref = tableRef(PROJECT_ID, DATASET, T_FACT_KPIS);

    const query = `
      SELECT
        ANY_VALUE(client_id) AS client_id,
        COUNT(*) AS days_count,
        SUM(leads) AS leads,
        SUM(mql) AS mql,
        SUM(sql) AS sql,
        SUM(deals_total) AS deals_total,
        SUM(deals_won) AS deals_won,
        SUM(revenue) AS revenue,
        SAFE_DIVIDE(SUM(mql), NULLIF(SUM(leads),0)) AS cr_lead_to_mql,
        SAFE_DIVIDE(SUM(sql), NULLIF(SUM(mql),0)) AS cr_mql_to_sql,
        SAFE_DIVIDE(SUM(deals_won), NULLIF(SUM(sql),0)) AS cr_sql_to_won
      FROM ${ref}
      WHERE client_id = @client_id
        AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
    `;

    const [job] = await bq.createQueryJob({
      query,
      location: BQ_LOCATION,
      params: { client_id, days },
    });

    const [rows] = await job.getQueryResults();
    res.json({ ok: true, data: rows[0] || { client_id, days_count: 0 } });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

/**
 * GET /kpis/series?client_id=demo&days=30
 * Série diária para charts (últimos N dias) da fact_kpis_daily
 */
app.get("/kpis/series", async (req, res) => {
  const client_id = asStr(req.query.client_id, "demo");
  const days = asInt(req.query.days, 30);

  try {
    const ref = tableRef(PROJECT_ID, DATASET, T_FACT_KPIS);

    const query = `
      SELECT
        date,
        leads,
        mql,
        sql,
        deals_total,
        deals_won,
        revenue,
        cr_lead_to_mql,
        cr_mql_to_sql,
        cr_sql_to_won
      FROM ${ref}
      WHERE client_id = @client_id
        AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
      ORDER BY date ASC
    `;

    const [job] = await bq.createQueryJob({
      query,
      location: BQ_LOCATION,
      params: { client_id, days },
    });

    const [rows] = await job.getQueryResults();
    res.json({ ok: true, rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

/**
 * GET /funnel?client_id=demo&days=30
 * Usa fact_funnel_daily para retornar agregado + série
 */
app.get("/funnel", async (req, res) => {
  const client_id = asStr(req.query.client_id, "demo");
  const days = asInt(req.query.days, 30);

  try {
    const ref = tableRef(PROJECT_ID, DATASET, T_FACT_FUNNEL);

    const queryAgg = `
      SELECT
        ANY_VALUE(client_id) AS client_id,
        SUM(leads) AS leads,
        SUM(mql) AS mql,
        SUM(sql) AS sql,
        SUM(deals_total) AS deals_total,
        SUM(deals_won) AS deals_won,
        SUM(revenue) AS revenue,
        SAFE_DIVIDE(SUM(deals_won), NULLIF(SUM(sql),0)) AS win_rate_over_sql
      FROM ${ref}
      WHERE client_id = @client_id
        AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
    `;

    const querySeries = `
      SELECT
        date,
        leads,
        mql,
        sql,
        deals_total,
        deals_won,
        revenue,
        win_rate_over_sql
      FROM ${ref}
      WHERE client_id = @client_id
        AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
      ORDER BY date ASC
    `;

    const [jobAgg] = await bq.createQueryJob({
      query: queryAgg,
      location: BQ_LOCATION,
      params: { client_id, days },
    });
    const [aggRows] = await jobAgg.getQueryResults();

    const [jobSeries] = await bq.createQueryJob({
      query: querySeries,
      location: BQ_LOCATION,
      params: { client_id, days },
    });
    const [seriesRows] = await jobSeries.getQueryResults();

    res.json({ ok: true, data: aggRows[0] || {}, rows: seriesRows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

/**
 * GET /data-health?client_id=demo&days=30
 * Retorna status de qualidade por tabela (fact_data_health_daily)
 */
app.get("/data-health", async (req, res) => {
  const client_id = asStr(req.query.client_id, "demo");
  const days = asInt(req.query.days, 30);

  try {
    const ref = tableRef(PROJECT_ID, DATASET, T_FACT_HEALTH);

    // Ajuste o schema se sua fact_data_health_daily tiver colunas diferentes
    const query = `
      SELECT *
      FROM ${ref}
      WHERE client_id = @client_id
        AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
      ORDER BY date DESC
      LIMIT 500
    `;

    const [job] = await bq.createQueryJob({
      query,
      location: BQ_LOCATION,
      params: { client_id, days },
    });

    const [rows] = await job.getQueryResults();
    res.json({ ok: true, rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ------------------------------------------------------------
// Start
// ------------------------------------------------------------
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`RevOps API listening on :${PORT} | version=${VERSION}`);
  console.log("Project:", PROJECT_ID, "Dataset:", DATASET, "Location:", BQ_LOCATION);
  console.log("Allowed origins:", allowedOrigins);
});
