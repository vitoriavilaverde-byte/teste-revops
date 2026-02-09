// index.js (Cloud Run + Express + BigQuery)
// ------------------------------------------------------------
// Endpoints:
//   GET  /         -> { ok: true, message: "API online", version: "..." }
//   GET  /healthz  -> { ok: true, version: "..." }
//   GET  /kpis?client_id=dark&days=30 -> funil + conversões
//
// CORS:
//   libera somente http://localhost:5173 e http://127.0.0.1:5173
//   responde OPTIONS com 204
// ------------------------------------------------------------

const express = require("express");
const { BigQuery } = require("@google-cloud/bigquery");

const app = express();
app.use(express.json());

// ---------------------------
// 1) CORS (whitelist)
// ---------------------------
const allowedOrigins = [
  "http://localhost:5173",
  "http://127.0.0.1:5173",
  // adicione seu domínio do front quando publicar:
  // "https://seu-dominio.com",
];

app.use((req, res, next) => {
  const origin = req.headers.origin;

  // libera só origens conhecidas
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

  // preflight
  if (req.method === "OPTIONS") return res.status(204).end();

  next();
});

// ---------------------------
// 2) BigQuery client
// ---------------------------
const bigquery = new BigQuery({
  projectId: process.env.BQ_PROJECT_ID || undefined,
});

// Helpers
function toIntSafe(v, def) {
  const n = parseInt(String(v ?? ""), 10);
  return Number.isFinite(n) ? n : def;
}
function toStr(v) {
  return String(v ?? "").trim();
}
function num(v, def = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}
function safeDiv(a, b) {
  a = num(a, 0);
  b = num(b, 0);
  if (!b) return 0;
  return a / b;
}

// ---------------------------
// 3) Rotas
// ---------------------------
const VERSION = "cors-fix-v2";

app.get("/", (req, res) => {
  // essa rota existe hoje e é ótima pra validar deploy
  res.json({ ok: true, message: "API online", version: VERSION });
});

app.get("/healthz", (req, res) => {
  res.setHeader("X-REVOPS-VERSION", VERSION);
  res.json({ ok: true, version: VERSION });
});

/**
 * KPI endpoint
 * GET /kpis?client_id=dark&days=30
 *
 * Observação:
 * - Eu deixei um “fallback” de exemplo caso o BigQuery não esteja configurado,
 *   pra você não ficar travado.
 * - Você pode substituir o SQL/tabela/colunas conforme seu schema real.
 */
app.get("/kpis", async (req, res) => {
  try {
    const clientId = toStr(req.query.client_id);
    const days = toIntSafe(req.query.days, 30);

    if (!clientId) {
      return res.status(400).json({ ok: false, error: "client_id é obrigatório" });
    }
    if (days < 1 || days > 365) {
      return res
        .status(400)
        .json({ ok: false, error: "days deve estar entre 1 e 365" });
    }

    // ---------------------------
    // A) Tenta BigQuery (se houver envs)
    // ---------------------------
    const dataset = process.env.BQ_DATASET;
    const table = process.env.BQ_TABLE_KPIS;

    // Se você não configurou dataset/tabela, mantém comportamento estável
    if (!dataset || !table) {
      // Fallback: exemplo (substitua por sua lógica real)
      const leads = 161;
      const mql = 54;
      const sql = 29;
      const deals_total = 46;
      const deals_won = 23;
      const revenue = 163905.42;

      return res.json({
        ok: true,
        data: {
          client_id: clientId,
          days_count: 42, // mantém padrão semelhante ao seu retorno atual
          leads,
          mql,
          sql,
          deals_total,
          deals_won,
          revenue,
          cr_lead_to_mql: safeDiv(mql, leads),
          cr_mql_to_sql: safeDiv(sql, mql),
          cr_sql_to_won: safeDiv(deals_won, sql),
        },
      });
    }

    // ---------------------------
    // B) Query real (ajuste os nomes das colunas)
    // ---------------------------
    // Esperado (exemplo):
    // - client_id (STRING)
    // - event_date (DATE)
    // - leads, mql, sql, deals_total, deals_won, revenue (NUMERIC/INT)
    //
    // Se seu schema é diferente, me diga os nomes que eu adapto.
    const query = `
      SELECT
        COUNT(DISTINCT event_date) AS days_count,
        SUM(CAST(leads AS FLOAT64))       AS leads,
        SUM(CAST(mql AS FLOAT64))         AS mql,
        SUM(CAST(sql AS FLOAT64))         AS sql,
        SUM(CAST(deals_total AS FLOAT64)) AS deals_total,
        SUM(CAST(deals_won AS FLOAT64))   AS deals_won,
        SUM(CAST(revenue AS FLOAT64))     AS revenue
      FROM \`${bigquery.projectId}.${dataset}.${table}\`
      WHERE client_id = @client_id
        AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
    `;

    const options = {
      query,
      location: process.env.BQ_LOCATION || "US",
      params: { client_id: clientId, days },
    };

    const [job] = await bigquery.createQueryJob(options);
    const [rows] = await job.getQueryResults();

    const r = rows?.[0] || {};
    const leads = num(r.leads, 0);
    const mql = num(r.mql, 0);
    const sqlN = num(r.sql, 0);
    const dealsTotal = num(r.deals_total, 0);
    const dealsWon = num(r.deals_won, 0);
    const revenue = num(r.revenue, 0);
    const daysCount = toIntSafe(r.days_count, 0);

    return res.json({
      ok: true,
      data: {
        client_id: clientId,
        days_count: daysCount,
        leads,
        mql,
        sql: sqlN,
        deals_total: dealsTotal,
        deals_won: dealsWon,
        revenue,
        cr_lead_to_mql: safeDiv(mql, leads),
        cr_mql_to_sql: safeDiv(sqlN, mql),
        cr_sql_to_won: safeDiv(dealsWon, sqlN),
      },
    });
  } catch (err) {
    console.error("ERROR /kpis:", err);
    return res.status(500).json({
      ok: false,
      error: "Erro interno ao calcular KPIs",
      detail: err?.message ? String(err.message) : undefined,
    });
  }
});

// ---------------------------
// 4) Start
// ---------------------------
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`RevOps API listening on :${PORT} | version=${VERSION}`);
  console.log("Allowed origins:", allowedOrigins);
});
