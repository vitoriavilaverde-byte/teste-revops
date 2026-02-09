/**
 * index.js — RevOps API (Cloud Run) + BigQuery
 * - CORS corrigido (localhost:5173)
 * - Responde preflight OPTIONS
 * - /healthz (debug de deploy)
 * - /kpis (exemplo com BigQuery)
 *
 * Requisitos:
 *   npm i express @google-cloud/bigquery
 *
 * Variáveis de ambiente (recomendado):
 *   BQ_PROJECT_ID=seu-projeto
 *   BQ_DATASET=revops
 *   BQ_TABLE_KPIS=kpis_daily   (ou a tabela/view que você usa)
 *   ALLOWED_ORIGINS=http://localhost:5173,https://seu-front.com
 *   PORT=8080
 */

const express = require("express");
const { BigQuery } = require("@google-cloud/bigquery");

const app = express();
app.use(express.json());

// ============================================================
// 1) CORS (MVP, sem biblioteca — mais previsível)
//    - Use origin exato (NÃO '*') para destravar o browser
// ============================================================
const DEFAULT_ALLOWED = ["http://localhost:5173", "http://127.0.0.1:5173"];
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const allowedOrigins = ALLOWED_ORIGINS.length ? ALLOWED_ORIGINS : DEFAULT_ALLOWED;

app.use((req, res, next) => {
  const origin = req.headers.origin;

  // Se veio do browser e está na whitelist, libera
  if (origin && allowedOrigins.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  }

  // Preflight / métodos / headers
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");

  // Se for OPTIONS, encerra aqui
  if (req.method === "OPTIONS") return res.status(204).end();

  next();
});

// ============================================================
// 2) BigQuery client
// ============================================================
const bigquery = new BigQuery({
  projectId: process.env.BQ_PROJECT_ID || undefined, // opcional se já estiver no ambiente GCP
});

// Helpers
function toIntSafe(v, def) {
  const n = parseInt(String(v || ""), 10);
  return Number.isFinite(n) ? n : def;
}

function badRequest(res, message) {
  return res.status(400).json({ ok: false, error: message });
}

// ============================================================
// 3) Rotas utilitárias (debug de deploy)
// ============================================================
app.get("/healthz", (req, res) => {
  // Troque o valor para confirmar que o deploy atualizou de verdade
  res.setHeader("X-REVOPS-VERSION", "cors-fix-v1");
  res.json({ ok: true, version: "cors-fix-v1" });
});

// ============================================================
// 4) KPI endpoint (ajuste SQL conforme seu schema)
//    GET /kpis?client_id=dark&days=30
// ============================================================
app.get("/kpis", async (req, res) => {
  try {
    const clientId = String(req.query.client_id || "").trim();
    const days = toIntSafe(req.query.days, 30);

    if (!clientId) return badRequest(res, "client_id é obrigatório");
    if (days < 1 || days > 365) return badRequest(res, "days deve estar entre 1 e 365");

    // Ajuste para seu dataset/tabela
    const dataset = process.env.BQ_DATASET || "revops";
    const table = process.env.BQ_TABLE_KPIS || "kpis_daily"; // pode ser view também

    // Exemplo: buscar total de leads nos últimos N dias para o cliente
    // IMPORTANTE: adapte os nomes de colunas (client_id, event_date, leads_total etc.)
    const query = `
      SELECT
        SUM(CAST(leads_total AS INT64)) AS total
      FROM \`${bigquery.projectId}.${dataset}.${table}\`
      WHERE client_id = @client_id
        AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
    `;

    const options = {
      query,
      location: "US",
      params: {
        client_id: clientId,
        days: days,
      },
    };

    const [job] = await bigquery.createQueryJob(options);
    const [rows] = await job.getQueryResults();

    const total = rows?.[0]?.total ?? 0;

    return res.json({
      ok: true,
      data: { total: Number(total) },
      meta: { client_id: clientId, days },
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

// ============================================================
// 5) Start
// ============================================================
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`RevOps API listening on :${PORT}`);
  console.log("Allowed origins:", allowedOrigins);
});
