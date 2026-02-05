// index.js
import express from "express";
import cors from "cors";
import { BigQuery } from "@google-cloud/bigquery";
import { VertexAI } from "@google-cloud/vertexai";

const app = express();
app.use(cors());
app.use(express.json());

// =====================
// 1) Config (env vars)
// =====================
const PROJECT_ID =
  process.env.GCP_PROJECT ||
  process.env.GOOGLE_CLOUD_PROJECT ||
  "looker-viz-484818";

const BQ_DATASET = process.env.BQ_DATASET || "ussouth1"; // ajuste se necessário
const BQ_LOCATION = process.env.BQ_LOCATION || "US";     // US ou EU (conforme dataset)
const VERTEX_LOCATION = process.env.VERTEX_LOCATION || "us-east1";
const GEMINI_MODEL = process.env.GEMINI_MODEL || "gemini-1.5-flash";

// =====================
// 2) Clients
// =====================
const bq = new BigQuery({ projectId: PROJECT_ID });

// Vertex AI client (Gemini)
const vertex = new VertexAI({ project: PROJECT_ID, location: VERTEX_LOCATION });
const gemini = vertex.getGenerativeModel({ model: GEMINI_MODEL });

// =====================
// 3) Basic routes
// =====================
app.get("/", (_req, res) => {
  res.status(200).json({
    service: "teste-revops-git",
    status: "running",
    endpoints: ["/healthz", "/metrics", "/insights"],
  });
});

app.get("/healthz", (_req, res) => res.status(200).send("ok"));

// =====================
// 4) BigQuery test route
// =====================
// - Essa rota testa: "Cloud Run consegue executar query no BigQuery?"
// - Ajuste TABLE para uma tabela que exista no seu dataset.
app.get("/metrics", async (req, res) => {
  try {
    const tenantId = String(req.query.tenant_id || "demo");

    // ⚠️ Troque para uma tabela real sua (ex.: fact_kpis_daily, fact_funnel_daily etc.)
    const TABLE = process.env.BQ_TABLE_METRICS || "fact_kpis_daily";

    const sql = `
      SELECT
        @tenant_id AS tenant_id,
        COUNT(*) AS total_rows
      FROM \`${PROJECT_ID}.${BQ_DATASET}.${TABLE}\`
      WHERE (@tenant_id IS NULL OR tenant_id = @tenant_id)
    `;

    const [job] = await bq.createQueryJob({
      query: sql,
      location: BQ_LOCATION,
      params: { tenant_id: tenantId },
    });

    const [rows] = await job.getQueryResults();
    res.json({ ok: true, tenant_id: tenantId, table: TABLE, result: rows?.[0] || {} });
  } catch (err) {
    res.status(500).json({
      ok: false,
      where: "GET /metrics",
      hint:
        "Se for erro 403/permission denied: dê BigQuery Job User + BigQuery Data Viewer para a service account do Cloud Run.",
      error: String(err?.message || err),
    });
  }
});

// =====================
// 5) Gemini (Vertex AI) route
// =====================
// - Essa rota testa: "Cloud Run consegue chamar o Gemini via Vertex AI?"
// - Ela usa um prompt simples e, opcionalmente, pode incluir dados do BigQuery.
app.get("/insights", async (req, res) => {
  try {
    const tenantId = String(req.query.tenant_id || "demo");

    const prompt = `
Você é um analista RevOps.
Responda em pt-br, direto e prático.
Contexto: tenant_id="${tenantId}"
Tarefa: gere 3 insights rápidos e 3 ações recomendadas para um dashboard RevOps.
`;

    const resp = await gemini.generateContent({
      contents: [{ role: "user", parts: [{ text: prompt }] }],
    });

    const text =
      resp?.response?.candidates?.[0]?.content?.parts?.map((p) => p.text).join("") || "";

    res.json({
      ok: true,
      tenant_id: tenantId,
      model: GEMINI_MODEL,
      insight: text,
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      where: "GET /insights",
      hint:
        "Se der erro 403: dê Vertex AI User para a service account do Cloud Run e confirme VERTEX_LOCATION/GEMINI_MODEL.",
      error: String(err?.message || err),
    });
  }
});

// =====================
// 6) Start server
// =====================
const port = process.env.PORT || 8080;
app.listen(port, () => console.log(`Listening on ${port}`));
