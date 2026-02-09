cd ~/revops-api

cat > index.js <<'JS'
const express = require("express");
const { BigQuery } = require("@google-cloud/bigquery");

const app = express();
app.use(express.json());

// BigQuery client (uses Cloud Run service account)
const bq = new BigQuery();

app.get("/", (req, res) => {
  res.status(200).json({ ok: true, message: "API online" });
});

app.get("/__health", (req, res) => {
  res.status(200).send("ok");
});

app.post("/echo", (req, res) => {
  res.status(200).json({ received: req.body });
});

app.get("/bq-test", async (req, res) => {
  try {
    const [job] = await bq.createQueryJob({
      query: "SELECT 1 AS ok",
      location: "US",
    });
    const [rows] = await job.getQueryResults();
    res.status(200).json({ ok: true, rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// Troque para uma tabela que existe no seu dataset
app.get("/kpis", async (req, res) => {
  try {
    const datasetId = "ussouth1";
    const tableId = "fact_kpis_daily"; // <-- TROCA AQUI

    const query = `
      SELECT *
      FROM \`${process.env.GCLOUD_PROJECT}.${datasetId}.${tableId}\`
      ORDER BY date DESC
      LIMIT 100
    `;

    const [job] = await bigquery.createQueryJob({ query, location: "US" });
    const [rows] = await job.getQueryResults();

    res.json({ ok: true, rows });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err) });
  }
});

app.use((req, res) => {
  res.status(404).json({ error: `Not found: ${req.method} ${req.path}` });
});

const port = process.env.PORT || 8080;
app.listen(port, "0.0.0.0", () => console.log(`Listening on ${port}`));
JS
