cd ~/revops-api

cat > index.js <<'JS'
const express = require("express");
const { BigQuery } = require("@google-cloud/bigquery");

const app = express();
app.use(express.json());

// BigQuery client (uses Cloud Run service account)
const bq = new BigQuery();

// Basic routes
app.get("/", (req, res) => {
  res.status(200).json({ ok: true, message: "API online" });
});

// Healthcheck (path neutro)
app.get("/__health", (req, res) => {
  res.status(200).send("ok");
});

// Echo
app.post("/echo", (req, res) => {
  res.status(200).json({ received: req.body });
});

// BigQuery test (SELECT 1)
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

// 404 default
app.use((req, res) => {
  res.status(404).json({ error: `Not found: ${req.method} ${req.path}` });
});

// Start
const port = process.env.PORT || 8080;
app.listen(port, "0.0.0.0", () => console.log(`Listening on ${port}`));
JS
