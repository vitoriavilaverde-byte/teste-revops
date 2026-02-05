// index.js
const express = require("express");
const { BigQuery } = require("@google-cloud/bigquery");

const app = express();
app.use(express.json());

// Cliente BigQuery (usa as credenciais do Service Account do Cloud Run automaticamente)
const bq = new BigQuery();

// --------------------
// Rotas básicas
// --------------------
app.get("/", (req, res) => {
  res.status(200).json({ ok: true, message: "API online" });
});

// Healthcheck (não use /healthz, aqui usamos /__health)
app.get("/__health", (req, res) => {
  res.status(200).send("ok");
});

// Echo para testar POST
app.post("/echo", (req, res) => {
  res.status(200).json({ received: req.body });
});

// --------------------
// BigQuery: teste mínimo (SELECT 1)hghgh
// --------------------
app.get("/bq-test", async (req, res) => {
  try {
    const [job] = await bq.createQueryJob({
      query: "SELECT 1 AS ok",
      location: "US", // se seu dataset for EU, troque para "EU"
    });

    const [rows] = await job.getQueryResults();
    res.status(200).json({ ok: true, rows });
  } catch (e) {
    res.status(500).json({
      ok: false,
      where: "GET /bq-test",
      error: String(e?.message || e),
      hint:
        "Se for erro 403: dê BigQuery Job User + BigQuery Data Viewer para a Service Account do Cloud Run (revops-api).",
    });
  }
});



# adiciona BigQuery no topo (se ainda não existir)
grep -q '@google-cloud/bigquery' index.js || sed -i '1i const { BigQuery } = require("@google-cloud/bigquery");' index.js

# cria cliente bq logo após app.use(express.json());
grep -q 'const bq = new BigQuery' index.js || sed -i '/app.use(express.json());/a \\\n// Cliente BigQuery\\\nconst bq = new BigQuery();\\\n' index.js

# adiciona a rota /bq-test antes do bloco do 404
grep -q 'app.get("/bq-test"' index.js || sed -i '/app.use((req, res) => {/i \\\n// BigQuery: teste mínimo (SELECT 1)\\\napp.get(\"\\/bq-test\", async (req, res) => {\\\n  try {\\\n    const [job] = await bq.createQueryJob({\\\n      query: \"SELECT 1 AS ok\",\\\n      location: \"US\"\\\n    });\\\n    const [rows] = await job.getQueryResults();\\\n    res.status(200).json({ ok: true, rows });\\\n  } catch (e) {\\\n    res.status(500).json({ ok: false, error: String(e?.message || e) });\\\n  }\\\n});\\\n' index.js

// --------------------
// 404 no final
// --------------------
app.use((req, res) => {
  res.status(404).json({ error: `Not found: ${req.method} ${req.path}` });
});

// --------------------
// Start
// --------------------
const port = process.env.PORT || 8080;
app.listen(port, "0.0.0.0", () => console.log(`Listening on ${port}`));
