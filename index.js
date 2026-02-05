
import express from "express";
import cors from "cors";
import { BigQuery } from "@google-cloud/bigquery";

const app = express();
app.use(cors());
app.use(express.json());

const bq = new BigQuery({ projectId: "looker-viz-484818" });
const DS = "looker-viz-484818.ussouth1";

app.get("/healthz", (_req, res) => res.status(200).send("ok"));

app.get("/kpis", async (req, res) => {
  const clientId = (req.query.client_id || "dark").toString();
  const days = Number(req.query.days || 30);

  const sql = `
    SELECT *
    FROM \`${DS}.fact_kpis_daily\`
    WHERE client_id = @client_id
      AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
    ORDER BY date DESC
  `;

  const [job] = await bq.createQueryJob({ query: sql, params: { client_id: clientId, days }});
  const [rows] = await job.getQueryResults();
  res.json({ client_id: clientId, rows });
});

app.listen(process.env.PORT || 8080, () => console.log("API online"));
