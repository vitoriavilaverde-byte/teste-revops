const express = require("express");

const app = express();
app.use(express.json());

app.get("/__health", (req, res) => {
  res.status(200).send("ok");
});


// Raiz
app.get("/", (req, res) => {
  res.status(200).json({ message: "API no ar ✅" });
});

// Exemplo de POST (pra testar)
app.post("/echo", (req, res) => {
  res.status(200).json({ received: req.body });
});

// 404 padrão (pra ficar explícito)
app.use((req, res) => {
  res.status(404).json({ error: `Rota não encontrada: ${req.method} ${req.path}` });
});

const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Listening on port ${port}`);
});
