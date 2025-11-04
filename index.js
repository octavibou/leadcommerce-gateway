import express from "express";
import cors from "cors";

const app = express();

// CORS: tus dominios
app.use(cors({
  origin: [
    "https://app.leadcommerce.io",
    "https://preview--leadcommerce.lovable.app"
  ]
}));

// Health
app.get("/health", (_req, res) => res.json({ ok: true, service: "gateway" }));

// Endpoint base (mock de momento)
app.get("/catastro/building/:rc", async (req, res) => {
  const { rc } = req.params; // RC14 o RC20
  // Devolvemos un mock para validar integración frontend
  return res.json({
    ok: true,
    id: `ES.SDGC.BU.${rc}`,
    units: [
      {
        refcat: rc.length === 20 ? rc : `${rc}0001AA`,
        uso_principal: "Residencial",
        tipo: "Pisos",
        anio_construccion: 1975,
        superficie_m2: 61
      }
    ],
    foto_url: null,
    croquis_url: null
  });
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`✅ Gateway listening on :${PORT}`));
