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
app.get("/catastro/ping", (_req, res) =>
  res.json({ ok: true, service: "catastro" })
);


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

/* ==================================================================
   /catastro/rc  →  PROXY XML (Coordenada_X / Coordenada_Y)
   ================================================================== */
app.get("/catastro/rc", async (req, res) => {
  try {
    const { lat, lng } = req.query;
    if (!lat || !lng) {
      return res.status(400).json({ ok: false, error: "lat & lng required" });
    }

    const url = new URL("https://ovc.catastro.meh.es/OVCServWeb/OVCSWLocalizacionRC/OVCCoordenadas.asmx/Consulta_RCCOOR");
    url.searchParams.set("SRS", "EPSG:4326");
    url.searchParams.set("Coordenada_X", String(lng));
    url.searchParams.set("Coordenada_Y", String(lat));

    const r = await axios.get(url.toString(), {
      headers: {
        Accept: "application/xml,text/xml;q=0.9,*/*;q=0.8",
        "User-Agent": "LeadCommerce-Gateway/1.0",
      },
      responseType: "text",
      validateStatus: () => true,
      timeout: 15000,
    });

    if (r.status >= 200 && r.status < 300 && typeof r.data === "string") {
      return res.type("application/xml").send(r.data);
    }
    return res.status(502).json({ ok: false, error: "catastro_bad_response", status: r.status });
  } catch (e) {
    console.error("Catastro proxy error:", e?.message || e);
    res.status(502).json({ ok: false, error: "catastro_unreachable" });
  }
});


const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`✅ Gateway listening on :${PORT}`));
