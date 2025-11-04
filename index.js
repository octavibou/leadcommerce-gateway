import express from "express";
import cors from "cors";
import axios from "axios";
import https from "https";
import http from "http";
import { XMLParser } from "fast-xml-parser";

const app = express();

/* -------------------- CORS -------------------- */
app.use(cors({
  origin: [
    "https://app.leadcommerce.io",
    "https://preview--leadcommerce.lovable.app",
  ]
}));

/* -------------------- Health -------------------- */
app.get("/health", (_req, res) => res.json({ ok: true, service: "gateway" }));
app.get("/catastro/ping", (_req, res) => res.json({ ok: true, service: "catastro" }));

/* -------------------- HTTP Agents + Headers -------------------- */
const httpsAgent = new https.Agent({ keepAlive: true, timeout: 45000 });
const httpAgent  = new http.Agent({  keepAlive: true, timeout: 45000 });

const CAT_HEADERS = {
  Accept: "application/xml,text/xml;q=0.9,*/*;q=0.8",
  "User-Agent": "LeadCommerce-Gateway/1.0 (+https://app.leadcommerce.io)",
  "Accept-Language": "es-ES,es;q=0.9",
  "Cache-Control": "no-cache",
};

/* -------------------- XML & utils -------------------- */
const xmlParser = new XMLParser({
  ignoreAttributes: false,
  removeNSPrefix: true,
  trimValues: true,
  ignoreDeclaration: true,
});

const stripAccentsUpper = (s) =>
  s ? s.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toUpperCase().trim() : s;

const cleanProvinceName = (name) => {
  if (!name) return null;
  let n = String(name).trim();
  n = n.replace(/^provincia\s+de\s+/i, "").trim();
  n = n.replace(/\s+provincia$/i, "").trim();
  return stripAccentsUpper(n);
};
const cleanMunicipalityName = (name) => (name ? stripAccentsUpper(name) : null);

const normalizeProvinciaCode = (cp) => {
  if (!cp) return null;
  const n = parseInt(String(cp), 10);
  return Number.isFinite(n) ? String(n) : null; // "08" -> "8"
};

const toNum = (v) => {
  if (v == null) return null;
  const n = Number(String(v).replace(",", "."));
  return Number.isFinite(n) ? n : null;
};

const findFirst = (obj, key) => {
  if (!obj || typeof obj !== "object") return null;
  const lk = key.toLowerCase();
  if (Array.isArray(obj)) {
    for (const it of obj) {
      const r = findFirst(it, key);
      if (r) return r;
    }
    return null;
  }
  for (const k in obj) {
    if (k.toLowerCase() === lk) {
      const v = obj[k];
      const s = v == null ? "" : String(v).trim();
      if (s) return s;
    }
  }
  for (const k in obj) {
    const r = findFirst(obj[k], key);
    if (r) return r;
  }
  return null;
};

/* ---------- Axios GET con reintentos/backoff ---------- */
async function axiosGetTxt(url, opts = {}) {
  const MAX_TRIES = opts.retries ?? 3;
  const REQ_TIMEOUT = opts.timeout ?? 45000;
  let lastErr = null;
  for (let i = 0; i < MAX_TRIES; i++) {
    try {
      const r = await axios.get(url, {
        headers: CAT_HEADERS,
        responseType: "text",
        validateStatus: () => true,
        timeout: REQ_TIMEOUT,
        httpsAgent,
        httpAgent,
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
      });
      return r;
    } catch (e) {
      lastErr = e;
      const sleep = 500 * Math.pow(2, i); // 0.5s, 1s, 2s
      await new Promise(res => setTimeout(res, sleep));
    }
  }
  throw lastErr;
}

/* ---------- Scraper OVCListaBienes → RC de unidad por nº puerta ---------- */
async function pickUnitRCFromList(parcelRC, desiredNumber) {
  try {
    const RC1 = parcelRC.slice(0, 7);
    const RC2 = parcelRC.slice(7, 14);
    const listUrl =
      `https://www1.sedecatastro.gob.es/CYCBienInmueble/OVCListaBienes.aspx` +
      `?RC1=${RC1}&RC2=${RC2}&RC3=&RC4=&esBice=&RCBice1=&RCBice2=&DenoBice=&pest=rc` +
      `&final=&RCCompleta=${parcelRC}&from=OVCBusqueda&tipoCarto=nuevo`;

    const r = await axiosGetTxt(listUrl);
    if (!(r.status >= 200 && r.status < 300) || typeof r.data !== "string") return null;

    const html = r.data;
    const lines = html.split(/\r?\n/);

    const rcRegex = /\b([0-9A-Z]{20})\b/g;
    const candidates = [];

    for (let i = 0; i < lines.length; i++) {
      const line = (lines.slice(i, i + 3).join(" ") || "").replace(/\s+/g, " ");
      let m;
      while ((m = rcRegex.exec(line)) !== null) {
        candidates.push({ rc20: m[1], context: line });
      }
    }
    if (!candidates.length) return null;

    if (desiredNumber) {
      const want = String(desiredNumber).trim();
      const byPt  = candidates.find((c) => new RegExp(`\\bPt:\\s*${want}\\b`).test(c.context));
      if (byPt) return byPt.rc20;
      const byFree = candidates.find((c) => new RegExp(`\\b${want}\\b`).test(c.context));
      if (byFree) return byFree.rc20;
    }
    return candidates[0].rc20;
  } catch (e) {
    console.warn("pickUnitRCFromList error:", e?.message || e);
    return null;
  }
}

/* --------- Extrae vía/número si DNPRC no trae, usando ldt --------- */
const addressFromLdt = (ldt) => {
  if (!ldt) return { tipo_via: null, via: null, numero: null, cp: null };
  let base = ldt.replace(/\s+[A-ZÑÁÉÍÓÚÜ.\-]+?\s*\(\s*[A-ZÑÁÉÍÓÚÜ.\-]+\s*\)\s*$/i, "").trim();
  const m = base.match(/^\s*(?:CL|AV|PS|PL|CR|CTRA|CM|RD|TRV|C\/|AV\.)?\s*([A-Z0-9ÁÉÍÓÚÑ\s]+?)\s+(\d+[A-Z]?)\b/i);
  return {
    tipo_via: null,
    via: m ? m[1].trim() : null,
    numero: m ? (m[2].trim() === "9999" ? null : m[2].trim()) : null,
    cp: null,
  };
};

/* ==================================================================
   MOCK building
   ================================================================== */
app.get("/catastro/building/:rc", async (req, res) => {
  const { rc } = req.params;
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
   /catastro/rc → XML directo por coordenadas
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

    const r = await axiosGetTxt(url.toString());
    if (r.status >= 200 && r.status < 300 && typeof r.data === "string") {
      return res.type("application/xml").send(r.data);
    }
    return res.status(502).json({ ok: false, error: "catastro_bad_response", status: r.status });
  } catch (e) {
    console.error("Catastro proxy error:", e?.message || e);
    res.status(502).json({ ok: false, error: "catastro_unreachable" });
  }
});

/* ==================================================================
   /catastro/full → JSON enriquecido (PRIORIDAD DIRECCIÓN + atajo by-coords/rc14)
   ================================================================== */
app.get("/catastro/full", async (req, res) => {
  try {
    const {
      lat, lng,
      street, street_number, postal_code,
      municipality_name, province_name,
      prefer, rc14: rc14Param, debug,
    } = req.query;

    const preferByCoords = String(prefer || "") === "bycoords";

    if (!lat || !lng)
      return res.status(400).json({ ok: false, error: "lat & lng required" });

    // Normalización
    const provNorm = cleanProvinceName(province_name);
    const munNorm  = cleanMunicipalityName(municipality_name);

    // DNPLOC → INE (opcional)
    let provincia_ine = null, municipio_ine = null;
    // Saltar DNPLOC si preferimos coords para acelerar
    if (!preferByCoords) {
      const provNorm = cleanProvinceName(province_name);
      const munNorm  = cleanMunicipalityName(municipality_name);
      if (provNorm && munNorm) {
        try {
          const dnploc = new URL("https://ovc.catastro.meh.es/OVCServWeb/OVCSWLocalizacionRC/OVCCallejero.asmx/Consulta_DNPLOC");
          dnploc.searchParams.set("Provincia", provNorm);
          dnploc.searchParams.set("Municipio", munNorm);
          const rLoc = await axiosGetTxt(dnploc.toString(), { timeout: 12000, retries: 1 });
          if (rLoc.status >= 200 && rLoc.status < 300 && typeof rLoc.data === "string") {
            const obj = xmlParser.parse(rLoc.data);
            const cp = findFirst(obj, "cp");
            const cm = findFirst(obj, "cm");
            if (cp) provincia_ine = normalizeProvinciaCode(cp);
            if (cm) municipio_ine = String(cm).padStart(3, "0");
          }
        } catch (err) {
          console.warn("DNPLOC error:", err?.message || err);
        }
      }
    }

    // Helpers resolución
    const tryRcFromCoords = async (LAT, LNG) => {
      try {
        const url = new URL("https://ovc.catastro.meh.es/OVCServWeb/OVCSWLocalizacionRC/OVCCoordenadas.asmx/Consulta_RCCOOR");
        url.searchParams.set("SRS", "EPSG:4326");
        url.searchParams.set("Coordenada_X", String(LNG));
        url.searchParams.set("Coordenada_Y", String(LAT));
        const r = await axiosGetTxt(url.toString());
        if (r.status >= 200 && r.status < 300 && typeof r.data === "string") {
          const obj = xmlParser.parse(r.data);
          const rc  = findFirst(obj, "rc");
          const pc1 = findFirst(obj, "pc1");
          const pc2 = findFirst(obj, "pc2");
          return {
            ok: !!(rc || (pc1 && pc2)),
            rc14: rc ? String(rc).slice(0,14) : (pc1 && pc2 ? `${pc1}${pc2}` : null),
            rawXml: r.data,
            obj
          };
        }
        return { ok: false, status: r.status };
      } catch (err) {
        console.warn("[tryRcFromCoords] error:", err?.message || err);
        return { ok: false, error: String(err?.message || err) };
      }
    };

    const tryRcFromAddress = async () => {
      if (!(provNorm && munNorm && street && street_number)) return { ok:false };
      const url = new URL("https://ovc.catastro.meh.es/OVCServWeb/OVCSWLocalizacionRC/OVCCallejero.asmx/Consulta_DNPPPLOC");
      url.searchParams.set("Provincia", provNorm);
      url.searchParams.set("Municipio", munNorm);
      url.searchParams.set("TipoVia", "");
      url.searchParams.set("NombreVia", stripAccentsUpper(street));
      url.searchParams.set("Numero", String(street_number));
      const r = await axiosGetTxt(url.toString());
      if (r.status >= 200 && r.status < 300 && typeof r.data === "string") {
        const obj = xmlParser.parse(r.data);
        const rc   = findFirst(obj, "rc");
        const pc1  = findFirst(obj, "pc1");
        const pc2  = findFirst(obj, "pc2");
        const rc14 = rc ? String(rc).slice(0,14) : (pc1 && pc2 ? `${pc1}${pc2}` : null);
        const municipio = findFirst(obj, "nm") || null;
        const provincia = findFirst(obj, "np") || null;
        const ldt = findFirst(obj, "ldt") || null;
        return { ok: !!rc14, rc14, obj, rawXml: r.data, municipio, provincia, ldt };
      }
      return { ok:false };
    };

    // 1) Inicialización
    let rc14 = rc14Param || null, rccoorRawXml = null, obj1 = null, municipio = null, provincia = null, ldt = null;

    if (!rc14 && preferByCoords) {
      try {
        const self = `${req.protocol}://${req.get("host")}`;
        const byc = await axios.get(`${self}/catalog/v1/addresses/by-coords`, {
          params: { lat, lng },
          timeout: 15000
        });
        if (byc?.data?.ok && byc?.data?.rc14) {
          rc14 = byc.data.rc14;
          ldt = byc.data.label || ldt;
        }
      } catch (e) {
        console.warn("[full] prefer=bycoords fast-path failed:", e?.message || e);
      }
    }

    // 2) PRIORIDAD: si llega rc14 → úsalo directo
    if (!rc14 && !preferByCoords) {
      // Dirección primero
      const adr = await tryRcFromAddress();
      if (adr.ok) {
        rc14 = adr.rc14;
        obj1 = adr.obj;
        rccoorRawXml = adr.rawXml;
        municipio = adr.municipio || municipio;
        provincia = adr.provincia || provincia;
        ldt = adr.ldt || ldt;
      }
    }

    // 3) Si prefer=bycoords o aún no hay rc14 → usa atajo interno estable
    if (!rc14 && prefer === "bycoords") {
      try {
        const self = `${req.protocol}://${req.get("host")}`;
        const byc = await axios.get(`${self}/catalog/v1/addresses/by-coords`, {
          params: { lat, lng },
          timeout: 15000
        });
        if (byc?.data?.ok && byc?.data?.rc14) {
          rc14 = byc.data.rc14;
          ldt = byc.data.label || ldt;
        }
      } catch (e) {
        console.warn("[full] prefer=bycoords fallback failed:", e?.message || e);
      }
    }

    // 4) Fallback final: RCCOOR con jitter
    if (!rc14) {
      const baseLat = Number(lat);
      const baseLng = Number(lng);
      const deltas = [
        [0, 0],
        [0.00025, 0],
        [-0.00025, 0],
        [0, 0.00025],
        [0, -0.00025],
      ];
      for (const [dlat, dlng] of deltas) {
        const tr = await tryRcFromCoords(baseLat + dlat, baseLng + dlng);
        if (tr.ok && tr.rc14) {
          rc14 = tr.rc14;
          rccoorRawXml = tr.rawXml;
          obj1 = tr.obj;
          break;
        }
      }
      if (obj1) {
        municipio = findFirst(obj1, "nm") || municipio;
        provincia = findFirst(obj1, "np") || provincia;
        ldt = findFirst(obj1, "ldt") || (rccoorRawXml?.match(/<ldt>([^<]+)<\/ldt>/i)?.[1]?.trim() || ldt);
      }
    }

    if (!rc14) {
      return res.json({
        ok: true,
        status: "not_found",
        step: "address_coords_to_rc",
        ...(debug ? { debug: { sample: "no_rc_from_address_nor_coords" } } : {}),
      });
    }

    // RC base y posible RC20
    let refcatBase = rc14;
    let refcat = refcatBase;
    if (refcatBase.length === 14 && street_number) {
      const unit = await pickUnitRCFromList(refcatBase, String(street_number));
      if (unit) refcat = unit;
    }

    // 5) DNPRC para datos ricos
    const provRccoor = provincia ? stripAccentsUpper(provincia) : null;
    const munRccoor  = municipio ? stripAccentsUpper(municipio) : null;

    let r2;
    const tryDNPRC = async (prov, mun, to = 12000) => {
      const url = new URL("https://ovc.catastro.meh.es/OVCServWeb/OVCSWLocalizacionRC/OVCCallejero.asmx/Consulta_DNPRC");
      url.searchParams.set("Provincia", prov ?? "");
      url.searchParams.set("Municipio", mun ?? "");
      url.searchParams.set("RC", refcat);
      try {
        const r = await axiosGetTxt(url.toString(), { timeout: to, retries: 1 });
        if (r.status >= 200 && r.status < 300 && typeof r.data === "string") {
          return { ok: true, xml: r.data, obj: xmlParser.parse(r.data) };
        }
      } catch (e) {
        console.warn("[DNPRC] error:", e?.message || e);
      }
      return { ok:false };
    };

    // Si venimos de coords, primero sin provincia/municipio para acelerar (muchas veces funciona)
    if (preferByCoords) {
      r2 = await tryDNPRC("", "", 12000);
      if (!r2.ok) r2 = await tryDNPRC(provRccoor, munRccoor, 12000);
      if (!r2.ok && ldt) {
        const m = ldt?.match(/\s+([A-ZÁÉÍÓÚÑ\s]+)\s+\(([A-ZÁÉÍÓÚÑ\s]+)\)\s*$/i);
        if (m) r2 = await tryDNPRC(stripAccentsUpper(m[2]), stripAccentsUpper(m[1]), 12000);
      }
      if (!r2.ok) r2 = await tryDNPRC(cleanProvinceName(province_name), cleanMunicipalityName(municipality_name), 12000);
    } else {
      // Camino normal (cuando no forzamos coords)
      r2 = await tryDNPRC(cleanProvinceName(province_name), cleanMunicipalityName(municipality_name), 20000);
      if (!r2.ok) r2 = await tryDNPRC(provRccoor, munRccoor, 20000);
      if (!r2.ok && ldt) {
        const m = ldt?.match(/\s+([A-ZÁÉÍÓÚÑ\s]+)\s+\(([A-ZÁÉÍÓÚÑ\s]+)\)\s*$/i);
        if (m) r2 = await tryDNPRC(stripAccentsUpper(m[2]), stripAccentsUpper(m[1]), 20000);
      }
      if (!r2.ok) r2 = await tryDNPRC("", "", 20000);
    }

    const provinciaOut = provincia || cleanProvinceName(province_name) || null;
    const municipioOut = municipio || cleanMunicipalityName(municipality_name) || null;

    const baseOut = {
      ok: true,
      refcat,
      provincia: provinciaOut,
      municipio: municipioOut,
      provincia_ine: provincia_ine,
      municipio_ine: municipio_ine,
      ...(debug ? { debug: { ldt, sampleXml: rccoorRawXml ? rccoorRawXml.slice(0, 400) : null } } : {}),
    };

    if (!r2?.ok && preferByCoords) {
      // Devuelve rápido con lo esencial: refcat + label parsed desde ldt
      let { via, numero } = addressFromLdt(ldt || "");
      if (!via && street) via = String(street);
      if (!numero && street_number && street_number !== "9999") numero = String(street_number);
      if (numero === "9999") numero = null;

      return res.json({
        ...baseOut,
        direccion: { tipo_via: null, via: via || null, numero: numero || null, cp: postal_code || null },
        uso: null,
        superficie_construida_m2: null,
        anio_construccion: null,
        note: "fast_return_no_dnprc",
        ...(debug ? { debug: { reason: "prefer=bycoords fast path" } } : {}),
      });
    }

    if (r2.ok) {
      const o2 = r2.obj;
      const tipoVia = findFirst(o2, "tv");
      let via       = findFirst(o2, "nv");
      let numero    = findFirst(o2, "pnp");
      const cp      = findFirst(o2, "pc");
      const uso     = findFirst(o2, "luso") || findFirst(o2, "uso");
      const sfc     = toNum(findFirst(o2, "sfc"));
      const ant     = findFirst(o2, "ant");

      // Rellenos suaves
      if ((!via || !numero) && ldt) {
        const m = ldt.match(/^\s*(?:CL|AV|PS|PL|CR|CTRA|CM|RD|TRV|C\/|AV\.)?\s*([A-Z0-9ÁÉÍÓÚÑ\s]+?)\s+(\d+[A-Z]?)\b/i);
        if (m) {
          if (!via) via = m[1].trim();
          if (!numero) numero = m[2].trim();
        }
      }
      if (!via && street) via = String(street);
      if (!numero && street_number && street_number !== "9999") numero = String(street_number);
      if (numero === "9999") numero = null;

      return res.json({
        ...baseOut,
        direccion: {
          tipo_via: tipoVia || null,
          via: via || null,
          numero: numero || null,
          cp: cp || postal_code || null
        },
        uso: uso || null,
        superficie_construida_m2: sfc,
        anio_construccion: ant || null,
        ...(debug ? { debug: { dnprcSample: r2.xml.slice(0, 400) } } : {}),
      });
    }

    // Fallback sin DNPRC utilizable
    let { via, numero } = addressFromLdt(ldt || "");
    if (!via && street) via = String(street);
    if (!numero && street_number && street_number !== "9999") numero = String(street_number);
    if (numero === "9999") numero = null;

    return res.json({
      ...baseOut,
      direccion: { tipo_via: null, via: via || null, numero: numero || null, cp: postal_code || null },
      uso: null,
      superficie_construida_m2: null,
      anio_construccion: null,
      note: "dnprc_failed",
      ...(debug ? { debug: { dnprcSample: null } } : {}),
    });
  } catch (e) {
    console.error("catastro/full error:", e?.message || e);
    res.status(502).json({ ok: false, error: "catastro_unreachable" });
  }
});

/* ==================================================================
   CAPA DE COMPATIBILIDAD (addresses/*)
   ================================================================== */

// Helpers compat
const toRC14 = (rc) => (rc && rc.length >= 14 ? rc.slice(0,14) : null);
const idBU = (rc14) => `ES.SDGC.BU.${rc14}`;
const idAD = (rc14) => `ES.SDGC.AD.${rc14}`;

// 1) Buscar “card” por coords
app.get("/catalog/v1/addresses/by-coords", async (req, res) => {
  try {
    const { lat, lng } = req.query;
    if (!lat || !lng) return res.status(400).json({ ok:false, error:"lat & lng required" });

    const self = `${req.protocol}://${req.get("host")}`;
    const r = await axios.get(`${self}/catastro/rc`, { params: { lat, lng }, responseType: "text", timeout: 20000 });

    const xml = r.data;
    const obj = xmlParser.parse(xml);
    const rc = findFirst(obj, "rc");
    const pc1 = findFirst(obj, "pc1");
    const pc2 = findFirst(obj, "pc2");
    const rc14 = toRC14(rc || (pc1 && pc2 ? `${pc1}${pc2}` : null));

    if (!rc14) return res.status(404).json({ ok:false, error:"no_rc_for_coords" });

    const label = findFirst(obj, "ldire") || findFirst(obj, "ldt") || `Coords ${lat},${lng}`;

    return res.json({
      ok: true,
      buildingId: idBU(rc14),
      addressId: idAD(rc14),
      rc14,
      label,
      photoUrl: null
    });
  } catch (e) {
    console.error("[addresses/by-coords] error:", e?.message || e);
    return res.status(502).json({ ok:false, error:"catastro_unreachable" });
  }
});

// 2) Listado de unidades de un addressId/buildingId
app.get("/catalog/v1/addresses/:id/units", async (req, res) => {
  try {
    const { id } = req.params;
    if (!id || !id.startsWith("ES.SDGC.BU.")) {
      return res.status(400).json({ ok:false, error:"invalid_building_id" });
    }
    const rc14 = id.replace("ES.SDGC.BU.", "");
    const self = `${req.protocol}://${req.get("host")}`;
    const r = await axios.get(`${self}/catastro/building/${rc14}`, { timeout: 15000 });
    return res.json({
      ok: true,
      buildingId: id,
      units: r.data?.units || [],
      meta: {
        uso_principal: r.data?.units?.[0]?.uso_principal || null,
        tipo: r.data?.units?.[0]?.tipo || null,
      }
    });
  } catch (e) {
    console.error("[addresses/:id/units] error:", e?.message || e);
    return res.status(502).json({ ok:false, error:"building_unreachable" });
  }
});

// 3) Foto del address (placeholder)
app.get("/catalog/v1/addresses/:id/photo", async (_req, res) => {
  return res.json({ ok:true, photoUrl: null });
});

/* -------------------- Start -------------------- */
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`✅ Gateway listening on :${PORT}`));