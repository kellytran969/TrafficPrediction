import { useCallback, useEffect, useState } from "react";
import { api, HistoryPoint, LatestReading, Prediction, Sensor } from "./api";
import SpeedChart from "./components/SpeedChart";

const LEVEL_COLOR: Record<string, string> = {
  free_flow: "#22c55e",
  moderate: "#eab308",
  heavy: "#f97316",
  severe: "#ef4444",
};

function levelFor(speed: number): string {
  if (speed >= 50) return "free_flow";
  if (speed >= 38) return "moderate";
  if (speed >= 28) return "heavy";
  return "severe";
}

export default function App() {
  const [cities, setCities] = useState<string[]>([]);
  const [city, setCity] = useState<string>("New York");
  const [sensors, setSensors] = useState<Sensor[]>([]);
  const [latest, setLatest] = useState<Record<string, LatestReading>>({});
  const [selected, setSelected] = useState<string | null>(null);
  const [history, setHistory] = useState<HistoryPoint[]>([]);
  const [prediction, setPrediction] = useState<Prediction | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api.cities().then((r) => setCities(r.cities)).catch(() => setCities([]));
  }, []);

  useEffect(() => {
    setSelected(null);
    api.sensors(city).then(setSensors).catch((e) => setError(String(e)));
  }, [city]);

  const refresh = useCallback(async () => {
    try {
      const readings = await Promise.all(sensors.map((s) => api.latest(s.sensor_id).catch(() => null)));
      const map: Record<string, LatestReading> = {};
      readings.forEach((r) => { if (r) map[r.sensor_id] = r; });
      setLatest(map);
      setError(null);
    } catch (e) { setError(String(e)); }
  }, [sensors]);

  useEffect(() => {
    refresh();
    const t = setInterval(refresh, 5000);
    return () => clearInterval(t);
  }, [refresh]);

  useEffect(() => {
    if (!selected) return;
    api.history(selected).then(setHistory).catch(() => setHistory([]));
    api.predict(selected).then(setPrediction).catch(() => setPrediction(null));
  }, [selected, latest]);

  return (
    <div style={{ maxWidth: 1100, margin: "0 auto", padding: 24 }}>
      <header style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div>
          <h1 style={{ margin: 0 }}>Traffic Prediction</h1>
          <p style={{ color: "#94a3b8", margin: "4px 0 0" }}>
            Live sensor speeds and next-hour ML forecasts
          </p>
        </div>
        <select value={city} onChange={(e) => setCity(e.target.value)}
          style={{ background: "#0f172a", color: "#e5e7eb", padding: 8, borderRadius: 8, border: "1px solid #1e293b" }}>
          {cities.map((c) => <option key={c}>{c}</option>)}
        </select>
      </header>

      {error && <p style={{ color: "#f87171" }}>API unreachable ({error}). Start the backend: <code>uvicorn services.api.app:app</code></p>}

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginTop: 16 }}>
        <div>
          <h3>Sensors</h3>
          <div style={{ display: "grid", gap: 8 }}>
            {sensors.map((s) => {
              const r = latest[s.sensor_id];
              const lvl = r ? levelFor(r.speed_mph) : "free_flow";
              return (
                <button key={s.sensor_id} onClick={() => setSelected(s.sensor_id)}
                  style={{
                    textAlign: "left", background: selected === s.sensor_id ? "#1e293b" : "#0f172a",
                    border: `1px solid ${selected === s.sensor_id ? "#38bdf8" : "#1e293b"}`,
                    borderRadius: 10, padding: 12, color: "#e5e7eb", cursor: "pointer",
                  }}>
                  <div style={{ display: "flex", justifyContent: "space-between" }}>
                    <strong>{s.highway} {s.direction}</strong>
                    <span style={{ color: LEVEL_COLOR[lvl], fontWeight: 700 }}>
                      {r ? `${r.speed_mph.toFixed(0)} mph` : "…"}
                    </span>
                  </div>
                  <div style={{ color: "#64748b", fontSize: 13 }}>
                    {s.sensor_id} · {r ? `${r.volume_vph} veh/h` : "waiting for data"}
                  </div>
                </button>
              );
            })}
          </div>
        </div>

        <div>
          <h3>{selected ? `Forecast · ${selected}` : "Select a sensor"}</h3>
          {selected ? (
            <>
              <SpeedChart
                points={history.map((h) => ({ t: h.hour, speed: h.avg_speed }))}
                prediction={prediction ? { t: prediction.target_time, speed: prediction.predicted_speed_mph } : null}
              />
              {prediction && (
                <div style={{ marginTop: 12, background: "#0f172a", border: "1px solid #1e293b", borderRadius: 10, padding: 12 }}>
                  <div>Next hour: <strong>{prediction.predicted_speed_mph.toFixed(0)} mph</strong></div>
                  <div>Volume: <strong>{prediction.predicted_volume_vph.toLocaleString()} veh/h</strong></div>
                  <div>Congestion: <strong style={{ color: LEVEL_COLOR[prediction.congestion_level] }}>
                    {prediction.congestion_level.replace("_", " ")}</strong></div>
                  <div style={{ color: "#64748b", fontSize: 12 }}>model {prediction.model_version}</div>
                </div>
              )}
            </>
          ) : <p style={{ color: "#64748b" }}>Pick a sensor to see its 24-hour history and the next-hour forecast.</p>}
        </div>
      </div>
    </div>
  );
}
