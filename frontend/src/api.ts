const BASE = (import.meta as any).env?.VITE_API_URL || "/api";

export interface Sensor {
  sensor_id: string;
  city: string;
  highway: string;
  direction: string;
  latitude: number;
  longitude: number;
  lanes: number;
}

export interface LatestReading {
  sensor_id: string;
  ts: string;
  speed_mph: number;
  volume_vph: number;
  occupancy_pct: number;
  source?: string;
}

export interface HistoryPoint {
  hour: string;
  avg_speed: number;
  total_volume: number;
  avg_occupancy: number;
}

export interface Prediction {
  sensor_id: string;
  target_time: string;
  predicted_speed_mph: number;
  predicted_volume_vph: number;
  congestion_level: string;
  model_version: string;
}

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) throw new Error(`${res.status} ${path}`);
  return res.json();
}

async function post<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${res.status} ${path}`);
  return res.json();
}

export const api = {
  cities: () => get<{ cities: string[] }>("/cities"),
  sensors: (city?: string) =>
    get<Sensor[]>(city ? `/sensors?city=${encodeURIComponent(city)}` : "/sensors"),
  latest: (id: string) => get<LatestReading>(`/sensors/${id}/latest`),
  history: (id: string, hours = 24) =>
    get<HistoryPoint[]>(`/sensors/${id}/history?hours=${hours}`),
  predict: (id: string) => post<Prediction>("/predict", { sensor_id: id }),
};
