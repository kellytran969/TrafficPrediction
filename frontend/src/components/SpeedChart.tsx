interface Props {
  points: { t: string; speed: number }[];
  prediction?: { t: string; speed: number } | null;
  height?: number;
}

/** Minimal SVG line chart: 24h observed speed + next-hour forecast marker. */
export default function SpeedChart({ points, prediction, height = 180 }: Props) {
  const W = 560;
  const H = height;
  const PAD = 28;
  if (points.length === 0) return <p style={{ color: "#64748b" }}>No history yet.</p>;

  const speeds = points.map((p) => p.speed);
  if (prediction) speeds.push(prediction.speed);
  const min = Math.min(...speeds) - 3;
  const max = Math.max(...speeds) + 3;
  const x = (i: number, n: number) => PAD + (i / Math.max(n - 1, 1)) * (W - 2 * PAD);
  const y = (v: number) => H - PAD - ((v - min) / Math.max(max - min, 1)) * (H - 2 * PAD);

  const d = points.map((p, i) => `${i === 0 ? "M" : "L"}${x(i, points.length)},${y(p.speed)}`).join(" ");
  const predX = prediction ? x(points.length, points.length + 1) : 0;

  return (
    <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ background: "#0f172a", borderRadius: 8 }}>
      {[0.25, 0.5, 0.75].map((f) => (
        <line key={f} x1={PAD} x2={W - PAD} y1={PAD + f * (H - 2 * PAD)} y2={PAD + f * (H - 2 * PAD)} stroke="#1e293b" />
      ))}
      <path d={d} fill="none" stroke="#38bdf8" strokeWidth={2} />
      {points.map((p, i) => (
        <circle key={i} cx={x(i, points.length)} cy={y(p.speed)} r={2.2} fill="#38bdf8" opacity={0.7} />
      ))}
      {prediction && (
        <g>
          <line x1={predX} x2={predX} y1={PAD} y2={H - PAD} stroke="#f59e0b" strokeDasharray="4 3" />
          <circle cx={predX} cy={y(prediction.speed)} r={5} fill="#f59e0b" />
          <text x={predX + 8} y={y(prediction.speed) - 8} fill="#f59e0b" fontSize={12}>
            {prediction.speed.toFixed(0)} mph forecast
          </text>
        </g>
      )}
      <text x={PAD} y={H - 8} fill="#64748b" fontSize={11}>{new Date(points[0].t).toLocaleString()}</text>
      <text x={W - PAD} y={H - 8} fill="#64748b" fontSize={11} textAnchor="end">mph</text>
    </svg>
  );
}
