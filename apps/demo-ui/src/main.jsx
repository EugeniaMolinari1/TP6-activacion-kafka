import React, { useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { io } from "socket.io-client";
import "./style.css";
const API = import.meta.env.VITE_API_URL || "http://localhost:3000";
const labels = {
  ActivationRequested: "Activación solicitada",
  BillingAccountCreated: "Cuenta de facturación creada",
  ProvisioningCompleted: "Aprovisionamiento completado",
  ActivationCompleted: "Plan activo",
  BillingFailed: "Facturación fallida",
  ProvisioningFailed: "Aprovisionamiento fallido",
  ActivationFailed: "Activación fallida",
  BillingAccountCancelled: "Cuenta anulada · compensación",
};
function App() {
  const [form, setForm] = useState({
    customerId: "C-1234",
    planId: "Plan Digital",
    simulateFailure: "none",
  });
  const [current, setCurrent] = useState(null),
    [saved, setSaved] = useState(() => {
      try {
        return JSON.parse(localStorage.getItem("tp6.activations") || "[]");
      } catch {
        return [];
      }
    });
  const [connected, setConnected] = useState(false),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false),
    [lookup, setLookup] = useState("");
  const socket = useRef(null),
    selected = useRef(null);
  useEffect(() => {
    const s = io(API);
    socket.current = s;
    s.on("connect", () => {
      setConnected(true);
      if (selected.current) s.emit("watch", selected.current);
    });
    s.on("disconnect", () => setConnected(false));
    s.on("activation.updated", (snapshot) => {
      if (snapshot.activationId === selected.current) setCurrent(snapshot);
    });
    return () => s.disconnect();
  }, []);
  async function open(id) {
    selected.current = id;
    setError("");
    setCurrent(null);
    socket.current?.emit("watch", id);
    try {
      const r = await fetch(`${API}/activations/${encodeURIComponent(id)}`);
      if (!r.ok) throw new Error("No se encontró la activación");
      const data = await r.json();
      if (selected.current === id) setCurrent(data);
    } catch (e) {
      setError(e.message);
    }
  }
  async function submit(e) {
    e.preventDefault();
    setBusy(true);
    setError("");
    try {
      const r = await fetch(`${API}/activations`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(form),
      });
      if (r.status !== 202)
        throw new Error(`No se pudo crear la activación (${r.status})`);
      const data = await r.json();
      const items = [
        { id: data.activationId, customerId: form.customerId },
        ...saved,
      ].slice(0, 30);
      setSaved(items);
      localStorage.setItem("tp6.activations", JSON.stringify(items));
      await open(data.activationId);
    } catch (e) {
      setError(e.message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <main>
      <header>
        <div>
          <p className="eyebrow">PROGRAMACIÓN AVANZADA · TP6</p>
          <h1>Activación de servicios</h1>
          <p>Un pedido. Varios servicios. Todo conectado por eventos.</p>
        </div>
        <span className={"connection " + (connected ? "on" : "")}>
          {connected ? "● En vivo" : "○ Sin conexión"}
        </span>
      </header>
      <div className="layout">
        <aside>
          <section>
            <h2>Contratar un plan</h2>
            <form onSubmit={submit}>
              <label>
                Cliente
                <input
                  required
                  maxLength={100}
                  value={form.customerId}
                  onChange={(e) =>
                    setForm({ ...form, customerId: e.target.value })
                  }
                />
              </label>
              <label>
                Plan
                <input
                  required
                  maxLength={100}
                  value={form.planId}
                  onChange={(e) => setForm({ ...form, planId: e.target.value })}
                />
              </label>
              <label>
                Simular fallo
                <select
                  value={form.simulateFailure}
                  onChange={(e) =>
                    setForm({ ...form, simulateFailure: e.target.value })
                  }
                >
                  <option value="none">Ninguno · camino feliz</option>
                  <option value="billing">Facturación</option>
                  <option value="provisioning">Aprovisionamiento</option>
                </select>
              </label>
              <button disabled={busy}>
                {busy ? "Solicitando…" : "Crear activación →"}
              </button>
            </form>
          </section>
          <section>
            <h2>Activaciones recientes</h2>
            <div className="recent">
              {saved.length === 0 ? (
                <p>Aún no creaste activaciones.</p>
              ) : (
                saved.map((a) => (
                  <button
                    className={
                      "item " +
                      (current?.activationId === a.id ? "selected" : "")
                    }
                    key={a.id}
                    onClick={() => open(a.id)}
                  >
                    <strong>{a.customerId}</strong>
                    <small>{a.id.slice(0, 18)}…</small>
                  </button>
                ))
              )}
            </div>
            <form
              onSubmit={(e) => {
                e.preventDefault();
                open(lookup);
              }}
            >
              <label>
                Consultar por ID
                <input
                  required
                  placeholder="activationId"
                  value={lookup}
                  onChange={(e) => setLookup(e.target.value)}
                />
              </label>
              <button className="secondary">Consultar</button>
            </form>
          </section>
        </aside>
        <section className="timeline-panel">
          <div className="panel-head">
            <div>
              <p className="eyebrow">HISTORIAL EN TIEMPO REAL</p>
              <h2>Timeline de activación</h2>
            </div>
            {current && (
              <span className={"badge " + current.status}>
                {current.status}
              </span>
            )}
          </div>
          {error && (
            <p role="alert" className="error">
              {error}
            </p>
          )}
          {!current ? (
            <div className="empty">
              <div>↗</div>
              <h3>Seguí el recorrido de tu pedido</h3>
              <p>
                Creá una activación o seleccioná una anterior para ver sus
                eventos.
              </p>
            </div>
          ) : (
            <>
              <dl>
                <div>
                  <dt>Cliente</dt>
                  <dd>{current.customerId}</dd>
                </div>
                <div>
                  <dt>Plan</dt>
                  <dd>{current.planId}</dd>
                </div>
                <div className="wide">
                  <dt>Correlation ID</dt>
                  <dd className="mono">{current.activationId}</dd>
                </div>
              </dl>
              <ol aria-live="polite">
                {current.history.map((e) => (
                  <li
                    key={e.eventId}
                    className={
                      e.eventType.includes("Failed")
                        ? "failed"
                        : e.eventType.includes("Cancelled")
                          ? "cancelled"
                          : "success"
                    }
                  >
                    <span className="dot" />
                    <div>
                      <h3>{labels[e.eventType] || e.eventType}</h3>
                      <p>
                        {e.source} <span>· v{e.version}</span>
                      </p>
                      {e.payload.reason && <p>{e.payload.reason}</p>}
                      <code>{e.eventType}</code>
                    </div>
                    <time>
                      {new Date(e.occurredAt).toLocaleTimeString("es-AR")}
                    </time>
                  </li>
                ))}
              </ol>
              <p className="footnote">
                Billing y provisioning reaccionan en paralelo. El orden de
                llegada de sus resultados puede variar.
              </p>
            </>
          )}
        </section>
      </div>
      <footer>
        NestJS · Kafka KRaft · PostgreSQL · React{" "}
        <span>POC local / Docker Compose</span>
      </footer>
    </main>
  );
}
createRoot(document.getElementById("root")).render(<App />);
