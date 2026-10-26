import { loadPyodide } from "https://cdn.jsdelivr.net/pyodide/v314.0.7/full/pyodide.mjs";

let pyodide;
let engineHandle;

async function boot() {
  self.postMessage({ type: "progress", value: 22, message: "Loading the Python runtime…" });
  pyodide = await loadPyodide({ indexURL: "https://cdn.jsdelivr.net/pyodide/v314.0.7/full/" });
  self.postMessage({ type: "progress", value: 72, message: "Mounting custom data structures…" });
  const sourceUrl = new URL("./python/engine.py", self.location.href);
  const source = await fetch(sourceUrl).then((response) => {
    if (!response.ok) throw new Error(`Could not load Python engine (${response.status})`);
    return response.text();
  });
  pyodide.FS.writeFile("/home/pyodide/engine.py", source);
  await pyodide.runPythonAsync(`
import sys
sys.path.insert(0, "/home/pyodide")
from engine import handle
`);
  engineHandle = pyodide.globals.get("handle");
  self.postMessage({ type: "progress", value: 100, message: "Sorting floor ready." });
  self.postMessage({ type: "ready" });
}

const ready = boot().catch((error) => {
  self.postMessage({ type: "fatal", error: error.message || String(error) });
  throw error;
});

self.onmessage = async (event) => {
  const { id, action, payload, state } = event.data;
  try {
    await ready;
    const raw = engineHandle(action, JSON.stringify(payload || {}), state ? JSON.stringify(state) : "");
    self.postMessage({ type: "response", id, data: JSON.parse(raw) });
  } catch (error) {
    self.postMessage({ type: "response", id, data: { ok: false, error: error.message || String(error) } });
  }
};
