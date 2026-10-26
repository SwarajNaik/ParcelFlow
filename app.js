const STORAGE_KEY = "parcelflow-state-v1";
const HUBS = ["Mumbai Central", "Andheri", "Bandra", "Powai", "Thane", "Navi Mumbai"];
const VIEWS = ["overview", "intake", "parcels", "dispatch", "track", "network", "lab"];

// Mirrors TRANSITIONS in python/engine.py so the UI only offers legal scans.
const TRANSITIONS = {
  READY: [],
  "IN TRANSIT": ["AT HUB"],
  "AT HUB": ["IN TRANSIT", "OUT FOR DELIVERY"],
  "OUT FOR DELIVERY": ["DELIVERED", "AT HUB"],
  DELIVERED: [],
};
const STAGES = [
  { status: "READY", label: "Queued" },
  { status: "IN TRANSIT", label: "In transit" },
  { status: "AT HUB", label: "At hub" },
  { status: "OUT FOR DELIVERY", label: "Out for delivery" },
  { status: "DELIVERED", label: "Delivered" },
];

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

const bootScreen = $("#boot-screen");
const appShell = $("#app-shell");
const progressBar = $("#boot-progress");
const bootCopy = $("#boot-copy");
const toastRegion = $("#toast-region");
const modal = $("#modal");
const modalBody = $("#modal-body");

let state = null;
let currentView = "overview";
let statusFilter = "ALL";
let modalParcelId = null;
let graphRunning = false;
let requestCounter = 0;
const pending = new Map();
const worker = new Worker(new URL("./python.worker.js", import.meta.url), { type: "module" });

worker.addEventListener("message", (event) => {
  const message = event.data;
  if (message.type === "progress") {
    progressBar.style.width = `${message.value}%`;
    bootCopy.textContent = message.message;
  }
  if (message.type === "ready") startApplication();
  if (message.type === "fatal") {
    bootCopy.textContent = `Python could not start: ${message.error}. Serve this folder over HTTP and check your connection.`;
    progressBar.style.background = "#df6038";
  }
  if (message.type === "response") {
    const handler = pending.get(message.id);
    if (handler) {
      pending.delete(message.id);
      handler.resolve(message.data);
    }
  }
});

// Engine calls are serialised so every request is built from the newest state;
// otherwise a slow read-only call could overwrite a change made meanwhile.
let engineChain = Promise.resolve();
function runEngine(action, payload = {}, options = {}) {
  const call = engineChain.then(
    () =>
      new Promise((resolve) => {
        const id = ++requestCounter;
        pending.set(id, { resolve });
        worker.postMessage({ id, action, payload, state: options.noState ? null : state });
      }),
  );
  engineChain = call.catch(() => {});
  return call.then((response) => {
    if (!response.ok) throw new Error(response.error || "The Python engine rejected this action");
    if (response.state) {
      state = response.state;
      try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
      } catch {
        /* storage may be unavailable in private windows */
      }
    }
    return response;
  });
}

async function startApplication() {
  try {
    let stored = null;
    try {
      stored = localStorage.getItem(STORAGE_KEY);
    } catch {
      stored = null;
    }
    if (stored) {
      state = JSON.parse(stored);
      // A cheap read normalises v1 saves (adds route, hop and location).
      await runEngine("lookup", { id: state.parcels?.[0]?.id || "P101" });
    } else {
      await runEngine("reset", {}, { noState: true });
    }
    setTimeout(() => {
      bootScreen.hidden = true;
      appShell.hidden = false;
      populateHubSelects();
      bindEvents();
      renderAll();
      renderTreePreview();
      routeFromHash();
      setInterval(renderClock, 30000);
    }, 200);
  } catch (error) {
    bootCopy.textContent = `Could not restore ParcelFlow: ${error.message}`;
  }
}

/* ---------- helpers ---------- */

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

const slug = (status) => String(status).toLowerCase().replaceAll(" ", "-");
const pad = (value) => String(value).padStart(2, "0");
const parcelById = (id) => state.parcels.find((item) => item.id === id);
const stageLabel = (status) => STAGES.find((stage) => stage.status === status)?.label || status;

function toast(message, kind = "success") {
  const element = document.createElement("div");
  element.className = `toast ${kind}`;
  element.textContent = message;
  toastRegion.append(element);
  setTimeout(() => element.remove(), 3600);
}

function handleError(error) {
  console.error(error);
  toast(error.message || String(error), "error");
}

function formatTime(timestamp) {
  try {
    return new Intl.DateTimeFormat("en-IN", { hour: "2-digit", minute: "2-digit", day: "2-digit", month: "short", timeZone: "Asia/Kolkata" }).format(new Date(timestamp));
  } catch {
    return timestamp;
  }
}

function nextAction(parcel) {
  const { route, status } = parcel;
  const hop = parcel.hop ?? 0;
  if (status === "READY") {
    const position = state.dispatch_queue.indexOf(parcel.id);
    return position === 0 ? "Next out of the queue" : `Queue position ${pad(position + 1)}`;
  }
  if (status === "IN TRANSIT") return `Arrive at ${route[Math.min(hop + 1, route.length - 1)]}`;
  if (status === "AT HUB") return parcel.location === parcel.destination ? "Send out for delivery" : `Depart toward ${route[hop + 1]}`;
  if (status === "OUT FOR DELIVERY") return "Mark delivered";
  return null;
}

function nextActionShort(parcel) {
  if (parcel.status === "IN TRANSIT") return "Arrive";
  if (parcel.status === "AT HUB") return parcel.location === parcel.destination ? "Out for delivery" : "Depart";
  if (parcel.status === "OUT FOR DELIVERY") return "Deliver";
  return null;
}

function statusPill(status) {
  return `<span class="status ${slug(status)}">${escapeHtml(stageLabel(status))}</span>`;
}

function routeRail(parcel, light = false) {
  const hop = parcel.hop ?? 0;
  const delivered = parcel.status === "DELIVERED";
  const moving = parcel.status === "IN TRANSIT";
  return `<ol class="route-rail ${light ? "light" : ""}" aria-label="Route">${parcel.route
    .map((hub, index) => {
      let cls = "";
      if (delivered || index < hop) cls = "done";
      else if (index === hop) cls = moving ? "done leaving" : parcel.status === "READY" ? "current waiting" : "current";
      else if (index === hop + 1 && moving) cls = "next";
      return `<li class="${cls}"><span class="stop"></span><span class="hub-name">${escapeHtml(hub)}</span></li>`;
    })
    .join("")}</ol>`;
}

function stepper(status) {
  const current = STAGES.findIndex((stage) => stage.status === status);
  return `<ol class="stepper">${STAGES.map(
    (stage, index) => `<li class="${index < current ? "done" : index === current ? "current" : ""}"><span>${index + 1}</span>${stage.label}</li>`,
  ).join("")}</ol>`;
}

function timeline(parcel) {
  const events = [...parcel.events].reverse();
  return `<div class="timeline">${events
    .map(
      (event, index) => `<div class="timeline-node ${index === 0 ? "latest" : ""}"><h3>${escapeHtml(event.status)} · ${escapeHtml(event.hub)}</h3><p>${escapeHtml(event.note)}</p><small>${formatTime(event.time)} · node ${parcel.events.length - index}${index === 0 ? " · tail" : ""}</small></div>`,
    )
    .join("")}</div>`;
}

function suggestId() {
  const max = state.parcels.reduce((best, parcel) => Math.max(best, Number(parcel.id.slice(1)) || 0), 100);
  return `P${max + 1}`;
}

/* ---------- navigation ---------- */

function populateHubSelects() {
  $$(".hub-select").forEach((select) => {
    select.innerHTML = HUBS.map((hub) => `<option>${hub}</option>`).join("");
  });
  $("#intake-destination").value = "Thane";
  $("#route-end").value = "Navi Mumbai";
}

function bindEvents() {
  document.addEventListener("click", (event) => {
    const nav = event.target.closest("[data-view], [data-go], [data-view-link]");
    if (nav) {
      event.preventDefault();
      closeModal();
      showView(nav.dataset.view || nav.dataset.go || nav.dataset.viewLink);
      return;
    }
    const filterGo = event.target.closest("[data-filter-go]");
    if (filterGo) {
      statusFilter = filterGo.dataset.filterGo;
      renderFilters();
      renderRecords();
      showView("parcels");
      return;
    }
    const opener = event.target.closest("[data-open]");
    if (opener) {
      event.preventDefault();
      openParcel(opener.dataset.open);
      return;
    }
    const advance = event.target.closest("[data-advance]");
    if (advance) {
      event.preventDefault();
      advanceParcel(advance.dataset.advance, advance);
      return;
    }
    const track = event.target.closest("[data-track]");
    if (track) {
      event.preventDefault();
      closeModal();
      trackParcel(track.dataset.track);
    }
  });
  $("#menu-button").addEventListener("click", toggleMenu);
  $("#register-form").addEventListener("submit", registerParcel);
  $("#suggest-id").addEventListener("click", () => {
    $("#register-form").id.value = suggestId();
    validateId();
  });
  $("#register-form").id.addEventListener("input", validateId);
  $("#intake-origin").addEventListener("change", previewRoute);
  $("#intake-destination").addEventListener("change", previewRoute);
  $("#search-form").addEventListener("submit", searchParcels);
  $("#sort-button").addEventListener("click", sortParcels);
  $("#records-filter").addEventListener("input", renderRecords);
  $("#dispatch-button").addEventListener("click", dispatchParcel);
  $("#advance-all").addEventListener("click", advanceAll);
  $("#lookup-form").addEventListener("submit", (event) => {
    event.preventDefault();
    trackParcel(new FormData(event.currentTarget).get("id"));
  });
  $("#bfs-button").addEventListener("click", () => runTraversal("bfs"));
  $("#dfs-button").addEventListener("click", () => runTraversal("dfs"));
  $("#route-button").addEventListener("click", findRoute);
  $("#expression-form").addEventListener("submit", processExpression);
  $("#recursion-form").addEventListener("submit", compareRecursion);
  $("#dock-enqueue").addEventListener("click", () => updateDock("enqueue"));
  $("#dock-dequeue").addEventListener("click", () => updateDock("dequeue"));
  $("#tree-button").addEventListener("click", updateTree);
  $("#undo-button").addEventListener("click", undoAction);
  $("#reset-button").addEventListener("click", resetData);
  $("#modal-close").addEventListener("click", closeModal);
  modal.addEventListener("click", (event) => {
    if (event.target === modal) closeModal();
  });
  modal.addEventListener("close", () => {
    modalParcelId = null;
  });
  window.addEventListener("hashchange", routeFromHash);
}

function routeFromHash() {
  const [view, parcelId] = location.hash.replace("#", "").split("/");
  if (view === "track" && parcelId) {
    trackParcel(decodeURIComponent(parcelId));
  } else if (VIEWS.includes(view) && view !== currentView) {
    showView(view);
  }
}

function toggleMenu() {
  const menu = $("#mobile-nav");
  menu.hidden = !menu.hidden;
  $("#menu-button").setAttribute("aria-expanded", String(!menu.hidden));
}

function showView(view, hash = view) {
  if (!VIEWS.includes(view)) return;
  currentView = view;
  $$("[data-view-panel]").forEach((panel) => {
    const active = panel.dataset.viewPanel === view;
    panel.hidden = !active;
    panel.classList.toggle("active", active);
  });
  $$(".nav-link").forEach((link) => link.classList.toggle("active", link.dataset.view === view));
  $("#mobile-nav").hidden = true;
  $("#menu-button").setAttribute("aria-expanded", "false");
  history.replaceState(null, "", `#${hash}`);
  window.scrollTo({ top: 0, behavior: "smooth" });
  if (view === "intake") {
    previewRoute();
    if (!$("#register-form").id.value) $("#register-form").id.value = suggestId();
    validateId();
  }
}

/* ---------- rendering ---------- */

function renderAll() {
  renderClock();
  renderMetrics();
  renderPipeline();
  renderQueue("#overview-queue", false);
  renderQueue("#dispatch-queue", true);
  renderLatestEvent();
  renderFilters();
  renderRecords();
  renderInflight();
  renderQuickPicks();
  renderHubLoad();
  renderDock();
  renderQueueForecast();
  $("#hero-throughput").textContent = `${pad(state.parcels.length)} parcels`;
  const undoDepth = state.undo_stack?.length || 0;
  $("#undo-button").disabled = undoDepth === 0;
  $("#undo-count").textContent = undoDepth;
  $("#undo-count").hidden = undoDepth === 0;
  if (modalParcelId && modal.open) openParcel(modalParcelId);
}

function renderClock() {
  $("#clock").textContent = new Intl.DateTimeFormat("en-IN", { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Kolkata", hour12: false }).format(new Date()) + " IST";
}

function countBy(status) {
  return state.parcels.filter((parcel) => parcel.status === status).length;
}

function renderMetrics() {
  const onRoad = countBy("IN TRANSIT") + countBy("AT HUB") + countBy("OUT FOR DELIVERY");
  const metrics = [
    [state.parcels.length, "Parcels registered"],
    [state.dispatch_queue.length, "Waiting to dispatch"],
    [onRoad, "On the road"],
    [countBy("DELIVERED"), "Delivered"],
  ];
  $("#metric-strip").innerHTML = metrics.map(([value, label]) => `<div class="metric"><i></i><strong>${value}</strong><span>${label}</span></div>`).join("");
}

function renderPipeline() {
  $("#pipeline").innerHTML = STAGES.map((stage) => {
    const parcels = state.parcels.filter((parcel) => parcel.status === stage.status);
    const shown = stage.status === "DELIVERED" ? parcels.slice(-5).reverse() : parcels.slice(0, 5);
    const more = parcels.length - shown.length;
    return `<div class="pipeline-col ${slug(stage.status)}"><div class="pipeline-head"><span>${stage.label}</span><b>${parcels.length}</b></div>${
      shown
        .map(
          (parcel) => `<button class="pipeline-card" data-open="${escapeHtml(parcel.id)}"><strong>${escapeHtml(parcel.id)}</strong><small>${escapeHtml(parcel.status === "DELIVERED" ? parcel.destination : parcel.location)}</small></button>`,
        )
        .join("") || `<p class="pipeline-empty">—</p>`
    }${more > 0 ? `<button class="text-button pipeline-more" data-filter-go="${stage.status}">+${more} more</button>` : ""}</div>`;
  }).join("");
}

function queueCard(parcelId, index) {
  const parcel = parcelById(parcelId);
  return `<button class="queue-card ${index === 0 ? "front" : ""}" data-open="${escapeHtml(parcelId)}"><span class="position"><span>${index === 0 ? "NEXT OUT" : "POSITION"}</span><span>${pad(index + 1)}</span></span><strong>${escapeHtml(parcelId)}</strong><small>${escapeHtml(parcel?.origin || "Unknown hub")} → ${escapeHtml(parcel?.destination || "")}</small></button>`;
}

function renderQueue(selector, includeEmpty) {
  const element = $(selector);
  if (!state.dispatch_queue.length) {
    element.innerHTML = includeEmpty
      ? `<div class="empty-state">No parcels are waiting. <button class="text-button light" data-go="intake">Register one</button></div>`
      : `<div class="empty-state">Queue clear.</div>`;
  } else {
    element.innerHTML = state.dispatch_queue
      .map((id, index) => `${queueCard(id, index)}${index < state.dispatch_queue.length - 1 ? '<span class="queue-arrow" aria-hidden="true">→</span>' : ""}`)
      .join("");
  }
  $("#queue-count-badge").textContent = `${state.dispatch_queue.length} waiting`;
  $("#dispatch-button").disabled = state.dispatch_queue.length === 0;
}

function renderLatestEvent() {
  const events = state.parcels
    .flatMap((parcel) => parcel.events.map((event, index) => ({ ...event, index, parcelId: parcel.id })))
    .sort((a, b) => String(b.time).localeCompare(String(a.time)) || b.index - a.index);
  const event = events[0];
  $("#latest-event").innerHTML = event
    ? `<div class="latest-card"><time>${formatTime(event.time)}</time><h3>${escapeHtml(event.parcelId)} · ${escapeHtml(stageLabel(event.status))}</h3><p>${escapeHtml(event.note)}<br><b>${escapeHtml(event.hub)}</b></p><button class="text-button light" data-open="${escapeHtml(event.parcelId)}">Open parcel</button></div>`
    : `<p>No scans yet.</p>`;
}

function renderFilters() {
  const options = [{ status: "ALL", label: "All" }, ...STAGES];
  $("#status-filters").innerHTML = options
    .map((option) => {
      const count = option.status === "ALL" ? state.parcels.length : countBy(option.status);
      return `<button class="filter-chip ${statusFilter === option.status ? "active" : ""}" data-filter="${option.status}" aria-pressed="${statusFilter === option.status}">${option.label}<b>${count}</b></button>`;
    })
    .join("");
  $$("#status-filters [data-filter]").forEach((button) =>
    button.addEventListener("click", () => {
      statusFilter = button.dataset.filter;
      renderFilters();
      renderRecords();
    }),
  );
}

function renderRecords() {
  $("#record-count").textContent = state.parcels.length;
  const query = $("#records-filter").value.trim().toLowerCase();
  const rows = state.parcels.filter((parcel) => {
    if (statusFilter !== "ALL" && parcel.status !== statusFilter) return false;
    if (!query) return true;
    return [parcel.id, parcel.sender, parcel.recipient, parcel.origin, parcel.destination].some((value) => String(value).toLowerCase().includes(query));
  });
  $("#records-body").innerHTML =
    rows
      .map(
        (parcel) => `<tr data-open="${escapeHtml(parcel.id)}" tabindex="-1"><td>${escapeHtml(parcel.id)}</td><td>${escapeHtml(parcel.origin)} → ${escapeHtml(parcel.destination)}<small>${parcel.route.length - 1} hop${parcel.route.length === 2 ? "" : "s"}</small></td><td>${escapeHtml(parcel.recipient)}</td><td>${statusPill(parcel.status)}</td><td>${escapeHtml(parcel.status === "DELIVERED" ? `Delivered at ${parcel.destination}` : nextAction(parcel))}</td><td><button class="row-open" data-open="${escapeHtml(parcel.id)}" aria-label="Open ${escapeHtml(parcel.id)}">Open ↗</button></td></tr>`,
      )
      .join("") || `<tr><td colspan="6" class="empty-row">No parcels match this filter.</td></tr>`;
}

function inflightParcels() {
  return state.parcels.filter((parcel) => ["IN TRANSIT", "AT HUB", "OUT FOR DELIVERY"].includes(parcel.status));
}

function renderInflight() {
  const parcels = inflightParcels();
  $("#inflight-count").textContent = parcels.length;
  $("#advance-all").disabled = parcels.length === 0;
  $("#inflight-list").innerHTML =
    parcels
      .map((parcel) => {
        const total = parcel.route.length - 1;
        const done = Math.min(total, (parcel.hop ?? 0) + (parcel.status === "IN TRANSIT" ? 0.5 : 0));
        const percent = parcel.status === "OUT FOR DELIVERY" ? 92 : Math.round((done / Math.max(total, 1)) * 85);
        return `<div class="inflight-row">
          <div class="inflight-main">
            <button class="inflight-id" data-open="${escapeHtml(parcel.id)}">${escapeHtml(parcel.id)}</button>
            ${statusPill(parcel.status)}
            <span class="inflight-route">${escapeHtml(parcel.location)} · ${escapeHtml(nextAction(parcel))}</span>
          </div>
          <div class="progress" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${percent}" aria-label="${escapeHtml(parcel.id)} progress"><span style="width:${percent}%"></span></div>
          <button class="secondary-button compact" data-advance="${escapeHtml(parcel.id)}">${escapeHtml(nextActionShort(parcel))} →</button>
        </div>`;
      })
      .join("") || `<div class="empty-state">Nothing on the road. Dispatch the front parcel to start its journey.</div>`;
}

function renderQuickPicks() {
  const picks = state.parcels.slice(-5).reverse();
  $("#quick-picks").innerHTML = picks.length
    ? `<span>Recent:</span>${picks.map((parcel) => `<button class="filter-chip" data-track="${escapeHtml(parcel.id)}">${escapeHtml(parcel.id)}</button>`).join("")}`
    : "";
}

function renderHubLoad() {
  HUBS.forEach((hub) => {
    const count = state.parcels.filter((parcel) => parcel.status !== "DELIVERED" && parcel.location === hub).length;
    const node = $(`[data-node="${CSS.escape(hub)}"] .load`);
    if (!node) return;
    node.classList.toggle("empty", count === 0);
    $("text", node).textContent = count || "";
  });
}

function renderQueueForecast() {
  const position = state.dispatch_queue.length + 1;
  $("#queue-forecast").innerHTML = `<span>Joins queue at</span><strong>${pad(position)}</strong><small>${position === 1 ? "Leaves next" : `${position - 1} parcel${position === 2 ? "" : "s"} ahead`}</small>`;
}

/* ---------- intake ---------- */

function validateId() {
  const input = $("#register-form").id;
  const value = input.value.trim().toUpperCase();
  const hint = $("#id-hint");
  let message = "Format P + 3–6 digits";
  let invalid = false;
  if (value && !/^P\d{3,6}$/.test(value)) {
    message = "Use P followed by 3–6 digits, e.g. P104";
    invalid = true;
  } else if (value && parcelById(value)) {
    message = `${value} already exists — try ${suggestId()}`;
    invalid = true;
  } else if (value) {
    message = "Available";
  }
  hint.textContent = message;
  hint.classList.toggle("error", invalid);
  hint.classList.toggle("ok", !invalid && Boolean(value));
  input.setAttribute("aria-invalid", String(invalid));
  return !invalid && Boolean(value);
}

async function previewRoute() {
  const start = $("#intake-origin").value;
  const end = $("#intake-destination").value;
  const rail = $("#route-preview");
  if (start === end) {
    rail.innerHTML = `<li class="rail-warning">Origin and destination must differ.</li>`;
    $("#preview-hops").textContent = "—";
    return;
  }
  try {
    const response = await runEngine("route", { start, end });
    const path = response.result.path;
    rail.innerHTML = path.map((hub, index) => `<li class="${index === 0 ? "current" : ""}"><span class="stop"></span><span class="hub-name">${escapeHtml(hub)}</span></li>`).join("");
    $("#preview-hops").textContent = `${response.result.edges} hop${response.result.edges === 1 ? "" : "s"}`;
  } catch (error) {
    handleError(error);
  }
}

async function registerParcel(event) {
  event.preventDefault();
  const formElement = event.currentTarget;
  if (!validateId()) {
    formElement.id.focus();
    toast($("#id-hint").textContent, "error");
    return;
  }
  const payload = Object.fromEntries(new FormData(formElement));
  if (payload.origin === payload.destination) {
    toast("Choose two different hubs", "error");
    return;
  }
  try {
    const response = await runEngine("register", payload);
    const { parcel, queue_position: position, bst_path: path } = response.result;
    formElement.reset();
    populateHubSelects();
    renderAll();
    formElement.id.value = suggestId();
    validateId();
    previewRoute();
    openModal(`
      <p class="eyebrow">REGISTERED / LABEL</p>
      <h2 id="modal-title">${escapeHtml(parcel.id)} entered the flow</h2>
      <div class="label-slip">
        <div><small>FROM</small><strong>${escapeHtml(parcel.sender)}</strong><span>${escapeHtml(parcel.origin)}</span></div>
        <div><small>TO</small><strong>${escapeHtml(parcel.recipient)}</strong><span>${escapeHtml(parcel.destination)}</span></div>
        <div class="slip-code" aria-hidden="true">${escapeHtml(parcel.id)}</div>
      </div>
      ${routeRail(parcel)}
      <div class="modal-facts">
        <div><small>QUEUE POSITION</small><strong>${pad(position)}</strong></div>
        <div><small>BST INSERT PATH</small><strong class="mono">${path.map(escapeHtml).join(" → ")}</strong></div>
        <div><small>ROUTE</small><strong>${parcel.route.length - 1} hop${parcel.route.length === 2 ? "" : "s"}</strong></div>
      </div>
      <div class="modal-actions">
        <button class="primary-button" data-go="dispatch">Go to dispatch <span>→</span></button>
        <button class="secondary-button" data-track="${escapeHtml(parcel.id)}">Tracking page</button>
        <button class="text-button" id="register-another">Register another</button>
      </div>`);
    $("#register-another").addEventListener("click", () => {
      closeModal();
      formElement.sender.focus();
    });
  } catch (error) {
    handleError(error);
  }
}

/* ---------- modal ---------- */

function openModal(html) {
  modalBody.innerHTML = html;
  if (!modal.open) modal.showModal();
}

function closeModal() {
  if (modal.open) modal.close();
  modalParcelId = null;
}

function scanForm(parcel) {
  const atDestination = parcel.location === parcel.destination;
  const allowed = (TRANSITIONS[parcel.status] || []).filter(
    (status) => !(status === "OUT FOR DELIVERY" && !atDestination) && !(status === "IN TRANSIT" && atDestination),
  );
  if (parcel.status === "READY") {
    const front = state.dispatch_queue[0] === parcel.id;
    return `<div class="scan-blocked"><p>Queued at position ${pad(state.dispatch_queue.indexOf(parcel.id) + 1)}. Scans start after FIFO dispatch.</p>${front ? `<button class="orange-button" id="modal-dispatch">Dispatch now <span>→</span></button>` : ""}</div>`;
  }
  if (!allowed.length) return `<div class="scan-blocked"><p>Delivered — the linked list is closed.</p></div>`;
  return `<form class="scan-form" id="scan-form">
    <label>Status<select name="status">${allowed.map((status) => `<option value="${status}">${stageLabel(status)}</option>`).join("")}</select></label>
    <label>Hub<select name="hub">${HUBS.map((hub) => `<option>${hub}</option>`).join("")}</select></label>
    <label class="wide">Note<input name="note" placeholder="Optional scan note" maxlength="120" /></label>
    <button class="secondary-button" type="submit">Append scan</button>
  </form>`;
}

function defaultScanHub(parcel, status) {
  if (status === "OUT FOR DELIVERY" || status === "DELIVERED") return parcel.destination;
  if (status === "IN TRANSIT") return parcel.location;
  if (status === "AT HUB") return parcel.route[(parcel.hop ?? 0) + 1] || parcel.location;
  return parcel.location;
}

function openParcel(parcelId) {
  const parcel = parcelById(parcelId);
  if (!parcel) return;
  modalParcelId = parcelId;
  const action = nextActionShort(parcel);
  openModal(`
    <div class="modal-head">
      <div><p class="eyebrow">PARCEL / ${escapeHtml(parcel.events.length)} TRACKING NODES</p><h2 id="modal-title">${escapeHtml(parcel.id)}</h2></div>
      ${statusPill(parcel.status)}
    </div>
    ${stepper(parcel.status)}
    ${routeRail(parcel)}
    <div class="modal-facts">
      <div><small>SENDER</small><strong>${escapeHtml(parcel.sender)}</strong></div>
      <div><small>RECIPIENT</small><strong>${escapeHtml(parcel.recipient)}</strong></div>
      <div><small>NOW</small><strong>${escapeHtml(parcel.status === "DELIVERED" ? "Delivered" : parcel.location)}</strong></div>
      <div><small>NEXT</small><strong>${escapeHtml(nextAction(parcel) || "—")}</strong></div>
    </div>
    <div class="modal-split">
      <div>
        <h3 class="mini-heading">Scan history <span>newest first</span></h3>
        ${timeline(parcel)}
      </div>
      <div>
        <h3 class="mini-heading">Record a scan</h3>
        ${action ? `<button class="primary-button full" data-advance="${escapeHtml(parcel.id)}">${escapeHtml(nextAction(parcel))} <span>→</span></button><p class="or-divider">or a manual scan</p>` : ""}
        ${scanForm(parcel)}
        <button class="text-button" data-track="${escapeHtml(parcel.id)}">Open recipient tracking page ↗</button>
      </div>
    </div>`);
  const form = $("#scan-form");
  if (form) {
    const sync = () => (form.hub.value = defaultScanHub(parcel, form.status.value));
    sync();
    form.status.addEventListener("change", sync);
    form.addEventListener("submit", (event) => addTrackingEvent(event, parcel.id));
  }
  $("#modal-dispatch")?.addEventListener("click", dispatchParcel);
}

/* ---------- actions ---------- */

async function searchParcels(event) {
  event.preventDefault();
  const payload = Object.fromEntries(new FormData(event.currentTarget));
  try {
    const response = await runEngine("search", payload);
    const result = response.result;
    const path = result.trace?.map((step) => step.value) || result.path || [];
    const element = $("#search-result");
    element.classList.remove("empty-state");
    element.innerHTML = `<div class="result-head"><strong>${result.found ? `${escapeHtml(payload.id.toUpperCase())} found` : "No matching parcel"}</strong><span class="complexity-pill">${escapeHtml(response.complexity)}</span></div><div class="search-path">${
      path.map((id, index) => `<span class="search-chip ${result.found && index === path.length - 1 ? "hit" : ""}" style="animation-delay:${index * 80}ms">${escapeHtml(id)}</span>`).join("") || '<span class="search-chip">∅</span>'
    }</div><p>${result.comparisons} comparison${result.comparisons === 1 ? "" : "s"}${result.sorted_ids ? `. Binary search used a merge-sorted copy: ${result.sorted_ids.map(escapeHtml).join(", ")}.` : "."}</p>${
      result.found ? `<button class="text-button" data-open="${escapeHtml(result.parcel.id)}">Open ${escapeHtml(result.parcel.id)} ↗</button>` : ""
    }`;
  } catch (error) {
    handleError(error);
  }
}

async function sortParcels() {
  const mode = $("#sort-mode").value;
  try {
    const response = await runEngine("sort", { mode });
    $("#sort-result").innerHTML = `<div class="result-head light"><strong>${mode[0].toUpperCase() + mode.slice(1)} sort</strong><span class="complexity-pill">${escapeHtml(response.complexity)}</span></div>${response.trace
      .map((step, index) => `<div class="sort-step"><b>PASS ${pad(index + 1)}</b>${(step.values || step.merged).map((value) => `<span>${escapeHtml(value)}</span>`).join("")}</div>`)
      .join("")}`;
  } catch (error) {
    handleError(error);
  }
}

async function dispatchParcel() {
  const button = $("#dispatch-button");
  button.disabled = true;
  try {
    const first = $("#dispatch-queue .queue-card.front");
    if (first && currentView === "dispatch" && !modal.open) {
      first.animate([{ transform: "translateX(0) rotate(-1deg)", opacity: 1 }, { transform: "translateX(180px) rotate(8deg)", opacity: 0 }], { duration: 420, easing: "ease-in", fill: "forwards" });
      await new Promise((resolve) => setTimeout(resolve, 300));
    }
    const response = await runEngine("dispatch");
    renderAll();
    toast(`${response.result.parcel.id} departed ${response.result.parcel.origin}`);
  } catch (error) {
    handleError(error);
  } finally {
    button.disabled = state.dispatch_queue.length === 0;
  }
}

async function advanceParcel(parcelId, button) {
  if (button) button.disabled = true;
  try {
    const response = await runEngine("advance", { id: parcelId });
    renderAll();
    const event = response.result.event;
    toast(`${parcelId} · ${stageLabel(event.status)} · ${event.hub}`);
    if (currentView === "track" && $("#track-result").dataset.parcel === parcelId) renderTrackResult(parcelId);
  } catch (error) {
    handleError(error);
    if (button) button.disabled = false;
  }
}

async function advanceAll() {
  const ids = inflightParcels().map((parcel) => parcel.id);
  $("#advance-all").disabled = true;
  try {
    for (const id of ids) await runEngine("advance", { id });
    renderAll();
    toast(`Advanced ${ids.length} parcel${ids.length === 1 ? "" : "s"} one scan`);
  } catch (error) {
    renderAll();
    handleError(error);
  }
}

async function addTrackingEvent(event, parcelId) {
  event.preventDefault();
  const payload = Object.fromEntries(new FormData(event.currentTarget));
  payload.id = parcelId;
  try {
    const response = await runEngine("track", payload);
    renderAll();
    toast(response.result.rerouted ? `${parcelId} re-routed with BFS via ${payload.hub}` : `Appended node ${response.result.nodes} to ${parcelId}`);
  } catch (error) {
    handleError(error);
  }
}

async function trackParcel(rawId) {
  const id = String(rawId || "").trim().toUpperCase();
  if (!id) return;
  $("#lookup-id").value = id;
  showView("track", `track/${encodeURIComponent(id)}`);
  await renderTrackResult(id);
}

async function renderTrackResult(id) {
  const element = $("#track-result");
  try {
    const response = await runEngine("lookup", { id });
    const result = response.result;
    element.dataset.parcel = result.found ? id : "";
    if (!result.found) {
      element.innerHTML = `<article class="paper-panel track-empty"><h2>No parcel ${escapeHtml(id)}</h2><p>The BST search compared ${result.comparisons} key${result.comparisons === 1 ? "" : "s"} (${result.path.map(escapeHtml).join(" → ") || "empty tree"}) and reached a null child.</p></article>`;
      return;
    }
    const parcel = result.parcel;
    const progress = result.progress;
    const headline =
      parcel.status === "DELIVERED"
        ? `Delivered to ${parcel.recipient}`
        : parcel.status === "READY"
          ? `Waiting at ${parcel.origin} · queue position ${pad(result.queue_position)}`
          : parcel.status === "OUT FOR DELIVERY"
            ? `Out for delivery in ${parcel.destination}`
            : parcel.status === "IN TRANSIT"
              ? `On the way to ${progress.next_hub}`
              : `At ${parcel.location} hub`;
    element.innerHTML = `
      <article class="paper-panel track-card">
        <div class="track-head">
          <div><p class="eyebrow">${escapeHtml(parcel.id)} · ${escapeHtml(parcel.origin)} → ${escapeHtml(parcel.destination)}</p><h2>${escapeHtml(headline)}</h2></div>
          <div class="eta"><small>HOPS LEFT</small><strong>${progress.hops_remaining}</strong></div>
        </div>
        ${stepper(parcel.status)}
        ${routeRail(parcel)}
        <div class="track-split">
          <div><h3 class="mini-heading">Journey <span>linked list, newest first</span></h3>${timeline(parcel)}</div>
          <aside class="track-aside">
            <div><small>RECIPIENT</small><strong>${escapeHtml(parcel.recipient)}</strong></div>
            <div><small>SENDER</small><strong>${escapeHtml(parcel.sender)}</strong></div>
            <div><small>FOUND WITH</small><strong>BST · ${result.comparisons} comparison${result.comparisons === 1 ? "" : "s"}</strong><span class="mono">${result.path.map(escapeHtml).join(" → ")}</span></div>
            <button class="secondary-button" data-open="${escapeHtml(parcel.id)}">Open in operations</button>
          </aside>
        </div>
      </article>`;
  } catch (error) {
    handleError(error);
  }
}

function clearGraph() {
  $$(".graph-node").forEach((node) => node.classList.remove("visited", "current", "waiting"));
  $$(".edges line").forEach((edge) => edge.classList.remove("active"));
  $("#visit-order").innerHTML = "";
}

function edgeBetween(a, b) {
  return $$("[data-edge]").find((line) => {
    const ends = line.dataset.edge.split("|");
    return ends.includes(a) && ends.includes(b);
  });
}

function setGraphButtons(disabled) {
  ["#bfs-button", "#dfs-button", "#route-button"].forEach((selector) => ($(selector).disabled = disabled));
}

async function runTraversal(mode) {
  if (graphRunning) return;
  graphRunning = true;
  setGraphButtons(true);
  const start = $("#graph-start").value;
  clearGraph();
  try {
    const response = await runEngine("graph", { mode, start });
    const order = response.result.order;
    const graph = response.result.graph;
    for (let index = 0; index < response.trace.length; index += 1) {
      const step = response.trace[index];
      $$(".graph-node").forEach((node) => node.classList.remove("current", "waiting"));
      $(`[data-node="${CSS.escape(step.visit)}"]`)?.classList.add("current", "visited");
      const waiting = step.queue || [];
      waiting.forEach((name) => $(`[data-node="${CSS.escape(name)}"]`)?.classList.add("waiting"));
      if (index > 0) {
        // Highlight the traversal-tree edge. BFS: the earliest visited neighbour.
        // DFS: the nearest earlier call one level shallower on the recursion stack.
        const parent =
          mode === "bfs"
            ? order.slice(0, index).find((hub) => graph[hub].includes(step.visit))
            : response.trace.slice(0, index).reverse().find((prior) => prior.depth === step.depth - 1)?.visit;
        edgeBetween(parent, step.visit)?.classList.add("active");
      }
      $("#visit-order").insertAdjacentHTML("beforeend", `<b title="${escapeHtml(step.visit)}">${index + 1}</b>`);
      $("#graph-trace").textContent = mode === "bfs" ? `visit ${step.visit}\nqueue = [${waiting.join(", ")}]` : `visit ${step.visit}\nrecursive depth = ${step.depth}`;
      await new Promise((resolve) => setTimeout(resolve, 520));
    }
    $$(".graph-node").forEach((node) => node.classList.remove("current", "waiting"));
    $("#graph-trace").textContent = `${mode.toUpperCase()} order: ${order.join(" → ")}\n${response.complexity}`;
  } catch (error) {
    handleError(error);
  } finally {
    graphRunning = false;
    setGraphButtons(false);
  }
}

async function findRoute() {
  if (graphRunning) return;
  const start = $("#route-start").value;
  const end = $("#route-end").value;
  try {
    const response = await runEngine("route", { start, end });
    clearGraph();
    const path = response.result.path;
    path.forEach((node) => $(`[data-node="${CSS.escape(node)}"]`)?.classList.add("visited"));
    for (let index = 1; index < path.length; index += 1) edgeBetween(path[index - 1], path[index])?.classList.add("active");
    $("#route-result").textContent = `${path.join(" → ")} · ${response.result.edges} edge${response.result.edges === 1 ? "" : "s"}`;
  } catch (error) {
    handleError(error);
  }
}

async function processExpression(event) {
  event.preventDefault();
  const expression = new FormData(event.currentTarget).get("expression");
  try {
    const response = await runEngine("expression", { expression });
    const result = response.result;
    $("#expression-result").innerHTML = result.balanced
      ? `<div class="output-metric"><div><small>BALANCED</small><strong>TRUE</strong></div><div><small>POSTFIX</small><strong>${escapeHtml(result.postfix)}</strong></div><div><small>RESULT</small><strong>${escapeHtml(result.value)}</strong></div></div>`
      : `<strong>FALSE</strong><br>Parentheses are not balanced. Stack trace stopped at the first mismatch.`;
  } catch (error) {
    handleError(error);
  }
}

async function compareRecursion(event) {
  event.preventDefault();
  const values = new FormData(event.currentTarget).get("values");
  try {
    const response = await runEngine("recursion", { values });
    const result = response.result;
    $("#recursion-result").innerHTML = `<div class="output-metric"><div><small>RECURSIVE</small><strong>${result.recursive_sum}</strong></div><div><small>ITERATIVE</small><strong>${result.iterative_sum}</strong></div><div><small>REVERSED</small><strong>${result.reversed.join(", ")}</strong></div></div><br>${escapeHtml(response.complexity)}`;
  } catch (error) {
    handleError(error);
  }
}

function renderDock() {
  const queue = state.dock_queue;
  $("#dock-ring").innerHTML = queue.slots
    .map((value, index) => `<div class="ring-slot ${queue.size && index === queue.front ? "front" : ""} ${queue.size && index === queue.rear ? "rear" : ""} ${value ? "" : "empty"}"><small>${index + 1}</small>${escapeHtml(value || "EMPTY")}</div>`)
    .join("");
}

async function updateDock(mode) {
  const value = $("#dock-value").value;
  try {
    const response = await runEngine("circular", { mode, value });
    renderDock();
    $("#dock-result").textContent = `${response.result}. ${response.complexity}`;
    if (mode === "enqueue") $("#dock-value").value = "";
  } catch (error) {
    handleError(error);
  }
}

function renderTree(levels = null) {
  if (!levels) return;
  $("#tree-visual").innerHTML = levels.map((level) => `<div class="tree-level">${level.map((value) => `<span class="tree-node">${escapeHtml(value)}</span>`).join("")}</div>`).join("");
}

async function renderTreePreview() {
  try {
    const response = await runEngine("tree", { mode: "traverse", order: "inorder" });
    renderTree(response.result.levels);
  } catch (error) {
    handleError(error);
  }
}

async function updateTree() {
  const mode = $("#tree-operation").value;
  const value = $("#tree-value").value;
  const order = $("#tree-order").value;
  try {
    const response = await runEngine("tree", { mode, value, order });
    renderTree(response.result.levels);
    $("#tree-result").textContent = `${response.result.message}. ${order}: ${response.result.order.join(" → ")}. ${response.complexity}`;
  } catch (error) {
    handleError(error);
  }
}

async function undoAction() {
  try {
    const response = await runEngine("undo");
    renderAll();
    toast(response.result);
    if (currentView === "track" && $("#track-result").dataset.parcel) renderTrackResult($("#track-result").dataset.parcel);
  } catch (error) {
    handleError(error);
  }
}

async function resetData() {
  try {
    await runEngine("reset");
    closeModal();
    renderAll();
    renderTreePreview();
    $("#track-result").innerHTML = "";
    toast("Sample network restored");
  } catch (error) {
    handleError(error);
  }
}
