const sessionsEl = document.getElementById("sessions");
const refreshEl = document.getElementById("refresh");
const userPillEl = document.getElementById("user-pill");
const socketLabelEl = document.getElementById("socket-label");
const titleEl = document.getElementById("session-title");
const metaEl = document.getElementById("session-meta");
const controlWrapEl = document.getElementById("control-wrap");
const controlToggleEl = document.getElementById("control-toggle");
const terminalEl = document.getElementById("terminal");

const term = new Terminal({
  cursorBlink: true,
  fontFamily:
    'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace',
  fontSize: 13,
  lineHeight: 1.15,
  scrollback: 8000,
  theme: {
    background: "#0c0e12",
    foreground: "#eef2f5",
    cursor: "#57c7a8",
    selectionBackground: "#315f54",
  },
});

term.open(terminalEl);
term.write("Select a tmux session from the left.\r\n");

let websocket = null;
let sessions = [];
let selectedSession = null;
let canWrite = false;

function basePath() {
  return window.location.pathname.endsWith("/")
    ? window.location.pathname
    : `${window.location.pathname}/`;
}

function websocketUrl(sessionName, writable) {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const encoded = encodeURIComponent(sessionName);
  const writeFlag = writable ? "?write=1" : "";
  return `${protocol}//${window.location.host}${basePath()}ws/session/${encoded}${writeFlag}`;
}

async function fetchSessions() {
  const response = await fetch(`${basePath()}api/sessions`, {
    credentials: "same-origin",
  });
  if (!response.ok) {
    throw new Error(`session refresh failed: HTTP ${response.status}`);
  }
  return response.json();
}

function renderSessions() {
  sessionsEl.replaceChildren();
  if (!sessions.length) {
    const empty = document.createElement("p");
    empty.className = "session-subtitle";
    empty.textContent = "No tmux sessions are running.";
    sessionsEl.appendChild(empty);
    return;
  }
  for (const session of sessions) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "session-button";
    if (selectedSession && selectedSession.name === session.name) {
      button.classList.add("active");
    }
    button.addEventListener("click", () => selectSession(session));

    const title = document.createElement("div");
    title.className = "session-title";
    const name = document.createElement("span");
    name.textContent = session.name;
    const dot = document.createElement("span");
    dot.className = "status-dot";
    title.append(name, dot);

    const subtitle = document.createElement("div");
    subtitle.className = "session-subtitle";
    const windows = session.window_count === 1 ? "window" : "windows";
    subtitle.textContent = `${session.window_count} ${windows}`;
    button.append(title, subtitle);
    sessionsEl.appendChild(button);
  }
}

async function refreshSessions() {
  try {
    const data = await fetchSessions();
    sessions = data.sessions || [];
    canWrite = Boolean(data.user && data.user.can_write);
    socketLabelEl.textContent = `tmux -L ${data.tmux_socket}`;
    userPillEl.textContent = data.user
      ? `${data.user.email}${canWrite ? " - admin" : " - read-only"}`
      : "read-only";
    controlWrapEl.classList.toggle("hidden", !canWrite);
    if (selectedSession) {
      selectedSession =
        sessions.find((item) => item.name === selectedSession.name) || null;
    }
    renderSessions();
  } catch (error) {
    term.writeln(`\r\n${error.message}`);
  }
}

function selectSession(session) {
  selectedSession = session;
  renderSessions();
  connectSession();
}

function connectSession() {
  if (!selectedSession) {
    return;
  }
  if (websocket) {
    websocket.close();
    websocket = null;
  }
  term.clear();
  titleEl.textContent = selectedSession.name;
  const windowText =
    selectedSession.window_count === 1 ? "window" : "windows";
  metaEl.textContent = `${selectedSession.window_count} ${windowText}`;
  const writable = canWrite && controlToggleEl.checked;
  websocket = new WebSocket(websocketUrl(selectedSession.name, writable));
  websocket.addEventListener("open", () => {
    resizeTerminal();
    term.focus();
  });
  websocket.addEventListener("message", (event) => {
    try {
      const payload = JSON.parse(event.data);
      if (payload.type === "output") {
        term.write(payload.data || "");
      }
    } catch {
      term.write(event.data);
    }
  });
  websocket.addEventListener("close", () => {
    term.writeln("\r\n[console disconnected]");
  });
}

function terminalGeometry() {
  const rect = terminalEl.getBoundingClientRect();
  const cols = Math.max(20, Math.floor((rect.width - 20) / 8));
  const rows = Math.max(5, Math.floor((rect.height - 20) / 17));
  return { cols, rows };
}

function resizeTerminal() {
  const { cols, rows } = terminalGeometry();
  term.resize(cols, rows);
  if (websocket && websocket.readyState === WebSocket.OPEN) {
    websocket.send(JSON.stringify({ type: "resize", cols, rows }));
  }
}

term.onData((data) => {
  if (!websocket || websocket.readyState !== WebSocket.OPEN) {
    return;
  }
  if (!canWrite || !controlToggleEl.checked) {
    return;
  }
  websocket.send(JSON.stringify({ type: "input", data }));
});

refreshEl.addEventListener("click", refreshSessions);
controlToggleEl.addEventListener("change", connectSession);
window.addEventListener("resize", resizeTerminal);

refreshSessions();
