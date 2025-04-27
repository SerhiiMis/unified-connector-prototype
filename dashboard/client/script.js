const API = "http://127.0.0.1:5000/runs";

async function fetchRuns() {
  const res = await fetch(API);
  const runs = await res.json();
  const tbody = document.querySelector("#runs-table tbody");
  tbody.innerHTML = "";
  runs.forEach((r) => {
    const tr = document.createElement("tr");
    [
      "id",
      "connector",
      "status",
      "started_at",
      "finished_at",
      "message",
    ].forEach((k) => {
      const td = document.createElement("td");
      td.textContent = r[k] ?? "";
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
}

async function trigger(connector) {
  await fetch(API, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ connector }),
  });
  // give it a moment then refresh
  setTimeout(fetchRuns, 1000);
}

// Setup
document.getElementById("run-json").onclick = () => trigger("jsonplaceholder");
document.getElementById("run-binary").onclick = () => trigger("binarylog");
window.onload = fetchRuns;
