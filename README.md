# ParcelFlow

ParcelFlow is a student-scale delivery-network simulator that turns Data Structures and Algorithms lab work into one connected product. The interface registers parcels, processes them in FIFO order, appends tracking events, searches the parcel index, and explores a small hub graph.

The public site is static and works on GitHub Pages. Its business logic still runs in Python: a Web Worker loads the version-pinned Pyodide runtime and executes [`python/engine.py`](python/engine.py) in the browser. Parcel records remain in browser `localStorage` and are never uploaded.

## The parcel flow

```text
Intake ──► Dispatch queue ──► In transit ⇄ At hub ──► Out for delivery ──► Delivered
 (array,     (FIFO dequeue)     (one hop per scan along      (destination        (linked list
  BST, BFS)                      the parcel's BFS route)      hub only)           closed)
```

| Page | What happens |
| --- | --- |
| **Overview** | Live metrics, a pipeline board for every lifecycle stage, the FIFO queue and the latest scan |
| **Intake** | Register a parcel with ID validation, "next free ID", a live BFS route preview and a label modal on success |
| **Parcels** | Filter the register by status or text, open any record in a detail modal, and compare search/sort traces |
| **Dispatch** | Dispatch the front of the queue, then advance each parcel hub by hub (or "Advance all") |
| **Track** | Recipient view: look up an ID (BST search), see the stepper, route rail and full scan history. Deep link: `#track/P101` |
| **Network** | BFS/DFS animation with traversal-tree edges and live parcel counts per hub; fewest-edge route finder |
| **DSA lab** | Stack expressions, recursion, circular queue and BST experiments |

The parcel modal (opened from any ID) shows the lifecycle stepper, route progress, scan history and the scans that are legal from the current state.

### Lifecycle rules (enforced in `engine.py`)

- A queued parcel can only leave through the FIFO dispatch queue — no scans skip the queue.
- `IN TRANSIT → AT HUB`, `AT HUB → IN TRANSIT | OUT FOR DELIVERY`, `OUT FOR DELIVERY → DELIVERED | AT HUB` (failed attempt).
- Out-for-delivery and delivered scans must happen at the destination hub; departures at the current hub.
- An `AT HUB` scan at a hub that is not on the planned route re-plans the rest of the trip with BFS.
- Every change is undoable (snapshot stack, capped at 30 entries).

## Features

- Register parcel records in an array and add their IDs to a custom BST
- Plan each parcel's route with BFS and move it one hop per scan
- Enqueue and dispatch parcels with a manually implemented FIFO queue
- Append status scans to a singly linked tracking history
- Compare linear search, binary search, and BST search traces
- Compare insertion sort, selection sort, and merge sort passes
- Animate BFS and DFS over an adjacency-list hub graph
- Convert infix expressions to postfix, evaluate postfix, and check parentheses with a stack
- Compare iteration with recursion
- Demonstrate a fixed-size circular queue with wrap-around
- Insert, search, delete, and traverse a binary search tree
- Undo recent data changes with a stack

## DSA mapping

| Concept | ParcelFlow use |
| --- | --- |
| Array | Parcel record store and visible register |
| Iteration / recursion | Sum comparison, DFS, merge sort and tree traversals |
| Linear / binary search | Search parcel IDs with comparison traces |
| Insertion / selection / merge sort | Order a copy of the parcel-ID array |
| Stack | Undo history and expression processing |
| Singly linked list | Parcel tracking events |
| Simple queue | FIFO dispatch order |
| Circular queue | Reusable delivery-van dock slots |
| Binary search tree | Unique parcel-ID index and traversal lab |
| Graph | Adjacency-list hub network |
| State machine | Allowed lifecycle transitions per parcel |
| BFS / DFS | Network exploration, per-parcel route planning and re-routing |

The implementations favor clarity and visible traces. For example, the simple queue shifts an array during dequeue so students can discuss its `O(n)` cost, while the circular queue uses `O(1)` front/rear updates.

## Run locally

Pyodide and `engine.py` must be fetched over HTTP, so opening `index.html` as a `file://` URL will not work. Start any static server from the project folder:

```bash
python3 -m http.server 4173
```

Open [http://localhost:4173](http://localhost:4173).

The first visit downloads the version-pinned Pyodide runtime from the official jsDelivr distribution. Later visits normally use the browser cache.

## Test

```bash
python3 -m unittest discover -s tests -v
```

The tests cover manual search and sorting functions, graph traversals, circular-queue wrap-around, all three BST deletion shapes, stack expression evaluation, the register → dispatch → hub scans → delivery workflow, lifecycle guards, BFS re-routing, undo, and migration of data saved by the first version.

## Deploy to GitHub Pages

1. Create an empty GitHub repository.
2. Push this folder to its `main` branch.
3. In **Settings → Pages**, select **GitHub Actions** as the source.
4. The included workflow tests the Python engine and deploys the static site.

GitHub Pages hosts only static files. Pyodide supplies CPython in WebAssembly, so no API server is required.

## Project structure

```text
parcelflow/
├── index.html                 UI structure
├── styles.css                Editorial visual system and motion
├── app.js                    Browser state, rendering and interactions
├── python.worker.js          Pyodide Web Worker bridge
├── python/engine.py          Custom Python DSA implementations
├── tests/test_engine.py      Standard-library unit and workflow tests
└── .github/workflows/pages.yml
```

## Scope

ParcelFlow is an educational simulation. It does not connect to couriers, GPS, payments, customer accounts, or real route-optimization services. Graph edges represent unweighted hub connections.
