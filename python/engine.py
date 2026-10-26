"""ParcelFlow's teaching-oriented data-structure engine.

The module intentionally implements the core algorithms instead of delegating to
Python's queue, sorting, searching, tree, or graph helper libraries.  It can run
under CPython for tests and under Pyodide for the GitHub Pages interface.
"""

from __future__ import annotations

import copy
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


# Adjacency-list representation of the delivery network.  Each dictionary key
# is a vertex (hub), and its list contains the vertices joined by direct edges.
# Because no edge has a distance or cost, this is an *unweighted* graph.
HUB_GRAPH = {
    "Mumbai Central": ["Andheri", "Bandra", "Navi Mumbai"],
    "Andheri": ["Mumbai Central", "Powai"],
    "Bandra": ["Mumbai Central", "Powai", "Thane"],
    "Powai": ["Andheri", "Bandra", "Thane"],
    "Thane": ["Bandra", "Powai", "Navi Mumbai"],
    "Navi Mumbai": ["Mumbai Central", "Thane"],
}


# Parcel lifecycle.  Each status lists the statuses a scan may move it to.
# READY -> IN TRANSIT happens only through the FIFO dispatch queue.
LIFECYCLE = ["REGISTERED", "READY", "IN TRANSIT", "AT HUB", "OUT FOR DELIVERY", "DELIVERED"]
TRANSITIONS: dict[str, set[str]] = {
    "REGISTERED": {"READY"},
    "READY": set(),
    "IN TRANSIT": {"AT HUB"},
    "AT HUB": {"IN TRANSIT", "OUT FOR DELIVERY"},
    "OUT FOR DELIVERY": {"DELIVERED", "AT HUB"},
    "DELIVERED": set(),
}
SCAN_STATUSES = ["IN TRANSIT", "AT HUB", "OUT FOR DELIVERY", "DELIVERED"]


def _now() -> str:
    """Return the current UTC time in JSON-friendly ISO 8601 text form.

    This is timestamp plumbing, not a DSA algorithm.  Removing microseconds
    keeps tracking records short and stable when displayed by JavaScript.
    """
    # ``now(timezone.utc)`` creates an aware UTC datetime; ``isoformat`` turns
    # it into text that can safely cross the Python/JavaScript JSON boundary.
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _event(status: str, hub: str, note: str) -> dict[str, str]:
    """Build one tracking-event record for the parcel's linked-list history."""
    # A plain dictionary is the serializable value stored inside each node.
    return {"status": status, "hub": hub, "note": note, "time": _now()}


def default_state() -> dict[str, Any]:
    """Create the sample application state used on first load or reset.

    The returned dictionary is application setup data rather than an
    algorithm.  It contains the array of parcels, the FIFO dispatch queue,
    circular dock queue, BST demonstration values, and undo stack.
    """
    # These four dictionaries are example parcel records for the teaching UI.
    # Their different statuses make every lifecycle view visible immediately.
    parcels = [
        {
            "id": "P101",
            "sender": "Mira Shah",
            "recipient": "Rohan Mehta",
            "origin": "Mumbai Central",
            "destination": "Thane",
            "status": "READY",
            "created_at": "2026-10-24T08:30:00+00:00",
            "events": [
                {
                    "status": "REGISTERED",
                    "hub": "Mumbai Central",
                    "note": "Parcel accepted at origin hub",
                    "time": "2026-10-24T08:30:00+00:00",
                },
                {
                    "status": "READY",
                    "hub": "Mumbai Central",
                    "note": "Added to dispatch queue",
                    "time": "2026-10-24T08:31:00+00:00",
                },
            ],
        },
        {
            "id": "P102",
            "sender": "Kabir Rao",
            "recipient": "Ananya Iyer",
            "origin": "Bandra",
            "destination": "Navi Mumbai",
            "status": "READY",
            "created_at": "2026-10-24T08:36:00+00:00",
            "events": [
                {
                    "status": "REGISTERED",
                    "hub": "Bandra",
                    "note": "Parcel accepted at origin hub",
                    "time": "2026-10-24T08:36:00+00:00",
                },
                {
                    "status": "READY",
                    "hub": "Bandra",
                    "note": "Added to dispatch queue",
                    "time": "2026-10-24T08:37:00+00:00",
                },
            ],
        },
        {
            "id": "P103",
            "sender": "Ishaan Patel",
            "recipient": "Neha Joshi",
            "origin": "Andheri",
            "destination": "Powai",
            "status": "DELIVERED",
            "created_at": "2026-10-24T07:55:00+00:00",
            "events": [
                {
                    "status": "REGISTERED",
                    "hub": "Andheri",
                    "note": "Parcel accepted at origin hub",
                    "time": "2026-10-24T07:55:00+00:00",
                },
                {
                    "status": "IN TRANSIT",
                    "hub": "Andheri",
                    "note": "Dispatched toward Powai",
                    "time": "2026-10-24T08:05:00+00:00",
                },
                {
                    "status": "DELIVERED",
                    "hub": "Powai",
                    "note": "Handed to recipient",
                    "time": "2026-10-24T08:25:00+00:00",
                },
            ],
        },
        {
            "id": "P099",
            "sender": "Sara Khan",
            "recipient": "Vikram Desai",
            "origin": "Andheri",
            "destination": "Thane",
            "status": "AT HUB",
            "created_at": "2026-10-24T07:40:00+00:00",
            "events": [
                {
                    "status": "REGISTERED",
                    "hub": "Andheri",
                    "note": "Parcel accepted at origin hub",
                    "time": "2026-10-24T07:40:00+00:00",
                },
                {
                    "status": "READY",
                    "hub": "Andheri",
                    "note": "Added to dispatch queue",
                    "time": "2026-10-24T07:41:00+00:00",
                },
                {
                    "status": "IN TRANSIT",
                    "hub": "Andheri",
                    "note": "Departed Andheri toward Powai",
                    "time": "2026-10-24T07:50:00+00:00",
                },
                {
                    "status": "AT HUB",
                    "hub": "Powai",
                    "note": "Arrived at Powai",
                    "time": "2026-10-24T08:15:00+00:00",
                },
            ],
        },
    ]
    # Add route/location fields consistently, including to older-shaped data.
    for parcel in parcels:
        _normalize_parcel(parcel)
    # Group every independent data structure into one JSON-serializable state.
    return {
        "parcels": parcels,
        "dispatch_queue": ["P101", "P102"],
        "dock_queue": {
            "capacity": 5,
            "slots": ["VAN-07", "VAN-12", None, None, None],
            "front": 0,
            "rear": 1,
            "size": 2,
        },
        "bst_demo": [104, 102, 106, 101, 103, 105, 107],
        "undo_stack": [],
    }


class ArrayQueue:
    """Teaching FIFO queue backed by a Python list.

    FIFO means first in, first out.  Enqueue appends in amortized O(1) time.
    Dequeue deliberately shifts all remaining elements, so it is O(n); a
    production queue would normally use ``collections.deque`` for O(1).
    """

    def __init__(self, values: list[str] | None = None) -> None:
        """Copy optional starting values so callers keep their own list."""
        # ``or []`` also handles ``None``; ``list`` prevents aliasing.
        self.items = list(values or [])

    def enqueue(self, value: str) -> None:
        """Add ``value`` at the rear of the queue (amortized O(1))."""
        # Appending preserves FIFO order because dequeue reads index zero.
        self.items.append(value)

    def dequeue(self) -> str:
        """Remove and return the front value by manually shifting the array."""
        # Underflow means a removal was requested from an empty queue.
        if not self.items:
            raise ValueError("Dispatch queue is empty")
        # Save the first item before later writes overwrite index zero.
        value = self.items[0]
        # Shift each remaining item one place left: O(n) work.
        for index in range(1, len(self.items)):
            self.items[index - 1] = self.items[index]
        # The final slot is now duplicated, so remove it.
        self.items.pop()
        # Return the item that entered the queue earliest.
        return value


class CircularQueue:
    """Fixed-size FIFO queue whose indices wrap around with modulo arithmetic.

    Unlike ``ArrayQueue``, both enqueue and dequeue are O(1) because values are
    never shifted.  ``front``, ``rear`` and ``size`` distinguish full/empty
    states while the same fixed array slots are reused.
    """

    def __init__(self, data: dict[str, Any]) -> None:
        """Restore a circular queue from its JSON-serializable dictionary."""
        # Cast numeric fields defensively because data crossed a JSON boundary.
        self.capacity = int(data["capacity"])
        # Copy the slots so queue operations do not mutate the caller's object.
        self.slots = list(data["slots"])
        self.front = int(data["front"])
        self.rear = int(data["rear"])
        self.size = int(data["size"])

    def enqueue(self, value: str) -> int:
        """Insert at the next rear slot and return its zero-based index."""
        # Reject overflow before overwriting a live value.
        if self.size == self.capacity:
            raise ValueError("Circular queue overflow: every dock is occupied")
        # Modulo maps the index after the last slot back to slot zero.
        self.rear = (self.rear + 1) % self.capacity
        # Store the new van label at the calculated rear position.
        self.slots[self.rear] = value
        # In an empty queue, the new rear is also the first/front element.
        if self.size == 0:
            self.front = self.rear
        # Record one additional occupied slot and report where it was placed.
        self.size += 1
        return self.rear

    def dequeue(self) -> tuple[str, int]:
        """Remove the front item in O(1) time and return value plus slot index."""
        # Reject underflow because no front item exists when size is zero.
        if self.size == 0:
            raise ValueError("Circular queue underflow: no van is waiting")
        # Remember both the index and value before clearing the slot.
        index = self.front
        value = self.slots[index]
        self.slots[index] = None
        self.size -= 1
        # Reset to the documented empty-state indices after removing the last item.
        if self.size == 0:
            self.front = 0
            self.rear = -1
        else:
            # Otherwise advance front, wrapping around at capacity.
            self.front = (self.front + 1) % self.capacity
        # Labels are displayed as text; ``index`` lets the UI identify the dock.
        return str(value), index

    def data(self) -> dict[str, Any]:
        """Return a JSON-serializable snapshot of the circular queue."""
        # This is serialization plumbing used to save the queue in app state.
        return {
            "capacity": self.capacity,
            "slots": self.slots,
            "front": self.front,
            "rear": self.rear,
            "size": self.size,
        }


@dataclass
class LinkedNode:
    """One node in a singly linked tracking history.

    ``value`` stores an event; ``next`` points only forward to the next node.
    The dataclass decorator generates the routine initializer automatically.
    """

    value: dict[str, Any]
    next: "LinkedNode | None" = None


class EventLinkedList:
    """Singly linked list with head and tail pointers for tracking events."""

    def __init__(self, values: list[dict[str, Any]] | None = None) -> None:
        """Create an empty list, then append any supplied events in order."""
        # Head is the oldest node; tail is the newest node.
        self.head: LinkedNode | None = None
        self.tail: LinkedNode | None = None
        # Reusing append maintains all pointer invariants for initial values.
        for value in values or []:
            self.append(value)

    def append(self, value: dict[str, Any]) -> None:
        """Append an event in O(1) time using the stored tail pointer."""
        # The new node initially has no successor.
        node = LinkedNode(value)
        # The first node becomes both the head and the tail.
        if self.head is None:
            self.head = self.tail = node
        else:
            # A non-empty list must have a tail; the assertion documents that invariant.
            assert self.tail is not None
            # Link the old tail forward, then move the tail pointer to the new node.
            self.tail.next = node
            self.tail = node

    def to_list(self) -> list[dict[str, Any]]:
        """Traverse from head to tail and return event values in O(n) time."""
        # JSON cannot encode custom nodes, so collect only their dictionaries.
        values: list[dict[str, Any]] = []
        current = self.head
        # Follow one ``next`` pointer per node until the end marker ``None``.
        while current is not None:
            values.append(current.value)
            current = current.next
        return values


@dataclass
class TreeNode:
    """A binary-search-tree node with at most one smaller and one larger child."""

    key: Any
    left: "TreeNode | None" = None
    right: "TreeNode | None" = None


class BinarySearchTree:
    """Unbalanced binary search tree used for parcel IDs and the DSA lab.

    For tree height ``h``, insert/search/delete are O(h): usually O(log n) in
    a balanced-looking tree, but O(n) in the worst-case skewed tree.
    """

    def __init__(self, values: list[Any] | None = None) -> None:
        """Start with an empty root and insert optional keys one at a time."""
        self.root: TreeNode | None = None
        # Calling insert applies the same duplicate and ordering rules to all keys.
        for value in values or []:
            self.insert(value)

    def insert(self, key: Any) -> list[Any]:
        """Insert one unique key and return the comparison path, O(h)."""
        # The path is educational trace data for the browser.
        path: list[Any] = []
        # An empty tree makes the first key its root.
        if self.root is None:
            self.root = TreeNode(key)
            return [key]
        # Walk downward until the ordering rule reveals an empty child slot.
        current = self.root
        while True:
            path.append(current.key)
            # This BST models unique parcel IDs, so equal keys are invalid.
            if key == current.key:
                raise ValueError(f"Duplicate key: {key}")
            # Smaller keys belong in the left subtree.
            if key < current.key:
                if current.left is None:
                    current.left = TreeNode(key)
                    path.append(key)
                    return path
                current = current.left
            else:
                # Larger keys belong in the right subtree.
                if current.right is None:
                    current.right = TreeNode(key)
                    path.append(key)
                    return path
                current = current.right

    def search(self, key: Any) -> tuple[bool, list[Any]]:
        """Find ``key`` by following BST ordering and return its path, O(h)."""
        path: list[Any] = []
        current = self.root
        # Each comparison discards one entire subtree.
        while current is not None:
            path.append(current.key)
            if key == current.key:
                return True, path
            # Choose left for a smaller target and right for a larger target.
            current = current.left if key < current.key else current.right
        # Reaching a missing child proves the key is absent.
        return False, path

    def delete(self, key: Any) -> bool:
        """Delete ``key`` with the standard three BST cases, O(h)."""
        # The nested recursive helper changes this flag when it finds the key.
        deleted = False

        def remove(node: TreeNode | None, target: Any) -> TreeNode | None:
            """Return the root that should replace this subtree after deletion."""
            nonlocal deleted
            # A missing subtree means the target was not found on this route.
            if node is None:
                return None
            # Use BST ordering to recurse into only one subtree.
            if target < node.key:
                node.left = remove(node.left, target)
            elif target > node.key:
                node.right = remove(node.right, target)
            else:
                # Case found: record success before choosing a replacement.
                deleted = True
                # Zero/one-child case: the existing child can replace this node.
                if node.left is None:
                    return node.right
                if node.right is None:
                    return node.left
                # Two-child case: choose the smallest key in the right subtree.
                successor = node.right
                while successor.left is not None:
                    successor = successor.left
                # Copy that inorder successor key, then remove its old node.
                node.key = successor.key
                node.right = remove_successor(node.right, successor.key)
            # Return the possibly updated subtree root to the caller.
            return node

        def remove_successor(node: TreeNode | None, target: Any) -> TreeNode | None:
            """Remove a known inorder successor without changing ``deleted``."""
            if node is None:
                return None
            # The successor is the leftmost node in this subtree.
            if target < node.key:
                node.left = remove_successor(node.left, target)
                return node
            # A leftmost successor has no left child; promote its right child.
            return node.right

        # Replace the root too, because deleting the root may promote a child.
        self.root = remove(self.root, key)
        return deleted

    def traverse(self, order: str) -> list[Any]:
        """Return preorder, inorder, or postorder keys using recursive DFS.

        Every node is visited once: O(n) time and O(h) call-stack space.
        Inorder traversal of a BST produces keys in sorted order.
        """
        result: list[Any] = []

        def visit(node: TreeNode | None) -> None:
            """Visit this subtree, placing the root at the requested stage."""
            # ``None`` is the recursion base case after a leaf.
            if node is None:
                return
            # Preorder: root before left and right subtrees.
            if order == "preorder":
                result.append(node.key)
            visit(node.left)
            # Inorder: left subtree, root, then right subtree.
            if order == "inorder":
                result.append(node.key)
            visit(node.right)
            # Postorder: both subtrees before their root.
            if order == "postorder":
                result.append(node.key)

        # Start the recursive traversal at the root.
        visit(self.root)
        return result

    def levels(self) -> list[list[Any]]:
        """Group tree keys by depth using iterative breadth-first search."""
        # An empty tree has no levels.
        if self.root is None:
            return []
        result: list[list[Any]] = []
        # Queue entries carry both a node and its zero-based depth.
        queue: list[tuple[TreeNode, int]] = [(self.root, 0)]
        # A moving front index avoids O(n) ``pop(0)`` shifts.
        front = 0
        while front < len(queue):
            node, level = queue[front]
            front += 1
            # First encounter of a depth creates its output bucket.
            if level == len(result):
                result.append([])
            result[level].append(node.key)
            # Enqueue children for the next level, left before right.
            if node.left:
                queue.append((node.left, level + 1))
            if node.right:
                queue.append((node.right, level + 1))
        return result


def linear_search(values: list[Any], target: Any) -> dict[str, Any]:
    """Scan left to right for ``target`` and expose every comparison.

    Time is O(n) in the worst case and no sorted input is required.
    """
    # Trace entries let students see the exact inspected index/value pairs.
    trace = []
    for index in range(len(values)):
        trace.append({"index": index, "value": values[index]})
        # Stop immediately when equality succeeds.
        if values[index] == target:
            return {"found": True, "index": index, "comparisons": len(trace), "trace": trace}
    # Exhausting the array means the target is absent.
    return {"found": False, "index": -1, "comparisons": len(trace), "trace": trace}


def binary_search(values: list[Any], target: Any) -> dict[str, Any]:
    """Search a sorted array by repeatedly halving its candidate range.

    Time is O(log n), but correctness requires ``values`` to be sorted first.
    """
    # ``low`` and ``high`` delimit the still-possible inclusive interval.
    low, high = 0, len(values) - 1
    trace = []
    while low <= high:
        # Integer division chooses the middle valid index.
        mid = (low + high) // 2
        trace.append({"low": low, "mid": mid, "high": high, "value": values[mid]})
        if values[mid] == target:
            return {"found": True, "index": mid, "comparisons": len(trace), "trace": trace}
        # Discard the half that cannot contain the target.
        if values[mid] < target:
            low = mid + 1
        else:
            high = mid - 1
    return {"found": False, "index": -1, "comparisons": len(trace), "trace": trace}


def insertion_sort(values: list[Any]) -> tuple[list[Any], list[dict[str, Any]]]:
    """Build a sorted prefix by inserting each key into its proper position.

    Worst/average time is O(n²), best time is O(n), and extra space is O(n)
    here because the input is copied (the in-place algorithm itself is O(1)).
    """
    # Never reorder the caller's list; sort a working copy.
    data = list(values)
    trace = []
    # A one-item prefix is already sorted, so begin at index one.
    for index in range(1, len(data)):
        key = data[index]
        cursor = index - 1
        # Shift larger prefix values right to open the insertion slot.
        while cursor >= 0 and data[cursor] > key:
            data[cursor + 1] = data[cursor]
            cursor -= 1
        # Insert the saved key after the last value not greater than it.
        data[cursor + 1] = key
        trace.append({"pass": index, "values": list(data)})
    return data, trace


def selection_sort(values: list[Any]) -> tuple[list[Any], list[dict[str, Any]]]:
    """Repeatedly select the smallest suffix value for the next position.

    It always performs O(n²) comparisons.  The copied working array makes the
    function's total extra space O(n), although selection sort itself is in-place.
    """
    data = list(values)
    trace = []
    # Each pass permanently fills position ``start``.
    for start in range(len(data) - 1):
        smallest = start
        # Find the smallest value in the unsorted suffix.
        for cursor in range(start + 1, len(data)):
            if data[cursor] < data[smallest]:
                smallest = cursor
        # Swap the selected minimum into the next output position.
        data[start], data[smallest] = data[smallest], data[start]
        trace.append({"pass": start + 1, "selected": smallest, "values": list(data)})
    return data, trace


def merge_sort(values: list[Any]) -> tuple[list[Any], list[dict[str, Any]]]:
    """Sort with divide-and-conquer merge sort: O(n log n) time, O(n) space."""
    # Trace records each merge for the teaching animation.
    trace: list[dict[str, Any]] = []

    def divide(data: list[Any], depth: int) -> list[Any]:
        """Recursively sort ``data`` and report the recursion depth."""
        # Lists of length zero or one are already sorted (base case).
        if len(data) <= 1:
            return data
        # Divide into two roughly equal halves.
        middle = len(data) // 2
        left = divide(data[:middle], depth + 1)
        right = divide(data[middle:], depth + 1)
        # Merge the two sorted results with one cursor per half.
        merged: list[Any] = []
        i = j = 0
        while i < len(left) and j < len(right):
            # Taking the smaller front value preserves sorted order.
            if left[i] <= right[j]:
                merged.append(left[i])
                i += 1
            else:
                merged.append(right[j])
                j += 1
        # Exactly one half may have unconsumed values; append both slices safely.
        merged.extend(left[i:])
        merged.extend(right[j:])
        trace.append({"depth": depth, "left": left, "right": right, "merged": list(merged)})
        return merged

    # Copy the input before the first recursive division.
    return divide(list(values), 0), trace


def graph_bfs(start: str) -> tuple[list[str], list[dict[str, Any]]]:
    """Traverse the hub graph level by level with breadth-first search.

    Complexity is O(V + E): every reachable vertex and edge is examined once.
    The queue controls visit order; the set prevents duplicate visits.
    """
    # Reject names that are not vertices in the adjacency list.
    if start not in HUB_GRAPH:
        raise ValueError("Choose a valid start hub")
    # Enqueue the start and mark it immediately so it cannot be queued twice.
    queue = [start]
    front = 0
    visited = {start}
    order: list[str] = []
    trace: list[dict[str, Any]] = []
    # A moving front gives queue-like O(1) reads without shifting the list.
    while front < len(queue):
        node = queue[front]
        front += 1
        order.append(node)
        # Discover every unvisited neighbor and place it at the queue rear.
        for neighbor in HUB_GRAPH[node]:
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)
        trace.append({"visit": node, "queue": queue[front:], "visited": list(order)})
    return order, trace


def graph_dfs(start: str) -> tuple[list[str], list[dict[str, Any]]]:
    """Traverse the hub graph deeply with recursive depth-first search.

    Complexity is O(V + E), with O(V) worst-case recursion/visited space.
    """
    if start not in HUB_GRAPH:
        raise ValueError("Choose a valid start hub")
    # A set supplies average O(1) membership checks for already visited hubs.
    visited: set[str] = set()
    order: list[str] = []
    trace: list[dict[str, Any]] = []

    def explore(node: str, depth: int) -> None:
        """Visit one node, then recursively explore each unvisited neighbor."""
        # Mark before recursion to prevent cycles from revisiting this vertex.
        visited.add(node)
        order.append(node)
        trace.append({"visit": node, "depth": depth, "visited": list(order)})
        # Recursion acts as the DFS stack; ``depth`` is only for visualization.
        for neighbor in HUB_GRAPH[node]:
            if neighbor not in visited:
                explore(neighbor, depth + 1)

    # Seed DFS at the selected hub.
    explore(start, 0)
    return order, trace


def shortest_path(start: str, end: str) -> list[str]:
    """Return a fewest-edge route in the unweighted graph using BFS.

    This is shortest by *number of hops*, not geographic distance, travel time,
    or weighted cost.  BFS provides that guarantee only because every edge is
    treated equally.  Complexity is O(V + E).
    """
    if start not in HUB_GRAPH or end not in HUB_GRAPH:
        raise ValueError("Choose valid route hubs")
    # The queue explores all one-hop paths before two-hop paths, and so on.
    queue = [start]
    front = 0
    # ``parent`` both marks discovery and remembers how to reconstruct the path.
    parent: dict[str, str | None] = {start: None}
    while front < len(queue):
        node = queue[front]
        front += 1
        # The first time BFS reaches the end is a fewest-edge arrival.
        if node == end:
            break
        # Record each neighbor's predecessor only on first discovery.
        for neighbor in HUB_GRAPH[node]:
            if neighbor not in parent:
                parent[neighbor] = node
                queue.append(neighbor)
    # This guard supports disconnected graphs, even though today's graph is connected.
    if end not in parent:
        return []
    # Walk parent pointers backward from destination to origin.
    path = []
    cursor: str | None = end
    while cursor is not None:
        path.append(cursor)
        cursor = parent[cursor]
    # Reverse the backward chain into travel order.
    return list(reversed(path))


def _tokens(expression: str) -> list[str]:
    """Tokenize numbers, parentheses, and supported arithmetic operators."""
    # Spaces are irrelevant in the lab's small arithmetic grammar.
    compact = expression.replace(" ", "")
    # The regular expression recognizes integers/decimals or one operator symbol.
    tokens = re.findall(r"\d+(?:\.\d+)?|[()+\-*/^]", compact)
    # Rejoining must reproduce the input; otherwise unsupported text was skipped.
    if "".join(tokens) != compact:
        raise ValueError("Expression contains an unsupported token")
    return tokens


def balanced_parentheses(expression: str) -> tuple[bool, list[dict[str, Any]]]:
    """Check matching round parentheses with a LIFO stack in O(n) time."""
    # The stack holds unmatched opening parentheses.
    stack: list[str] = []
    trace = []
    # Read characters left to right; non-parenthesis characters need no action.
    for token in expression:
        if token == "(":
            # Opening delimiters wait on the stack for a later close.
            stack.append(token)
            trace.append({"token": token, "operation": "push", "stack": list(stack)})
        elif token == ")":
            # Closing with an empty stack is underflow and therefore unbalanced.
            if not stack:
                trace.append({"token": token, "operation": "underflow", "stack": []})
                return False, trace
            # LIFO pop pairs this close with the most recent unmatched open.
            stack.pop()
            trace.append({"token": token, "operation": "pop", "stack": list(stack)})
    # Any openings left after the scan are also unmatched.
    return len(stack) == 0, trace


def infix_to_postfix(expression: str) -> tuple[list[str], list[dict[str, Any]]]:
    """Convert infix arithmetic to postfix with an operator stack.

    This is a teaching version of the shunting-yard idea.  Each token is pushed
    and popped at most once, so time and auxiliary space are O(n).
    """
    # Larger numbers mean the operator should be applied earlier.
    precedence = {"+": 1, "-": 1, "*": 2, "/": 2, "^": 3}
    # Operands go to output; pending operators wait on a LIFO stack.
    output: list[str] = []
    operators: list[str] = []
    trace = []
    for token in _tokens(expression):
        # Numbers can be emitted immediately in postfix order.
        if re.fullmatch(r"\d+(?:\.\d+)?", token):
            output.append(token)
        elif token == "(":
            # An opening parenthesis creates a precedence boundary.
            operators.append(token)
        elif token == ")":
            # Pop the whole parenthesized group until its opening marker.
            while operators and operators[-1] != "(":
                output.append(operators.pop())
            # No opening marker means the original expression was unbalanced.
            if not operators:
                raise ValueError("Parentheses are not balanced")
            # Discard the matched opening marker; parentheses are not output.
            operators.pop()
        else:
            # Move equal/higher-precedence pending operators to output first.
            while (
                operators
                and operators[-1] != "("
                and precedence[operators[-1]] >= precedence[token]
            ):
                output.append(operators.pop())
            operators.append(token)
        # Copy both lists so later mutations do not alter earlier trace frames.
        trace.append({"token": token, "output": list(output), "stack": list(operators)})
    # Flush operators that remain after all input tokens.
    while operators:
        operator = operators.pop()
        if operator == "(":
            raise ValueError("Parentheses are not balanced")
        output.append(operator)
    return output, trace


def evaluate_postfix(tokens: list[str]) -> tuple[float, list[dict[str, Any]]]:
    """Evaluate postfix tokens using an operand stack in O(n) time."""
    # Numbers wait on this LIFO stack until an operator consumes them.
    stack: list[float] = []
    trace = []
    for token in tokens:
        # An operand becomes immediately available to a later operator.
        if re.fullmatch(r"\d+(?:\.\d+)?", token):
            stack.append(float(token))
            operation = "push operand"
        else:
            # Every supported operator is binary and therefore needs two operands.
            if len(stack) < 2:
                raise ValueError("Invalid expression")
            # Pop right before left because subtraction/division are order-sensitive.
            right = stack.pop()
            left = stack.pop()
            # Calculate all supported alternatives, then select the current token.
            operations = {
                "+": left + right,
                "-": left - right,
                "*": left * right,
                "/": left / right,
                "^": left**right,
            }
            stack.append(operations[token])
            operation = f"apply {token}"
        trace.append({"token": token, "operation": operation, "stack": list(stack)})
    # A valid expression reduces to exactly one finite numeric result.
    if len(stack) != 1 or not math.isfinite(stack[0]):
        raise ValueError("Expression did not reduce to one finite result")
    return stack[0], trace


def recursive_sum(values: list[int]) -> tuple[int, list[dict[str, Any]]]:
    """Sum an array recursively and return a call/return trace.

    Time is O(n); the recursive call stack uses O(n) space.
    """
    trace: list[dict[str, Any]] = []

    def add(index: int) -> int:
        """Return the sum from ``index`` onward."""
        trace.append({"call": f"sum({index})", "remaining": values[index:]})
        # Past the final item, the additive identity zero ends recursion.
        if index == len(values):
            return 0
        # Add this item to the recursively computed suffix total.
        result = values[index] + add(index + 1)
        trace.append({"return": result, "index": index})
        return result

    # Start with the whole array at index zero.
    return add(0), trace


def _normalize_parcel(parcel: dict[str, Any]) -> dict[str, Any]:
    """Add route fields to parcels saved by older versions of the app.

    This migration helper is application plumbing.  It calls BFS only when an
    older record has no route; its other checks supply missing state fields.
    """
    # Old v1 records did not store their BFS route.
    if not parcel.get("route"):
        parcel["route"] = shortest_path(parcel["origin"], parcel["destination"])
    # Events are stored oldest to newest; the last event reveals current location.
    events = parcel.get("events") or []
    if "location" not in parcel:
        parcel["location"] = events[-1]["hub"] if events else parcel["origin"]
    # ``hop`` is the current location's index on the planned route.
    if "hop" not in parcel:
        route = parcel["route"]
        parcel["hop"] = route.index(parcel["location"]) if parcel["location"] in route else 0
    return parcel


def _normalize(state: dict[str, Any]) -> dict[str, Any]:
    """Migrate every parcel and ensure the state has an undo stack."""
    # Normalize records independently so mixed-version saved data still loads.
    for parcel in state.get("parcels", []):
        _normalize_parcel(parcel)
    # ``setdefault`` preserves an existing stack and creates only a missing one.
    state.setdefault("undo_stack", [])
    return state


def progress(parcel: dict[str, Any]) -> dict[str, Any]:
    """Summarise where a parcel is in its route and lifecycle for the UI.

    This performs constant-time index/arithmetic work apart from copying the
    small fixed lifecycle slice; it is presentation plumbing, not a new DSA.
    """
    # Read the saved route, hop pointer, and lifecycle state once.
    route = parcel["route"]
    hop = int(parcel.get("hop", 0))
    status = parcel["status"]
    # The next hub exists only when another route edge remains.
    next_hub = route[hop + 1] if hop + 1 < len(route) else None
    remaining = max(0, len(route) - 1 - hop)
    # Map a status name to its display step; unknown legacy data falls back to zero.
    stage = LIFECYCLE.index(status) if status in LIFECYCLE else 0
    return {
        "stage": stage,
        "stages": LIFECYCLE[1:],
        "location": parcel.get("location"),
        "next_hub": next_hub,
        "hops_total": len(route) - 1,
        "hops_remaining": remaining if status != "DELIVERED" else 0,
        "next_action": next_action(parcel),
    }


def next_action(parcel: dict[str, Any]) -> str | None:
    """Describe the legal next operational step for the current status."""
    # Route/hop are needed only for transit and hub departure descriptions.
    status = parcel["status"]
    route, hop = parcel["route"], int(parcel.get("hop", 0))
    # Each branch mirrors the finite-state transition rules near the module top.
    if status == "READY":
        return "Wait for dispatch (FIFO)"
    if status == "IN TRANSIT":
        return f"Arrive at {route[min(hop + 1, len(route) - 1)]}"
    if status == "AT HUB":
        if parcel["location"] == parcel["destination"]:
            return "Send out for delivery"
        return f"Depart toward {route[hop + 1]}"
    if status == "OUT FOR DELIVERY":
        return "Mark delivered"
    return None


def _scan(parcel: dict[str, Any], status: str, hub: str, note: str) -> None:
    """Append one tracking node and update the parcel's current state.

    Rebuilding from serialized events is O(n), while the actual linked-list tail
    append is O(1).  Conversion back to a list is required for JSON storage.
    """
    # Reconstruct the linked list from the JSON-friendly event array.
    linked = EventLinkedList(parcel["events"])
    # Append at the tail, then serialize the chain back into application state.
    linked.append(_event(status, hub, note))
    parcel["events"] = linked.to_list()
    # Keep quick-access parcel fields synchronized with the newest event.
    parcel["status"] = status
    parcel["location"] = hub


def _set_location(parcel: dict[str, Any], hub: str) -> bool:
    """Move the hop pointer to ``hub``; re-plan with BFS when it is off-route.

    Returns ``True`` only when rerouting occurred.  Route lookup is O(r), where
    r is route length; rerouting additionally costs BFS O(V + E).
    """
    # On-route arrival only needs the existing route index.
    if hub in parcel["route"]:
        parcel["hop"] = parcel["route"].index(hub)
        return False
    # Preserve already travelled hubs, then join a new fewest-hop BFS suffix.
    travelled = parcel["route"][: int(parcel.get("hop", 0)) + 1]
    parcel["route"] = travelled + shortest_path(hub, parcel["destination"])
    parcel["hop"] = len(travelled)
    return True


def advance_parcel(parcel: dict[str, Any]) -> str:
    """Apply the next automatic legal scan for a dispatched parcel.

    This is a finite-state-machine operation.  Route-pointer moves are O(1);
    event serialization inside ``_scan`` is O(n) for n saved events.
    """
    status = parcel["status"]
    route, hop = parcel["route"], int(parcel.get("hop", 0))
    # Queued and completed parcels have no automatic operational scan.
    if status == "READY":
        raise ValueError(f"{parcel['id']} is still in the dispatch queue; dispatch it first")
    if status == "DELIVERED":
        raise ValueError(f"{parcel['id']} has already been delivered")
    # Transit arrives at the next route index.
    if status == "IN TRANSIT":
        parcel["hop"] = hop + 1
        hub = route[hop + 1]
        _scan(parcel, "AT HUB", hub, f"Arrived at {hub}")
    elif status == "AT HUB":
        # Destination hubs move to last-mile delivery; other hubs depart onward.
        if parcel["location"] == parcel["destination"]:
            _scan(parcel, "OUT FOR DELIVERY", parcel["destination"], f"With courier for {parcel['recipient']}")
        else:
            _scan(parcel, "IN TRANSIT", parcel["location"], f"Departed {parcel['location']} toward {route[hop + 1]}")
    elif status == "OUT FOR DELIVERY":
        # The final automatic scan closes the lifecycle.
        _scan(parcel, "DELIVERED", parcel["destination"], f"Handed to {parcel['recipient']}")
    return parcel["status"]


def _parcel(state: dict[str, Any], parcel_id: str) -> dict[str, Any]:
    """Find a parcel dictionary by ID with linear search, O(n)."""
    # The main parcel store is an array, so inspect records in order.
    for parcel in state["parcels"]:
        if parcel["id"] == parcel_id:
            return parcel
    raise ValueError(f"Parcel {parcel_id} was not found")


UNDO_LIMIT = 30


def _response(state: dict[str, Any], result: Any, trace: Any, complexity: str) -> str:
    """Serialize one successful engine response for the browser worker.

    This is JSON-boundary plumbing.  Trimming an oversized undo stack is O(n)
    for the slice; JSON encoding is proportional to the returned data size.
    """
    # Cap retained snapshots so long sessions do not grow without bound.
    if len(state.get("undo_stack", [])) > UNDO_LIMIT:
        state["undo_stack"] = state["undo_stack"][-UNDO_LIMIT:]
    # Every success uses the same envelope expected by ``python.worker.js``.
    return json.dumps(
        {"ok": True, "state": state, "result": result, "trace": trace, "complexity": complexity}
    )


def _handle(action: str, payload: dict[str, Any], state: dict[str, Any]) -> str:
    """Dispatch one named browser action to its ParcelFlow operation.

    This large function is an application command router.  Individual branches
    deliberately call the teaching DSA helpers above; the string comparisons
    that select a branch are ordinary control flow, not a data structure.
    """
    # Reset replaces saved state with deterministic teaching examples.
    if action == "reset":
        state = default_state()
        return _response(state, "Sample network restored", [], "O(1)")

    # Registration validates input, builds a BFS route, then updates array,
    # queue, undo-stack, and BST representations of the new parcel.
    if action == "register":
        # Normalize IDs so searches remain case-insensitive and consistently keyed.
        parcel_id = str(payload.get("id", "")).strip().upper()
        if not re.fullmatch(r"P\d{3,6}", parcel_id):
            raise ValueError("Parcel ID must look like P104")
        # ``any`` linearly rejects duplicate IDs already in the parcel array.
        if any(item["id"] == parcel_id for item in state["parcels"]):
            raise ValueError(f"{parcel_id} already exists")
        # Endpoints must be two distinct vertices in the known graph.
        origin = str(payload.get("origin", ""))
        destination = str(payload.get("destination", ""))
        if origin not in HUB_GRAPH or destination not in HUB_GRAPH or origin == destination:
            raise ValueError("Choose two different hubs")
        # BFS supplies the new parcel's fewest-edge route.
        route = shortest_path(origin, destination)
        # Build the JSON record; slicing names to 60 characters protects the UI.
        parcel = {
            "id": parcel_id,
            "sender": str(payload.get("sender", "")).strip()[:60] or "Walk-in sender",
            "recipient": str(payload.get("recipient", "")).strip()[:60] or "Counter pickup",
            "origin": origin,
            "destination": destination,
            "status": "READY",
            "created_at": _now(),
            "route": route,
            "hop": 0,
            "location": origin,
            "events": [
                _event("REGISTERED", origin, "Parcel accepted at origin hub"),
                _event("READY", origin, "Added to dispatch queue"),
            ],
        }
        # Array append is amortized O(1).
        state["parcels"].append(parcel)
        # Restore, update, and reserialize the FIFO dispatch queue.
        queue = ArrayQueue(state["dispatch_queue"])
        queue.enqueue(parcel_id)
        state["dispatch_queue"] = queue.items
        # Push an inverse-operation record for LIFO undo.
        state["undo_stack"].append({"type": "register", "parcel_id": parcel_id})
        # Rebuild the educational BST from existing IDs, then trace this insertion.
        bst = BinarySearchTree([item["id"] for item in state["parcels"][:-1]])
        path = bst.insert(parcel_id)
        return _response(
            state,
            {
                "parcel": parcel,
                "queue_position": len(state["dispatch_queue"]),
                "bst_path": path,
                "route": route,
                "progress": progress(parcel),
            },
            [f"Array append: {parcel_id}", f"Queue enqueue: {parcel_id}", f"BST path: {' → '.join(path)}", f"BFS route: {' → '.join(route)}"],
            "Array O(1) amortized, queue O(1), BST O(h), BFS O(V + E)",
        )

    # Lookup demonstrates BST search, then enriches a found record for display.
    if action == "lookup":
        target = str(payload.get("id", "")).strip().upper()
        # Build a temporary BST from current IDs because JSON stores only parcels.
        found, path = BinarySearchTree([item["id"] for item in state["parcels"]]).search(target)
        result: dict[str, Any] = {"found": found, "path": path, "comparisons": len(path)}
        # Only successful lookup can safely fetch details and queue position.
        if found:
            parcel = _parcel(state, target)
            result["parcel"] = parcel
            result["progress"] = progress(parcel)
            if parcel["status"] == "READY":
                result["queue_position"] = state["dispatch_queue"].index(target) + 1
        return _response(state, result, path, "BST search O(h)")

    # Advance applies the state machine and saves a deep snapshot for undo.
    if action == "advance":
        parcel_id = str(payload.get("id", "")).strip().upper()
        parcel = _parcel(state, parcel_id)
        # ``deepcopy`` preserves nested events/route exactly as they were.
        snapshot = copy.deepcopy(parcel)
        advance_parcel(parcel)
        state["undo_stack"].append({"type": "advance", "parcel_id": parcel_id, "snapshot": snapshot})
        return _response(
            state,
            {"parcel": parcel, "progress": progress(parcel), "event": parcel["events"][-1]},
            [event["status"] for event in parcel["events"]],
            "Linked-list append O(1); route pointer O(1)",
        )

    # Search compares linear, binary, and BST strategies over parcel IDs.
    if action == "search":
        target = str(payload.get("id", "")).strip().upper()
        ids = [item["id"] for item in state["parcels"]]
        mode = payload.get("mode", "linear")
        # Binary search needs merge-sort preprocessing; linear/BST do not.
        if mode == "linear":
            result = linear_search(ids, target)
            complexity = "O(n)"
        elif mode == "binary":
            sorted_ids, sort_trace = merge_sort(ids)
            result = binary_search(sorted_ids, target)
            result["sorted_ids"] = sorted_ids
            result["sort_steps"] = len(sort_trace)
            complexity = "O(n log n) preprocessing + O(log n) search"
        else:
            found, path = BinarySearchTree(ids).search(target)
            result = {"found": found, "path": path, "comparisons": len(path)}
            complexity = "O(h), O(log n) average and O(n) worst case"
        # Attach the actual parcel only after the chosen algorithm reports success.
        if result["found"]:
            result["parcel"] = _parcel(state, target)
        return _response(state, result, result.get("trace", result.get("path", [])), complexity)

    # Sort runs the selected teaching algorithm on a copy of current IDs.
    if action == "sort":
        values = [item["id"] for item in state["parcels"]]
        mode = payload.get("mode", "insertion")
        # This lookup table pairs each implementation with its honest complexity.
        algorithms = {
            "insertion": (insertion_sort, "O(n²), O(n) best case"),
            "selection": (selection_sort, "O(n²)"),
            "merge": (merge_sort, "O(n log n)"),
        }
        if mode not in algorithms:
            raise ValueError("Unknown sorting algorithm")
        # Select the callable and run it without changing parcel order in state.
        algorithm, complexity = algorithms[mode]
        output, trace = algorithm(values)
        return _response(state, {"input": values, "output": output, "algorithm": mode}, trace, complexity)

    # Dispatch removes the FIFO front parcel and creates its first transit scan.
    if action == "dispatch":
        # Rehydrate the teaching queue, whose dequeue visibly shifts its array.
        queue = ArrayQueue(state["dispatch_queue"])
        parcel_id = queue.dequeue()
        state["dispatch_queue"] = queue.items
        # Save the selected parcel before changing it so undo can restore it.
        parcel = _parcel(state, parcel_id)
        snapshot = copy.deepcopy(parcel)
        # The first route edge names the next hub in the departure note.
        first_leg = parcel["route"][1] if len(parcel["route"]) > 1 else parcel["destination"]
        _scan(parcel, "IN TRANSIT", parcel["origin"], f"Departed {parcel['origin']} toward {first_leg}")
        # A newly dispatched parcel is still located at route index zero.
        parcel["hop"] = 0
        state["undo_stack"].append({"type": "dispatch", "parcel_id": parcel_id, "snapshot": snapshot})
        return _response(
            state,
            {"parcel": parcel, "remaining": list(state["dispatch_queue"]), "progress": progress(parcel)},
            [f"Dequeue front: {parcel_id}", "Append tracking node", "Move front to next parcel"],
            "Queue dequeue O(n) in this teaching array; linked-list append O(1)",
        )

    # Track validates a manual scan against the parcel lifecycle/state machine.
    if action == "track":
        parcel_id = str(payload.get("id", "")).strip().upper()
        parcel = _parcel(state, parcel_id)
        # Snapshot before validation/mutation for a later LIFO undo entry.
        snapshot = copy.deepcopy(parcel)
        status = str(payload.get("status", "AT HUB")).strip().upper()
        hub = str(payload.get("hub", parcel["destination"]))
        # Validate the requested scan name and hub against known constants.
        if status not in SCAN_STATUSES:
            raise ValueError(f"Unknown scan status: {status}")
        if hub not in HUB_GRAPH:
            raise ValueError("Choose a valid hub")
        # READY parcels must leave via FIFO dispatch rather than skipping the queue.
        if parcel["status"] == "READY":
            raise ValueError(f"{parcel_id} is still queued; dispatch it before scanning")
        # The transition table rejects lifecycle jumps such as transit -> delivered.
        if status not in TRANSITIONS.get(parcel["status"], set()):
            allowed = ", ".join(sorted(TRANSITIONS.get(parcel["status"], set()))) or "none"
            raise ValueError(f"{parcel['status']} → {status} is not allowed (next: {allowed})")
        # Last-mile statuses are only meaningful at the parcel's destination.
        if status in {"OUT FOR DELIVERY", "DELIVERED"} and hub != parcel["destination"]:
            raise ValueError(f"{status.title()} must be scanned at {parcel['destination']}")
        # These guards keep location and departure scans physically consistent.
        if status == "OUT FOR DELIVERY" and parcel["location"] != parcel["destination"]:
            raise ValueError(f"{parcel_id} must reach {parcel['destination']} before going out for delivery")
        if status == "IN TRANSIT" and parcel["location"] == parcel["destination"]:
            raise ValueError(f"{parcel_id} is already at its destination; send it out for delivery")
        if status == "IN TRANSIT" and hub != parcel["location"]:
            raise ValueError(f"A departure scan must happen at the current hub, {parcel['location']}")
        # An arrival off the current path triggers a new BFS suffix.
        rerouted = _set_location(parcel, hub) if status == "AT HUB" else False
        hop = int(parcel["hop"])
        # Generate a helpful note when the user did not supply one.
        default_notes = {
            "AT HUB": f"Arrived at {hub}" + (" · re-routed with BFS" if rerouted else ""),
            "IN TRANSIT": f"Departed {hub} toward {parcel['route'][min(hop + 1, len(parcel['route']) - 1)]}",
            "OUT FOR DELIVERY": f"With courier for {parcel['recipient']}",
            "DELIVERED": f"Handed to {parcel['recipient']}",
        }
        # Limit free text for safe layout, append the linked event, then save undo.
        note = str(payload.get("note", "")).strip()[:120] or default_notes[status]
        _scan(parcel, status, hub, note)
        state["undo_stack"].append({"type": "track", "parcel_id": parcel_id, "snapshot": snapshot})
        return _response(
            state,
            {"parcel": parcel, "nodes": len(parcel["events"]), "rerouted": rerouted, "progress": progress(parcel)},
            [event["status"] for event in parcel["events"]],
            "Linked-list append O(1) with a tail pointer; traversal O(n)",
        )

    # Undo is a LIFO stack: the most recent change is reversed first.
    if action == "undo":
        # Stack underflow is an error because there is no operation to reverse.
        if not state["undo_stack"]:
            raise ValueError("The undo stack is empty")
        # ``pop`` is O(1) and selects the newest inverse-operation record.
        operation = state["undo_stack"].pop()
        parcel_id = operation["parcel_id"]
        # Registration undo removes the new parcel from both array and queue.
        if operation["type"] == "register":
            state["parcels"] = [item for item in state["parcels"] if item["id"] != parcel_id]
            state["dispatch_queue"] = [item for item in state["dispatch_queue"] if item != parcel_id]
        # Modern advance/track/dispatch entries restore a complete deep snapshot.
        elif "snapshot" in operation:
            state["parcels"] = [
                operation["snapshot"] if item["id"] == parcel_id else item for item in state["parcels"]
            ]
            # Dispatch additionally returns the parcel to the FIFO front.
            if operation["type"] == "dispatch":
                state["dispatch_queue"].insert(0, parcel_id)
        else:  # Legacy v1 entries stored counts/status instead of a snapshot.
            parcel = _parcel(state, parcel_id)
            parcel["events"] = parcel["events"][: operation["previous_count"]]
            parcel["status"] = operation["previous_status"]
            if operation["type"] == "dispatch":
                state["dispatch_queue"].insert(0, parcel_id)
        return _response(state, f"Undid {operation['type']} for {parcel_id}", [operation], "Stack pop O(1)")

    # Graph action selects BFS or DFS traversal from a requested start vertex.
    if action == "graph":
        start = str(payload.get("start", "Mumbai Central"))
        mode = payload.get("mode", "bfs")
        # Both algorithms return display order plus an educational trace.
        order, trace = graph_bfs(start) if mode == "bfs" else graph_dfs(start)
        return _response(
            state,
            {"mode": mode, "order": order, "graph": HUB_GRAPH},
            trace,
            "O(V + E)",
        )

    # Route action runs BFS specifically for a fewest-edge unweighted path.
    if action == "route":
        start, end = str(payload.get("start", "")), str(payload.get("end", ""))
        path = shortest_path(start, end)
        return _response(
            state,
            {"path": path, "edges": max(0, len(path) - 1)},
            path,
            "BFS O(V + E), shortest by edge count for an unweighted graph",
        )

    # Expression action demonstrates three stack passes: balance, convert, evaluate.
    if action == "expression":
        expression = str(payload.get("expression", ""))
        balanced, balance_trace = balanced_parentheses(expression)
        # Stop after the first pass when parentheses are invalid.
        if not balanced:
            return _response(
                state,
                {"balanced": False, "postfix": None, "value": None},
                balance_trace,
                "O(n) time and O(n) auxiliary stack space",
            )
        # Balanced infix can be converted to postfix and evaluated safely.
        postfix, conversion_trace = infix_to_postfix(expression)
        value, evaluation_trace = evaluate_postfix(postfix)
        return _response(
            state,
            {"balanced": True, "postfix": " ".join(postfix), "value": value},
            {"balance": balance_trace, "conversion": conversion_trace, "evaluation": evaluation_trace},
            "Each stack pass is O(n)",
        )

    # Recursion action compares recursive and iterative sums of the same array.
    if action == "recursion":
        # Parse comma-separated integers; the small limit keeps traces readable.
        values = [int(part.strip()) for part in str(payload.get("values", "")).split(",") if part.strip()]
        if not values or len(values) > 20:
            raise ValueError("Enter 1 to 20 comma-separated integers")
        # Run the recursive algorithm first, then a constant-space loop for comparison.
        recursive, trace = recursive_sum(values)
        iterative = 0
        iteration_trace = []
        for value in values:
            # Accumulate one prefix total per iteration for the visualization.
            iterative += value
            iteration_trace.append(iterative)
        return _response(
            state,
            {"recursive_sum": recursive, "iterative_sum": iterative, "reversed": values[::-1]},
            {"recursion": trace, "iteration": iteration_trace},
            "O(n) time; recursion uses O(n) call-stack space, iteration uses O(1)",
        )

    # Circular action demonstrates fixed-array queue enqueue/dequeue operations.
    if action == "circular":
        # Rehydrate the queue from JSON state before applying the requested mode.
        queue = CircularQueue(state["dock_queue"])
        mode = payload.get("mode", "enqueue")
        if mode == "enqueue":
            # Van labels are required and normalized for consistent display.
            value = str(payload.get("value", "")).strip().upper()
            if not value:
                raise ValueError("Enter a van label")
            index = queue.enqueue(value)
            result = f"Placed {value} in slot {index + 1}"
        else:
            value, index = queue.dequeue()
            result = f"Released {value} from slot {index + 1}"
        # Serialize changed pointers/slots back into browser state.
        state["dock_queue"] = queue.data()
        return _response(state, result, queue.data(), "O(1) using modulo wrap-around")

    # Tree action rebuilds the BST lab, performs one operation, then visualizes it.
    if action == "tree":
        # JSON stores a flat value list; inserting it reconstructs the same BST.
        values = [int(value) for value in state["bst_demo"]]
        tree = BinarySearchTree(values)
        mode = payload.get("mode", "traverse")
        result: dict[str, Any]
        # Insert returns its comparison path and extends the persisted key list.
        if mode == "insert":
            value = int(payload["value"])
            path = tree.insert(value)
            values.append(value)
            result = {"message": f"Inserted {value}", "path": path}
        # Delete handles leaf, one-child, and two-child cases inside the BST.
        elif mode == "delete":
            value = int(payload["value"])
            deleted = tree.delete(value)
            if deleted:
                values.remove(value)
            result = {"message": f"Deleted {value}" if deleted else f"{value} was not found"}
        # Search follows ordering without changing the tree.
        elif mode == "search":
            value = int(payload["value"])
            found, path = tree.search(value)
            result = {"message": f"{'Found' if found else 'Did not find'} {value}", "path": path}
        else:
            result = {"message": "Traversal complete"}
        # Save update inputs, then calculate traversal and level-order display data.
        state["bst_demo"] = values
        order = str(payload.get("order", "inorder"))
        result.update({"order": tree.traverse(order), "levels": tree.levels(), "traversal": order})
        return _response(state, result, result.get("path", result["order"]), "O(h) update/search; O(n) traversal")

    # Reaching here means the JavaScript caller sent no supported command name.
    raise ValueError(f"Unknown action: {action}")


def handle(action: str, payload_json: str = "{}", state_json: str = "") -> str:
    """JSON boundary used by both tests and the browser's Pyodide worker.

    Incoming strings are decoded, caller state is deep-copied and normalized,
    and output is always JSON text.  This is serialization/error-handling
    plumbing rather than a DSA, though it invokes all DSA operations via
    ``_handle``.  Work is proportional to the serialized state size.
    """
    try:
        # Decode the small action-specific payload, defaulting to an empty object.
        payload = json.loads(payload_json or "{}")
        # Restore supplied browser state or create samples for a first-time caller.
        state = json.loads(state_json) if state_json else default_state()
        # Copy before mutation so a failed request cannot partially alter its input.
        return _handle(action, payload, _normalize(copy.deepcopy(state)))
    # Convert expected user/data errors into a stable failure envelope for JavaScript.
    except (ValueError, KeyError, TypeError, ZeroDivisionError) as exc:
        return json.dumps({"ok": False, "error": str(exc)})


if __name__ == "__main__":
    # Direct execution provides a tiny smoke test; imports do not run this block.
    print(handle("reset"))
