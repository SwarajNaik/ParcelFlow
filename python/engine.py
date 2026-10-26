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
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _event(status: str, hub: str, note: str) -> dict[str, str]:
    return {"status": status, "hub": hub, "note": note, "time": _now()}


def default_state() -> dict[str, Any]:
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
    for parcel in parcels:
        _normalize_parcel(parcel)
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
    """Simple FIFO queue implemented with a Python list and a front index."""

    def __init__(self, values: list[str] | None = None) -> None:
        self.items = list(values or [])

    def enqueue(self, value: str) -> None:
        self.items.append(value)

    def dequeue(self) -> str:
        if not self.items:
            raise ValueError("Dispatch queue is empty")
        value = self.items[0]
        for index in range(1, len(self.items)):
            self.items[index - 1] = self.items[index]
        self.items.pop()
        return value


class CircularQueue:
    def __init__(self, data: dict[str, Any]) -> None:
        self.capacity = int(data["capacity"])
        self.slots = list(data["slots"])
        self.front = int(data["front"])
        self.rear = int(data["rear"])
        self.size = int(data["size"])

    def enqueue(self, value: str) -> int:
        if self.size == self.capacity:
            raise ValueError("Circular queue overflow: every dock is occupied")
        self.rear = (self.rear + 1) % self.capacity
        self.slots[self.rear] = value
        if self.size == 0:
            self.front = self.rear
        self.size += 1
        return self.rear

    def dequeue(self) -> tuple[str, int]:
        if self.size == 0:
            raise ValueError("Circular queue underflow: no van is waiting")
        index = self.front
        value = self.slots[index]
        self.slots[index] = None
        self.size -= 1
        if self.size == 0:
            self.front = 0
            self.rear = -1
        else:
            self.front = (self.front + 1) % self.capacity
        return str(value), index

    def data(self) -> dict[str, Any]:
        return {
            "capacity": self.capacity,
            "slots": self.slots,
            "front": self.front,
            "rear": self.rear,
            "size": self.size,
        }


@dataclass
class LinkedNode:
    value: dict[str, Any]
    next: "LinkedNode | None" = None


class EventLinkedList:
    def __init__(self, values: list[dict[str, Any]] | None = None) -> None:
        self.head: LinkedNode | None = None
        self.tail: LinkedNode | None = None
        for value in values or []:
            self.append(value)

    def append(self, value: dict[str, Any]) -> None:
        node = LinkedNode(value)
        if self.head is None:
            self.head = self.tail = node
        else:
            assert self.tail is not None
            self.tail.next = node
            self.tail = node

    def to_list(self) -> list[dict[str, Any]]:
        values: list[dict[str, Any]] = []
        current = self.head
        while current is not None:
            values.append(current.value)
            current = current.next
        return values


@dataclass
class TreeNode:
    key: Any
    left: "TreeNode | None" = None
    right: "TreeNode | None" = None


class BinarySearchTree:
    def __init__(self, values: list[Any] | None = None) -> None:
        self.root: TreeNode | None = None
        for value in values or []:
            self.insert(value)

    def insert(self, key: Any) -> list[Any]:
        path: list[Any] = []
        if self.root is None:
            self.root = TreeNode(key)
            return [key]
        current = self.root
        while True:
            path.append(current.key)
            if key == current.key:
                raise ValueError(f"Duplicate key: {key}")
            if key < current.key:
                if current.left is None:
                    current.left = TreeNode(key)
                    path.append(key)
                    return path
                current = current.left
            else:
                if current.right is None:
                    current.right = TreeNode(key)
                    path.append(key)
                    return path
                current = current.right

    def search(self, key: Any) -> tuple[bool, list[Any]]:
        path: list[Any] = []
        current = self.root
        while current is not None:
            path.append(current.key)
            if key == current.key:
                return True, path
            current = current.left if key < current.key else current.right
        return False, path

    def delete(self, key: Any) -> bool:
        deleted = False

        def remove(node: TreeNode | None, target: Any) -> TreeNode | None:
            nonlocal deleted
            if node is None:
                return None
            if target < node.key:
                node.left = remove(node.left, target)
            elif target > node.key:
                node.right = remove(node.right, target)
            else:
                deleted = True
                if node.left is None:
                    return node.right
                if node.right is None:
                    return node.left
                successor = node.right
                while successor.left is not None:
                    successor = successor.left
                node.key = successor.key
                node.right = remove_successor(node.right, successor.key)
            return node

        def remove_successor(node: TreeNode | None, target: Any) -> TreeNode | None:
            if node is None:
                return None
            if target < node.key:
                node.left = remove_successor(node.left, target)
                return node
            return node.right

        self.root = remove(self.root, key)
        return deleted

    def traverse(self, order: str) -> list[Any]:
        result: list[Any] = []

        def visit(node: TreeNode | None) -> None:
            if node is None:
                return
            if order == "preorder":
                result.append(node.key)
            visit(node.left)
            if order == "inorder":
                result.append(node.key)
            visit(node.right)
            if order == "postorder":
                result.append(node.key)

        visit(self.root)
        return result

    def levels(self) -> list[list[Any]]:
        if self.root is None:
            return []
        result: list[list[Any]] = []
        queue: list[tuple[TreeNode, int]] = [(self.root, 0)]
        front = 0
        while front < len(queue):
            node, level = queue[front]
            front += 1
            if level == len(result):
                result.append([])
            result[level].append(node.key)
            if node.left:
                queue.append((node.left, level + 1))
            if node.right:
                queue.append((node.right, level + 1))
        return result


def linear_search(values: list[Any], target: Any) -> dict[str, Any]:
    trace = []
    for index in range(len(values)):
        trace.append({"index": index, "value": values[index]})
        if values[index] == target:
            return {"found": True, "index": index, "comparisons": len(trace), "trace": trace}
    return {"found": False, "index": -1, "comparisons": len(trace), "trace": trace}


def binary_search(values: list[Any], target: Any) -> dict[str, Any]:
    low, high = 0, len(values) - 1
    trace = []
    while low <= high:
        mid = (low + high) // 2
        trace.append({"low": low, "mid": mid, "high": high, "value": values[mid]})
        if values[mid] == target:
            return {"found": True, "index": mid, "comparisons": len(trace), "trace": trace}
        if values[mid] < target:
            low = mid + 1
        else:
            high = mid - 1
    return {"found": False, "index": -1, "comparisons": len(trace), "trace": trace}


def insertion_sort(values: list[Any]) -> tuple[list[Any], list[dict[str, Any]]]:
    data = list(values)
    trace = []
    for index in range(1, len(data)):
        key = data[index]
        cursor = index - 1
        while cursor >= 0 and data[cursor] > key:
            data[cursor + 1] = data[cursor]
            cursor -= 1
        data[cursor + 1] = key
        trace.append({"pass": index, "values": list(data)})
    return data, trace


def selection_sort(values: list[Any]) -> tuple[list[Any], list[dict[str, Any]]]:
    data = list(values)
    trace = []
    for start in range(len(data) - 1):
        smallest = start
        for cursor in range(start + 1, len(data)):
            if data[cursor] < data[smallest]:
                smallest = cursor
        data[start], data[smallest] = data[smallest], data[start]
        trace.append({"pass": start + 1, "selected": smallest, "values": list(data)})
    return data, trace


def merge_sort(values: list[Any]) -> tuple[list[Any], list[dict[str, Any]]]:
    trace: list[dict[str, Any]] = []

    def divide(data: list[Any], depth: int) -> list[Any]:
        if len(data) <= 1:
            return data
        middle = len(data) // 2
        left = divide(data[:middle], depth + 1)
        right = divide(data[middle:], depth + 1)
        merged: list[Any] = []
        i = j = 0
        while i < len(left) and j < len(right):
            if left[i] <= right[j]:
                merged.append(left[i])
                i += 1
            else:
                merged.append(right[j])
                j += 1
        merged.extend(left[i:])
        merged.extend(right[j:])
        trace.append({"depth": depth, "left": left, "right": right, "merged": list(merged)})
        return merged

    return divide(list(values), 0), trace


def graph_bfs(start: str) -> tuple[list[str], list[dict[str, Any]]]:
    if start not in HUB_GRAPH:
        raise ValueError("Choose a valid start hub")
    queue = [start]
    front = 0
    visited = {start}
    order: list[str] = []
    trace: list[dict[str, Any]] = []
    while front < len(queue):
        node = queue[front]
        front += 1
        order.append(node)
        for neighbor in HUB_GRAPH[node]:
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)
        trace.append({"visit": node, "queue": queue[front:], "visited": list(order)})
    return order, trace


def graph_dfs(start: str) -> tuple[list[str], list[dict[str, Any]]]:
    if start not in HUB_GRAPH:
        raise ValueError("Choose a valid start hub")
    visited: set[str] = set()
    order: list[str] = []
    trace: list[dict[str, Any]] = []

    def explore(node: str, depth: int) -> None:
        visited.add(node)
        order.append(node)
        trace.append({"visit": node, "depth": depth, "visited": list(order)})
        for neighbor in HUB_GRAPH[node]:
            if neighbor not in visited:
                explore(neighbor, depth + 1)

    explore(start, 0)
    return order, trace


def shortest_path(start: str, end: str) -> list[str]:
    if start not in HUB_GRAPH or end not in HUB_GRAPH:
        raise ValueError("Choose valid route hubs")
    queue = [start]
    front = 0
    parent: dict[str, str | None] = {start: None}
    while front < len(queue):
        node = queue[front]
        front += 1
        if node == end:
            break
        for neighbor in HUB_GRAPH[node]:
            if neighbor not in parent:
                parent[neighbor] = node
                queue.append(neighbor)
    if end not in parent:
        return []
    path = []
    cursor: str | None = end
    while cursor is not None:
        path.append(cursor)
        cursor = parent[cursor]
    return list(reversed(path))


def _tokens(expression: str) -> list[str]:
    compact = expression.replace(" ", "")
    tokens = re.findall(r"\d+(?:\.\d+)?|[()+\-*/^]", compact)
    if "".join(tokens) != compact:
        raise ValueError("Expression contains an unsupported token")
    return tokens


def balanced_parentheses(expression: str) -> tuple[bool, list[dict[str, Any]]]:
    stack: list[str] = []
    trace = []
    for token in expression:
        if token == "(":
            stack.append(token)
            trace.append({"token": token, "operation": "push", "stack": list(stack)})
        elif token == ")":
            if not stack:
                trace.append({"token": token, "operation": "underflow", "stack": []})
                return False, trace
            stack.pop()
            trace.append({"token": token, "operation": "pop", "stack": list(stack)})
    return len(stack) == 0, trace


def infix_to_postfix(expression: str) -> tuple[list[str], list[dict[str, Any]]]:
    precedence = {"+": 1, "-": 1, "*": 2, "/": 2, "^": 3}
    output: list[str] = []
    operators: list[str] = []
    trace = []
    for token in _tokens(expression):
        if re.fullmatch(r"\d+(?:\.\d+)?", token):
            output.append(token)
        elif token == "(":
            operators.append(token)
        elif token == ")":
            while operators and operators[-1] != "(":
                output.append(operators.pop())
            if not operators:
                raise ValueError("Parentheses are not balanced")
            operators.pop()
        else:
            while (
                operators
                and operators[-1] != "("
                and precedence[operators[-1]] >= precedence[token]
            ):
                output.append(operators.pop())
            operators.append(token)
        trace.append({"token": token, "output": list(output), "stack": list(operators)})
    while operators:
        operator = operators.pop()
        if operator == "(":
            raise ValueError("Parentheses are not balanced")
        output.append(operator)
    return output, trace


def evaluate_postfix(tokens: list[str]) -> tuple[float, list[dict[str, Any]]]:
    stack: list[float] = []
    trace = []
    for token in tokens:
        if re.fullmatch(r"\d+(?:\.\d+)?", token):
            stack.append(float(token))
            operation = "push operand"
        else:
            if len(stack) < 2:
                raise ValueError("Invalid expression")
            right = stack.pop()
            left = stack.pop()
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
    if len(stack) != 1 or not math.isfinite(stack[0]):
        raise ValueError("Expression did not reduce to one finite result")
    return stack[0], trace


def recursive_sum(values: list[int]) -> tuple[int, list[dict[str, Any]]]:
    trace: list[dict[str, Any]] = []

    def add(index: int) -> int:
        trace.append({"call": f"sum({index})", "remaining": values[index:]})
        if index == len(values):
            return 0
        result = values[index] + add(index + 1)
        trace.append({"return": result, "index": index})
        return result

    return add(0), trace


def _normalize_parcel(parcel: dict[str, Any]) -> dict[str, Any]:
    """Add route fields to parcels saved by older versions of the app."""
    if not parcel.get("route"):
        parcel["route"] = shortest_path(parcel["origin"], parcel["destination"])
    events = parcel.get("events") or []
    if "location" not in parcel:
        parcel["location"] = events[-1]["hub"] if events else parcel["origin"]
    if "hop" not in parcel:
        route = parcel["route"]
        parcel["hop"] = route.index(parcel["location"]) if parcel["location"] in route else 0
    return parcel


def _normalize(state: dict[str, Any]) -> dict[str, Any]:
    for parcel in state.get("parcels", []):
        _normalize_parcel(parcel)
    state.setdefault("undo_stack", [])
    return state


def progress(parcel: dict[str, Any]) -> dict[str, Any]:
    """Summarise where a parcel is in its route and lifecycle."""
    route = parcel["route"]
    hop = int(parcel.get("hop", 0))
    status = parcel["status"]
    next_hub = route[hop + 1] if hop + 1 < len(route) else None
    remaining = max(0, len(route) - 1 - hop)
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
    status = parcel["status"]
    route, hop = parcel["route"], int(parcel.get("hop", 0))
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
    linked = EventLinkedList(parcel["events"])
    linked.append(_event(status, hub, note))
    parcel["events"] = linked.to_list()
    parcel["status"] = status
    parcel["location"] = hub


def _set_location(parcel: dict[str, Any], hub: str) -> bool:
    """Move the hop pointer to ``hub``; re-plan with BFS when it is off-route."""
    if hub in parcel["route"]:
        parcel["hop"] = parcel["route"].index(hub)
        return False
    travelled = parcel["route"][: int(parcel.get("hop", 0)) + 1]
    parcel["route"] = travelled + shortest_path(hub, parcel["destination"])
    parcel["hop"] = len(travelled)
    return True


def advance_parcel(parcel: dict[str, Any]) -> str:
    """Apply the next automatic scan for a parcel already dispatched."""
    status = parcel["status"]
    route, hop = parcel["route"], int(parcel.get("hop", 0))
    if status == "READY":
        raise ValueError(f"{parcel['id']} is still in the dispatch queue; dispatch it first")
    if status == "DELIVERED":
        raise ValueError(f"{parcel['id']} has already been delivered")
    if status == "IN TRANSIT":
        parcel["hop"] = hop + 1
        hub = route[hop + 1]
        _scan(parcel, "AT HUB", hub, f"Arrived at {hub}")
    elif status == "AT HUB":
        if parcel["location"] == parcel["destination"]:
            _scan(parcel, "OUT FOR DELIVERY", parcel["destination"], f"With courier for {parcel['recipient']}")
        else:
            _scan(parcel, "IN TRANSIT", parcel["location"], f"Departed {parcel['location']} toward {route[hop + 1]}")
    elif status == "OUT FOR DELIVERY":
        _scan(parcel, "DELIVERED", parcel["destination"], f"Handed to {parcel['recipient']}")
    return parcel["status"]


def _parcel(state: dict[str, Any], parcel_id: str) -> dict[str, Any]:
    for parcel in state["parcels"]:
        if parcel["id"] == parcel_id:
            return parcel
    raise ValueError(f"Parcel {parcel_id} was not found")


UNDO_LIMIT = 30


def _response(state: dict[str, Any], result: Any, trace: Any, complexity: str) -> str:
    if len(state.get("undo_stack", [])) > UNDO_LIMIT:
        state["undo_stack"] = state["undo_stack"][-UNDO_LIMIT:]
    return json.dumps(
        {"ok": True, "state": state, "result": result, "trace": trace, "complexity": complexity}
    )


def _handle(action: str, payload: dict[str, Any], state: dict[str, Any]) -> str:
    if action == "reset":
        state = default_state()
        return _response(state, "Sample network restored", [], "O(1)")

    if action == "register":
        parcel_id = str(payload.get("id", "")).strip().upper()
        if not re.fullmatch(r"P\d{3,6}", parcel_id):
            raise ValueError("Parcel ID must look like P104")
        if any(item["id"] == parcel_id for item in state["parcels"]):
            raise ValueError(f"{parcel_id} already exists")
        origin = str(payload.get("origin", ""))
        destination = str(payload.get("destination", ""))
        if origin not in HUB_GRAPH or destination not in HUB_GRAPH or origin == destination:
            raise ValueError("Choose two different hubs")
        route = shortest_path(origin, destination)
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
        state["parcels"].append(parcel)
        queue = ArrayQueue(state["dispatch_queue"])
        queue.enqueue(parcel_id)
        state["dispatch_queue"] = queue.items
        state["undo_stack"].append({"type": "register", "parcel_id": parcel_id})
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

    if action == "lookup":
        target = str(payload.get("id", "")).strip().upper()
        found, path = BinarySearchTree([item["id"] for item in state["parcels"]]).search(target)
        result: dict[str, Any] = {"found": found, "path": path, "comparisons": len(path)}
        if found:
            parcel = _parcel(state, target)
            result["parcel"] = parcel
            result["progress"] = progress(parcel)
            if parcel["status"] == "READY":
                result["queue_position"] = state["dispatch_queue"].index(target) + 1
        return _response(state, result, path, "BST search O(h)")

    if action == "advance":
        parcel_id = str(payload.get("id", "")).strip().upper()
        parcel = _parcel(state, parcel_id)
        snapshot = copy.deepcopy(parcel)
        advance_parcel(parcel)
        state["undo_stack"].append({"type": "advance", "parcel_id": parcel_id, "snapshot": snapshot})
        return _response(
            state,
            {"parcel": parcel, "progress": progress(parcel), "event": parcel["events"][-1]},
            [event["status"] for event in parcel["events"]],
            "Linked-list append O(1); route pointer O(1)",
        )

    if action == "search":
        target = str(payload.get("id", "")).strip().upper()
        ids = [item["id"] for item in state["parcels"]]
        mode = payload.get("mode", "linear")
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
        if result["found"]:
            result["parcel"] = _parcel(state, target)
        return _response(state, result, result.get("trace", result.get("path", [])), complexity)

    if action == "sort":
        values = [item["id"] for item in state["parcels"]]
        mode = payload.get("mode", "insertion")
        algorithms = {
            "insertion": (insertion_sort, "O(n²), O(n) best case"),
            "selection": (selection_sort, "O(n²)"),
            "merge": (merge_sort, "O(n log n)"),
        }
        if mode not in algorithms:
            raise ValueError("Unknown sorting algorithm")
        algorithm, complexity = algorithms[mode]
        output, trace = algorithm(values)
        return _response(state, {"input": values, "output": output, "algorithm": mode}, trace, complexity)

    if action == "dispatch":
        queue = ArrayQueue(state["dispatch_queue"])
        parcel_id = queue.dequeue()
        state["dispatch_queue"] = queue.items
        parcel = _parcel(state, parcel_id)
        snapshot = copy.deepcopy(parcel)
        first_leg = parcel["route"][1] if len(parcel["route"]) > 1 else parcel["destination"]
        _scan(parcel, "IN TRANSIT", parcel["origin"], f"Departed {parcel['origin']} toward {first_leg}")
        parcel["hop"] = 0
        state["undo_stack"].append({"type": "dispatch", "parcel_id": parcel_id, "snapshot": snapshot})
        return _response(
            state,
            {"parcel": parcel, "remaining": list(state["dispatch_queue"]), "progress": progress(parcel)},
            [f"Dequeue front: {parcel_id}", "Append tracking node", "Move front to next parcel"],
            "Queue dequeue O(n) in this teaching array; linked-list append O(1)",
        )

    if action == "track":
        parcel_id = str(payload.get("id", "")).strip().upper()
        parcel = _parcel(state, parcel_id)
        snapshot = copy.deepcopy(parcel)
        status = str(payload.get("status", "AT HUB")).strip().upper()
        hub = str(payload.get("hub", parcel["destination"]))
        if status not in SCAN_STATUSES:
            raise ValueError(f"Unknown scan status: {status}")
        if hub not in HUB_GRAPH:
            raise ValueError("Choose a valid hub")
        if parcel["status"] == "READY":
            raise ValueError(f"{parcel_id} is still queued; dispatch it before scanning")
        if status not in TRANSITIONS.get(parcel["status"], set()):
            allowed = ", ".join(sorted(TRANSITIONS.get(parcel["status"], set()))) or "none"
            raise ValueError(f"{parcel['status']} → {status} is not allowed (next: {allowed})")
        if status in {"OUT FOR DELIVERY", "DELIVERED"} and hub != parcel["destination"]:
            raise ValueError(f"{status.title()} must be scanned at {parcel['destination']}")
        if status == "OUT FOR DELIVERY" and parcel["location"] != parcel["destination"]:
            raise ValueError(f"{parcel_id} must reach {parcel['destination']} before going out for delivery")
        if status == "IN TRANSIT" and parcel["location"] == parcel["destination"]:
            raise ValueError(f"{parcel_id} is already at its destination; send it out for delivery")
        if status == "IN TRANSIT" and hub != parcel["location"]:
            raise ValueError(f"A departure scan must happen at the current hub, {parcel['location']}")
        rerouted = _set_location(parcel, hub) if status == "AT HUB" else False
        hop = int(parcel["hop"])
        default_notes = {
            "AT HUB": f"Arrived at {hub}" + (" · re-routed with BFS" if rerouted else ""),
            "IN TRANSIT": f"Departed {hub} toward {parcel['route'][min(hop + 1, len(parcel['route']) - 1)]}",
            "OUT FOR DELIVERY": f"With courier for {parcel['recipient']}",
            "DELIVERED": f"Handed to {parcel['recipient']}",
        }
        note = str(payload.get("note", "")).strip()[:120] or default_notes[status]
        _scan(parcel, status, hub, note)
        state["undo_stack"].append({"type": "track", "parcel_id": parcel_id, "snapshot": snapshot})
        return _response(
            state,
            {"parcel": parcel, "nodes": len(parcel["events"]), "rerouted": rerouted, "progress": progress(parcel)},
            [event["status"] for event in parcel["events"]],
            "Linked-list append O(1) with a tail pointer; traversal O(n)",
        )

    if action == "undo":
        if not state["undo_stack"]:
            raise ValueError("The undo stack is empty")
        operation = state["undo_stack"].pop()
        parcel_id = operation["parcel_id"]
        if operation["type"] == "register":
            state["parcels"] = [item for item in state["parcels"] if item["id"] != parcel_id]
            state["dispatch_queue"] = [item for item in state["dispatch_queue"] if item != parcel_id]
        elif "snapshot" in operation:
            state["parcels"] = [
                operation["snapshot"] if item["id"] == parcel_id else item for item in state["parcels"]
            ]
            if operation["type"] == "dispatch":
                state["dispatch_queue"].insert(0, parcel_id)
        else:  # undo entries saved by v1 of the app
            parcel = _parcel(state, parcel_id)
            parcel["events"] = parcel["events"][: operation["previous_count"]]
            parcel["status"] = operation["previous_status"]
            if operation["type"] == "dispatch":
                state["dispatch_queue"].insert(0, parcel_id)
        return _response(state, f"Undid {operation['type']} for {parcel_id}", [operation], "Stack pop O(1)")

    if action == "graph":
        start = str(payload.get("start", "Mumbai Central"))
        mode = payload.get("mode", "bfs")
        order, trace = graph_bfs(start) if mode == "bfs" else graph_dfs(start)
        return _response(
            state,
            {"mode": mode, "order": order, "graph": HUB_GRAPH},
            trace,
            "O(V + E)",
        )

    if action == "route":
        start, end = str(payload.get("start", "")), str(payload.get("end", ""))
        path = shortest_path(start, end)
        return _response(
            state,
            {"path": path, "edges": max(0, len(path) - 1)},
            path,
            "BFS O(V + E), shortest by edge count for an unweighted graph",
        )

    if action == "expression":
        expression = str(payload.get("expression", ""))
        balanced, balance_trace = balanced_parentheses(expression)
        if not balanced:
            return _response(
                state,
                {"balanced": False, "postfix": None, "value": None},
                balance_trace,
                "O(n) time and O(n) auxiliary stack space",
            )
        postfix, conversion_trace = infix_to_postfix(expression)
        value, evaluation_trace = evaluate_postfix(postfix)
        return _response(
            state,
            {"balanced": True, "postfix": " ".join(postfix), "value": value},
            {"balance": balance_trace, "conversion": conversion_trace, "evaluation": evaluation_trace},
            "Each stack pass is O(n)",
        )

    if action == "recursion":
        values = [int(part.strip()) for part in str(payload.get("values", "")).split(",") if part.strip()]
        if not values or len(values) > 20:
            raise ValueError("Enter 1 to 20 comma-separated integers")
        recursive, trace = recursive_sum(values)
        iterative = 0
        iteration_trace = []
        for value in values:
            iterative += value
            iteration_trace.append(iterative)
        return _response(
            state,
            {"recursive_sum": recursive, "iterative_sum": iterative, "reversed": values[::-1]},
            {"recursion": trace, "iteration": iteration_trace},
            "O(n) time; recursion uses O(n) call-stack space, iteration uses O(1)",
        )

    if action == "circular":
        queue = CircularQueue(state["dock_queue"])
        mode = payload.get("mode", "enqueue")
        if mode == "enqueue":
            value = str(payload.get("value", "")).strip().upper()
            if not value:
                raise ValueError("Enter a van label")
            index = queue.enqueue(value)
            result = f"Placed {value} in slot {index + 1}"
        else:
            value, index = queue.dequeue()
            result = f"Released {value} from slot {index + 1}"
        state["dock_queue"] = queue.data()
        return _response(state, result, queue.data(), "O(1) using modulo wrap-around")

    if action == "tree":
        values = [int(value) for value in state["bst_demo"]]
        tree = BinarySearchTree(values)
        mode = payload.get("mode", "traverse")
        result: dict[str, Any]
        if mode == "insert":
            value = int(payload["value"])
            path = tree.insert(value)
            values.append(value)
            result = {"message": f"Inserted {value}", "path": path}
        elif mode == "delete":
            value = int(payload["value"])
            deleted = tree.delete(value)
            if deleted:
                values.remove(value)
            result = {"message": f"Deleted {value}" if deleted else f"{value} was not found"}
        elif mode == "search":
            value = int(payload["value"])
            found, path = tree.search(value)
            result = {"message": f"{'Found' if found else 'Did not find'} {value}", "path": path}
        else:
            result = {"message": "Traversal complete"}
        state["bst_demo"] = values
        order = str(payload.get("order", "inorder"))
        result.update({"order": tree.traverse(order), "levels": tree.levels(), "traversal": order})
        return _response(state, result, result.get("path", result["order"]), "O(h) update/search; O(n) traversal")

    raise ValueError(f"Unknown action: {action}")


def handle(action: str, payload_json: str = "{}", state_json: str = "") -> str:
    """JSON boundary used by both tests and the browser worker."""
    try:
        payload = json.loads(payload_json or "{}")
        state = json.loads(state_json) if state_json else default_state()
        return _handle(action, payload, _normalize(copy.deepcopy(state)))
    except (ValueError, KeyError, TypeError, ZeroDivisionError) as exc:
        return json.dumps({"ok": False, "error": str(exc)})


if __name__ == "__main__":
    print(handle("reset"))
