import json
import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parents[1] / "python"))

from engine import (  # noqa: E402
    BinarySearchTree,
    CircularQueue,
    binary_search,
    default_state,
    evaluate_postfix,
    graph_bfs,
    graph_dfs,
    handle,
    infix_to_postfix,
    insertion_sort,
    linear_search,
    merge_sort,
    selection_sort,
    shortest_path,
)


class AlgorithmTests(unittest.TestCase):
    def test_search_algorithms_report_comparisons(self):
        values = ["P104", "P101", "P103", "P102"]
        self.assertEqual(linear_search(values, "P103")["comparisons"], 3)
        result = binary_search(sorted(values), "P103")
        self.assertTrue(result["found"])
        self.assertLessEqual(result["comparisons"], 3)

    def test_manual_sorts(self):
        values = [7, 2, 9, 1, 4]
        expected = [1, 2, 4, 7, 9]
        for algorithm in (insertion_sort, selection_sort, merge_sort):
            with self.subTest(algorithm=algorithm.__name__):
                output, trace = algorithm(values)
                self.assertEqual(output, expected)
                self.assertTrue(trace)

    def test_bst_deletes_leaf_one_child_and_two_children(self):
        tree = BinarySearchTree([50, 30, 70, 20, 40, 60, 80, 65])
        for key in (20, 60, 70):
            self.assertTrue(tree.delete(key))
            self.assertFalse(tree.search(key)[0])
        self.assertEqual(tree.traverse("inorder"), [30, 40, 50, 65, 80])

    def test_graph_traversals_visit_every_hub(self):
        bfs, _ = graph_bfs("Mumbai Central")
        dfs, _ = graph_dfs("Mumbai Central")
        self.assertEqual(len(bfs), 6)
        self.assertEqual(set(bfs), set(dfs))
        self.assertEqual(shortest_path("Andheri", "Navi Mumbai"), ["Andheri", "Mumbai Central", "Navi Mumbai"])

    def test_stack_expression_pipeline(self):
        postfix, _ = infix_to_postfix("(12 + 8) * 3 - 6 / 2")
        value, _ = evaluate_postfix(postfix)
        self.assertEqual(value, 57)

    def test_circular_queue_wraps_around(self):
        queue = CircularQueue({"capacity": 3, "slots": ["A", "B", None], "front": 0, "rear": 1, "size": 2})
        self.assertEqual(queue.dequeue(), ("A", 0))
        queue.enqueue("C")
        self.assertEqual(queue.dequeue(), ("B", 1))
        self.assertEqual(queue.dequeue(), ("C", 2))
        queue.enqueue("D")
        self.assertEqual(queue.data()["rear"], 0)


class WorkflowTests(unittest.TestCase):
    def call(self, action, payload, state):
        response = json.loads(handle(action, json.dumps(payload), json.dumps(state)))
        self.assertTrue(response["ok"], response)
        return response

    def test_register_dispatch_track_and_undo(self):
        state = default_state()
        registered = self.call(
            "register",
            {
                "id": "P104",
                "sender": "Test Sender",
                "recipient": "Test Recipient",
                "origin": "Powai",
                "destination": "Thane",
            },
            state,
        )
        state = registered["state"]
        self.assertEqual(state["dispatch_queue"][-1], "P104")
        self.assertEqual(len(state["parcels"]), 5)
        self.assertEqual(registered["result"]["route"], ["Powai", "Thane"])

        while state["dispatch_queue"][0] != "P104":
            state = self.call("dispatch", {}, state)["state"]
        dispatched = self.call("dispatch", {}, state)
        state = dispatched["state"]
        parcel = next(item for item in state["parcels"] if item["id"] == "P104")
        self.assertEqual(parcel["status"], "IN TRANSIT")

        state = self.call("track", {"id": "P104", "status": "AT HUB", "hub": "Thane", "note": "Arrived"}, state)["state"]
        state = self.call("track", {"id": "P104", "status": "OUT FOR DELIVERY", "hub": "Thane"}, state)["state"]
        tracked = self.call(
            "track",
            {"id": "P104", "status": "DELIVERED", "hub": "Thane", "note": "Received"},
            state,
        )
        state = tracked["state"]
        self.assertEqual(next(item for item in state["parcels"] if item["id"] == "P104")["status"], "DELIVERED")
        undone = self.call("undo", {}, state)
        self.assertEqual(next(item for item in undone["state"]["parcels"] if item["id"] == "P104")["status"], "OUT FOR DELIVERY")

    def test_advance_walks_the_bfs_route_to_delivery(self):
        state = self.call(
            "register",
            {"id": "P200", "recipient": "R", "origin": "Andheri", "destination": "Navi Mumbai"},
            default_state(),
        )["state"]
        state["dispatch_queue"] = ["P200"]
        state = self.call("dispatch", {}, state)["state"]
        seen = []
        while True:
            response = self.call("advance", {"id": "P200"}, state)
            state = response["state"]
            seen.append((response["result"]["parcel"]["status"], response["result"]["parcel"]["location"]))
            if response["result"]["parcel"]["status"] == "DELIVERED":
                break
        self.assertEqual(
            seen,
            [
                ("AT HUB", "Mumbai Central"),
                ("IN TRANSIT", "Mumbai Central"),
                ("AT HUB", "Navi Mumbai"),
                ("OUT FOR DELIVERY", "Navi Mumbai"),
                ("DELIVERED", "Navi Mumbai"),
            ],
        )
        response = json.loads(handle("advance", json.dumps({"id": "P200"}), json.dumps(state)))
        self.assertFalse(response["ok"])

    def test_queued_parcel_cannot_skip_dispatch(self):
        state = default_state()
        for action, payload in (("advance", {"id": "P101"}), ("track", {"id": "P101", "status": "DELIVERED", "hub": "Thane"})):
            response = json.loads(handle(action, json.dumps(payload), json.dumps(state)))
            self.assertFalse(response["ok"])
            self.assertIn("dispatch", response["error"])

    def test_invalid_transition_and_wrong_hub_are_rejected(self):
        state = default_state()  # P099 is AT HUB in Powai, bound for Thane
        bad = json.loads(handle("track", json.dumps({"id": "P099", "status": "DELIVERED", "hub": "Thane"}), json.dumps(state)))
        self.assertFalse(bad["ok"])
        self.assertIn("not allowed", bad["error"])

    def test_cannot_skip_to_delivery_or_leave_destination(self):
        state = default_state()  # P099 is AT HUB in Powai, one hop from Thane
        early = json.loads(handle("track", json.dumps({"id": "P099", "status": "OUT FOR DELIVERY", "hub": "Thane"}), json.dumps(state)))
        self.assertFalse(early["ok"])
        state = self.call("advance", {"id": "P099"}, state)["state"]
        state = self.call("advance", {"id": "P099"}, state)["state"]  # now AT HUB Thane
        loop = json.loads(handle("track", json.dumps({"id": "P099", "status": "IN TRANSIT", "hub": "Thane"}), json.dumps(state)))
        self.assertFalse(loop["ok"])

    def test_off_route_scan_replans_with_bfs(self):
        state = default_state()
        state = self.call("track", {"id": "P099", "status": "IN TRANSIT", "hub": "Powai"}, state)["state"]
        response = self.call("track", {"id": "P099", "status": "AT HUB", "hub": "Bandra"}, state)
        parcel = response["result"]["parcel"]
        self.assertTrue(response["result"]["rerouted"])
        self.assertEqual(parcel["route"], ["Andheri", "Powai", "Bandra", "Thane"])
        self.assertEqual(response["result"]["progress"]["next_hub"], "Thane")

    def test_lookup_reports_queue_position(self):
        result = self.call("lookup", {"id": "p102"}, default_state())["result"]
        self.assertTrue(result["found"])
        self.assertEqual(result["queue_position"], 2)

    def test_v1_state_is_migrated(self):
        state = default_state()
        for parcel in state["parcels"]:
            for key in ("route", "hop", "location"):
                parcel.pop(key)
        result = self.call("lookup", {"id": "P099"}, state)["result"]
        self.assertEqual(result["parcel"]["hop"], 1)
        self.assertEqual(result["progress"]["next_action"], "Depart toward Thane")

    def test_duplicate_parcel_is_rejected(self):
        state = default_state()
        response = json.loads(
            handle(
                "register",
                json.dumps({"id": "P101", "origin": "Powai", "destination": "Thane"}),
                json.dumps(state),
            )
        )
        self.assertFalse(response["ok"])
        self.assertIn("already exists", response["error"])


if __name__ == "__main__":
    unittest.main()
