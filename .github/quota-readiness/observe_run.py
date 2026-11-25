"""Run the upstream E2E suite and save a focused, read-only quota readiness trace."""
import concurrent.futures
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import re
import socket
import subprocess
import sys
import threading
import time
from urllib.parse import urlencode
from urllib.request import urlopen

TEST = "Test_Examples_BackendQuotaCrossNamespace"
CONTEXT = "kind-envoy-ai-gateway"
CASES = ["without_QuotaPolicy_ReferenceGrant", "grant_restores_enforcement",
         "deleting_policy_removes_enforcement", "recreating_policy_restores_enforcement"]
MARKERS = ["QUOTA_WAIT_GATEWAY_ENTER", "QUOTA_WAIT_SIDECAR_RETURN",
           "QUOTA_WAIT_GATEWAY_READY_RETURN", "QUOTA_WAIT_BACKEND_READY_RETURN"]
SELECTORS = {
    "pods": {"fieldSelector": "metadata.namespace=envoy-gateway-system",
             "labelSelector": "gateway.envoyproxy.io/owning-gateway-name=envoy-ai-gateway-quota-cross-ns"},
    "backend": {"fieldSelector": "metadata.namespace=quota-backend-ns",
                "labelSelector": "app=cross-ns-quota-testupstream"},
}


def now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_time(value):
    padded = re.sub(r"\.(\d+)", lambda m: "." + m[1][:6].ljust(6, "0"), value)
    return datetime.fromisoformat(padded.replace("Z", "+00:00"))


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def pod_row(obj, typ):
    meta = obj["metadata"]
    spec = obj.get("spec", {})
    status = obj.get("status", {})
    names = [v["name"] for k in ("initContainers", "containers") for v in spec.get(k, [])]
    containers = {}
    for key in ("initContainerStatuses", "containerStatuses"):
        for c in status.get(key, []):
            state, detail = next(iter(c.get("state", {}).items()), ("unknown", {}))
            containers[c["name"]] = {"ready": c.get("ready", False), "state": state,
                                      "reason": detail.get("reason"), "restarts": c.get("restartCount")}
    conditions = {c["type"]: {"status": c["status"], "transition": c.get("lastTransitionTime"),
                              "reason": c.get("reason")} for c in status.get("conditions", [])}
    owner = next((r["name"] for r in meta.get("ownerReferences", []) if r["kind"] == "ReplicaSet"), None)
    return dict(at=now(), event=typ, uid=meta["uid"], name=meta["name"], rv=meta["resourceVersion"],
                created=meta.get("creationTimestamp"), deadline=meta.get("deletionTimestamp"),
                rs=owner, sidecar="ai-gateway-extproc" in names, names=names, containers=containers,
                conditions=conditions, ready=conditions.get("Ready", {}).get("status") == "True",
                phase=status.get("phase"))


def monitor(out, suite_done, target_started, target_done):
    while not suite_done.is_set():
        context = subprocess.run(["kubectl", "config", "current-context"], capture_output=True,
                                 text=True, timeout=10)
        if context.returncode == 0 and context.stdout.strip() == CONTEXT:
            break
        suite_done.wait(1)
    else:
        raise RuntimeError("Suite ended before the expected kind context became available")

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    proxy = subprocess.Popen(["kubectl", "--context", CONTEXT, "proxy", "--address=127.0.0.1",
                              f"--port={port}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    api = f"http://127.0.0.1:{port}"
    def get(path, params=None):
        with urlopen(api + path + ("?" + urlencode(params) if params else ""), timeout=10) as stream:
            return json.load(stream)

    stop = threading.Event()
    seen = {k: set() for k in SELECTORS}
    gone = {k: set() for k in SELECTORS}
    lock = threading.Lock()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=len(SELECTORS))
    futures = []

    def watch(kind, rv):
        with (out / (kind + ".ndjson")).open("x", encoding="utf-8", buffering=1) as trace:
            while not stop.is_set():
                params = dict(SELECTORS[kind], watch="true", allowWatchBookmarks="true",
                              resourceVersion=rv, timeoutSeconds="30")
                try:
                    with urlopen(api + "/api/v1/pods?" + urlencode(params), timeout=45) as stream:
                        for line in stream:
                            event = json.loads(line)
                            typ, obj = event["type"], event["object"]
                            if typ == "ERROR":
                                raise RuntimeError("Kubernetes watch error " + str(obj.get("code")))
                            rv = obj["metadata"]["resourceVersion"]
                            if typ != "BOOKMARK":
                                row = pod_row(obj, typ)
                                trace.write(json.dumps(row, separators=(",", ":")) + "\n")
                                with lock:
                                    if typ == "ADDED":
                                        seen[kind].add(row["uid"])
                                    elif typ == "DELETED":
                                        gone[kind].add(row["uid"])
                            if stop.is_set():
                                return
                except Exception:
                    if stop.is_set():
                        return
                    raise

    try:
        deadline = time.monotonic() + 130
        while True:
            try:
                server = get("/version")["gitVersion"]
                break
            except OSError:
                if suite_done.is_set() or time.monotonic() >= deadline or proxy.poll() is not None:
                    raise
                time.sleep(0.5)
        anchors = {}
        for kind, params in SELECTORS.items():
            listed = get("/api/v1/pods", params)
            if listed["items"]:
                raise RuntimeError(f"{kind} fixture already existed before the watch anchor")
            anchors[kind] = listed["metadata"]["resourceVersion"]
        anchor = {"time": now(), "resourceVersions": anchors, "selectors": SELECTORS}
        save(out / "anchor.json", anchor)
        for kind, rv in anchors.items():
            futures.append(pool.submit(watch, kind, rv))
        env = {"context": CONTEXT, "kubernetes": server, "runner_os": os.getenv("RUNNER_OS"),
               "runner_arch": os.getenv("RUNNER_ARCH"), "runner_image": os.getenv("ImageOS"),
               "runner_image_version": os.getenv("ImageVersion")}
        save(out / "environment.json", env)
        while True:
            for future in futures:
                if future.done():
                    future.result()
            if target_started.is_set() and "gateway_images" not in env:
                dep = get("/apis/apps/v1/namespaces/envoy-gateway-system/deployments/envoy-gateway")
                env["gateway_images"] = [v["image"] for v in dep["spec"]["template"]["spec"]["containers"]]
                env["nodes"] = [{"name": n["metadata"]["name"], "kubelet": n["status"]["nodeInfo"]["kubeletVersion"]}
                                for n in get("/api/v1/nodes")["items"]]
                save(out / "environment.json", env)
            with lock:
                complete = all(seen[k] and seen[k] == gone[k] for k in SELECTORS)
            if target_done.is_set() and complete:
                return {"complete": True, "pod_counts": {k: len(v) for k, v in seen.items()}}
            if suite_done.is_set():
                raise RuntimeError("Suite ended before the target and all pod watches completed")
            time.sleep(0.25)
    finally:
        stop.set()
        proxy.terminate()
        proxy.wait(timeout=10)
        pool.shutdown(wait=True)
        for f in futures:
            if f.done():
                f.result()


def main():
    out = Path(sys.argv[1])
    if not out.is_absolute():
        raise ValueError("Give an absolute output directory")
    out.mkdir(parents=True, exist_ok=True)
    if any(out.iterdir()):
        raise ValueError("Trace destination must be empty")
    if os.getenv("EG_VERSION") != "v1.9.3":
        raise ValueError("The experiment requires EG_VERSION=v1.9.3")
    for override in ["TEST_SKIP_SETUP", "K8S_VERSION"]:
        if os.getenv(override):
            raise ValueError("Unexpected setting for upstream E2E " + override)
    command = ["make", "test-e2e", "GO_TEST_ARGS=-json"]
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    suite_done, target_started, target_done = (threading.Event() for _ in range(3))
    first = None
    start = None
    parent = None
    cases = {}
    other_failures = []
    watch = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        observer = pool.submit(monitor, out, suite_done, target_started, target_done)
        try:
            print("Starting full E2E suite at", revision, flush=True)
            with (out / "test.ndjson").open("x", encoding="utf-8", buffering=1) as trace, \
                    (out / "test.log").open("x", encoding="utf-8", buffering=1) as go_log:
                with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                      text=True, encoding="utf-8", errors="replace", bufsize=1) as make:
                    for line in make.stdout:
                        if not line.startswith("{"):
                            print(line, end="", flush=True)
                            continue
                        try:
                            e = json.loads(line)
                        except ValueError:
                            continue
                        name, action = e.get("Test", ""), e.get("Action")
                        if not name and action == "output" and not target_started.is_set():
                            print(e.get("Output", ""), end="", flush=True)
                        if name != TEST and not name.startswith(TEST + "/"):
                            if action == "fail":
                                other_failures.append(name or e.get("Package", "(package)"))
                            continue
                        trace.write(json.dumps(e, separators=(",", ":")) + "\n")
                        go_log.write(e.get("Output", ""))
                        if name == TEST and action == "run":
                            start = e["Time"]
                            target_started.set()
                        elif name != TEST and action == "run" and first is None:
                            first = e["Time"]
                        elif action in {"pass", "fail", "skip"}:
                            if name == TEST:
                                parent = e
                                target_done.set()
                                print("Target", action, "after", e.get("Elapsed"), "seconds", flush=True)
                            else:
                                cases[name.removeprefix(TEST + "/")] = {"outcome": action, "seconds": e.get("Elapsed")}
                    status = make.wait()
        finally:
            suite_done.set()
        try:
            watch = observer.result()
        except Exception as exc:
            watch = {"complete": False, "error": f"{type(exc).__name__}: {exc}"}
    detail = (out / "test.log").read_text(encoding="utf-8")
    marker_counts = {m: detail.count(m) for m in MARKERS}
    anchor_path, env_path = out / "anchor.json", out / "environment.json"
    anchor_ok = bool(start and anchor_path.exists() and parse_time(json.loads(anchor_path.read_text())["time"]) < parse_time(start))
    images = json.loads(env_path.read_text()).get("gateway_images", []) if env_path.exists() else []
    version_ok = any(re.search(r":v1\.9\.3(?:@sha256:\w+)?$", i) for i in images)
    passed = bool(parent and parent["Action"] == "pass" and list(cases) == CASES and
                  all(c["outcome"] == "pass" for c in cases.values()))
    valid = bool(passed and watch["complete"] and anchor_ok and version_ok and
                 all(count == 1 for count in marker_counts.values()))
    report = dict(revision=revision, command=command, suite_exit=status, other_failures=other_failures,
                  start=start, first_case_seconds=(parse_time(first)-parse_time(start)).total_seconds() if first and start else None,
                  total_seconds=parent.get("Elapsed") if parent else None, quota_result=parent.get("Action") if parent else None,
                  cases=cases, marker_counts=marker_counts, watch=watch, anchored_before_test=anchor_ok,
                  confirmed_gateway_version=version_ok, valid_pair_member=valid)
    save(out / "trial.json", report)
    print(json.dumps(report), flush=True)
    return status if valid else 1


if __name__ == "__main__":
    sys.exit(main())
