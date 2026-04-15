

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import socket as _socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, List

import uvicorn

try:
    import yaml as _yaml
except ImportError:
    _yaml = None
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from pydantic import BaseModel

logger = logging.getLogger(__name__)


INSTRUMENT_BASE = os.environ.get("GSOD_INSTRUMENT_BASE", "/mnt/d/instrument")
_GSOD_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_BASE = os.environ.get("GSOD_DATA_BASE", os.path.join(_GSOD_ROOT, "data"))
WORKDIR_BASE = os.environ.get(
    "GSOD_WORKDIR_BASE", os.path.join(_GSOD_ROOT, "gsod_output_v5")
)
PYTHON = sys.executable
MAIN_PY = os.path.join(_GSOD_ROOT, "main.py")


_jobs: Dict[str, dict] = {}
_log_buffers: Dict[str, deque] = {}
_processes: Dict[str, subprocess.Popen] = {}


_ckpt_preview_cache: Dict[
    str, dict
] = {}
_cache_lock = threading.Lock()

_CACHE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".ckpt_preview_cache.json"
)


def _load_cache_from_disk() -> None:
    """Load the persisted preview cache from disk into _ckpt_preview_cache."""
    if not os.path.isfile(_CACHE_FILE):
        return
    try:
        with open(_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        with _cache_lock:
            _ckpt_preview_cache.update(data)
        logger.info("[ckpt-cache] 从磁盘加载持久化缓存: %d 条", len(data))
    except Exception as exc:
        logger.warning("[ckpt-cache] 加载持久化缓存失败: %s", exc)


def _save_cache_to_disk() -> None:
    """Persist the current in-memory preview cache to disk."""
    try:
        with _cache_lock:
            snapshot = dict(_ckpt_preview_cache)
        with open(_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False)
    except Exception as exc:
        logger.warning("[ckpt-cache] 持久化缓存写入失败: %s", exc)


def _now():
    return datetime.now().isoformat(timespec="seconds")


def _job_log(job_id: str, line: str):
    if job_id not in _log_buffers:
        _log_buffers[job_id] = deque(maxlen=2000)
    _log_buffers[job_id].append(line)


def _scan_instruments(base: str) -> List[dict]:
    """Scan *base* for subdirectories that look like instrumented operators."""
    result = []
    if not os.path.isdir(base):
        return result
    for name in sorted(os.listdir(base)):
        subdir = os.path.join(base, name)
        if not os.path.isdir(subdir):
            continue
        info = {"name": name, "path": subdir}

        iij = os.path.join(subdir, "instrument_info.json")
        info["has_instrument_info"] = os.path.isfile(iij)
        info["instrument_info"] = iij if info["has_instrument_info"] else ""

        frj = os.path.join(subdir, "field_relations.json")
        info["has_field_relations"] = os.path.isfile(frj)
        info["field_relations"] = frj if info["has_field_relations"] else ""
        if info["has_field_relations"]:
            try:
                with open(frj) as f:
                    fr = json.load(f)
                info["field_relations_count"] = len(fr)
            except Exception:
                info["field_relations_count"] = 0
        else:
            info["field_relations_count"] = 0

        runner_yaml = os.path.join(DATA_BASE, name, "runner.yaml")
        info["has_runner_yaml"] = os.path.isfile(runner_yaml)
        info["runner_yaml"] = runner_yaml if info["has_runner_yaml"] else ""

        project_path = ""
        operator_namespace = ""
        if info["has_runner_yaml"] and _yaml is not None:
            try:
                with open(runner_yaml, encoding="utf-8") as _f:
                    _ry = _yaml.safe_load(_f) or {}
                project_path = (
                    (_ry.get("testplan") or {}).get("project_path", "")
                    or (_ry.get("run") or {}).get("project_path", "")
                    or (_ry.get("coverage-test") or {}).get("project_path", "")
                    or ""
                )
                operator_namespace = (_ry.get("common") or {}).get(
                    "operator_namespace", ""
                )
            except Exception:
                pass
        info["project_path"] = project_path or ""
        info["operator_namespace"] = operator_namespace or ""

        ctx = os.path.join(DATA_BASE, name, "context.json")
        info["has_context"] = os.path.isfile(ctx)
        info["context"] = ctx if info["has_context"] else ""
        result.append(info)
    return result


def _read_checkpoint_preview(ckpt_path: str, mode: str) -> dict:
    """Read a checkpoint JSON and extract a lightweight preview dict.

    Reads unconditionally regardless of file size.
    Returns an empty dict on any error.
    """
    try:
        with open(ckpt_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}

    try:
        if mode == "tp":
            tp = data.get("testplan", {})
            testcases: dict = tp.get("testcases", {})
            coverage_map: dict = tp.get("coverage_map", {})
            targets: dict = tp.get("targets", {})
            covered = sum(1 for v in coverage_map.values() if v)
            resolved = sum(
                1 for t in targets.values() if isinstance(t, dict) and t.get("resolved")
            )
            tc_ids = sorted(
                testcases.keys(), key=lambda x: int(x) if x.isdigit() else 0
            )
            return {
                "tc_count": len(testcases),
                "covered_branches": covered,
                "total_branches": len(coverage_map),
                "resolved_targets": resolved,
                "total_targets": len(targets),
                "round_n": tp.get("round_n", 0),
                "tc_sample": tc_ids[:15],
            }
        elif mode in ("run",):
            e2e = data.get("e2e_test", {})
            tc_count = len(data.get("testplan", {}).get("testcases", {}))
            return {
                "rounds": e2e.get("rounds", 0),
                "passed": e2e.get("passed", 0),
                "failed": e2e.get("failed", 0),
                "errors": e2e.get("errors", 0),
                "total_tc": tc_count,
            }
        elif mode in ("fault", "ft"):
            ft = data.get("fault_test", {})
            tc_count = len(data.get("testplan", {}).get("testcases", {}))
            return {
                "rounds": ft.get("rounds", 0),
                "failures": ft.get("failures", 0),
                "fault_counts": ft.get("fault_counts", {}),
                "total_tc": tc_count,
            }
        elif mode == "ea":
            ea = data.get("explore_all", {})
            ml = ea.get("mutation_log", [])
            ok = sum(1 for m in ml if isinstance(m, dict) and m.get("status") == "ok")
            n_rel = len(data.get("field_relations", {}))
            return {
                "mutation_count": len(ml),
                "ok_count": ok,
                "fail_count": len(ml) - ok,
                "relation_count": n_rel,
            }
    except Exception:
        pass
    return {}


def _refresh_preview_cache() -> None:
    """Scan WORKDIR_BASE and update _ckpt_preview_cache for new/modified checkpoints.

    Uses a high size limit since this runs in a background daemon thread.
    """
    if not os.path.isdir(WORKDIR_BASE):
        return
    for dn in os.listdir(WORKDIR_BASE):
        full = os.path.join(WORKDIR_BASE, dn)
        ckpt = os.path.join(full, "checkpoint.json")
        if not os.path.isfile(ckpt):
            continue
        try:
            mtime = os.path.getmtime(ckpt)
        except OSError:
            continue
        with _cache_lock:
            cached = _ckpt_preview_cache.get(ckpt)
            if cached and cached.get("mtime") == mtime:
                continue

        _pfx = dn.split("-")[0]
        mode = _pfx if _pfx in ("ea", "tp", "ft", "run", "fault") else "v5"
        preview = _read_checkpoint_preview(ckpt, mode)
        with _cache_lock:
            _ckpt_preview_cache[ckpt] = {"preview": preview, "mtime": mtime}


def _cache_watcher_loop() -> None:
    """Background daemon: load disk cache, scan once, then refresh every 30 s."""

    _load_cache_from_disk()
    logger.info("[ckpt-cache] 启动检查点预览缓存后台扫描 (WORKDIR=%s)", WORKDIR_BASE)
    try:
        _refresh_preview_cache()
        _save_cache_to_disk()
        with _cache_lock:
            n = len(_ckpt_preview_cache)
        logger.info("[ckpt-cache] 初始扫描完成，已缓存 %d 个检查点", n)
    except Exception as exc:
        logger.warning("[ckpt-cache] 初始扫描异常: %s", exc)
    while True:
        time.sleep(30)
        try:
            before = len(_ckpt_preview_cache)
            _refresh_preview_cache()
            after = len(_ckpt_preview_cache)
            if after != before:
                logger.info(
                    "[ckpt-cache] 定期扫描更新，缓存条目 %d → %d", before, after
                )
                _save_cache_to_disk()
        except Exception as exc:
            logger.warning("[ckpt-cache] 定期扫描异常: %s", exc)


def _start_cache_watcher() -> None:
    """Start the background checkpoint-preview cache watcher thread."""
    t = threading.Thread(
        target=_cache_watcher_loop,
        name="ckpt-preview-watcher",
        daemon=True,
    )
    t.start()
    logger.info("[ckpt-cache] 后台扫描线程已启动")


def _find_checkpoints(op_name: str, prefix: str = "") -> List[dict]:
    """Find checkpoint files under WORKDIR_BASE for a given operator.

    If *prefix* is given (e.g. ``"tp"``, ``"ea"``, ``"run"``, ``"fault"``),
    only directories whose name starts with ``<prefix>-<op_name>`` are
    returned.  Otherwise all directories containing *op_name* are returned.
    """
    result = []
    if not os.path.isdir(WORKDIR_BASE):
        return result
    for dn in sorted(os.listdir(WORKDIR_BASE), reverse=True):
        dn_lower = dn.lower()
        if prefix:

            expected = f"{prefix.lower()}-{op_name.lower()}-"
            if not dn_lower.startswith(expected):
                continue
        else:
            if op_name.lower() not in dn_lower:
                continue
        full = os.path.join(WORKDIR_BASE, dn)
        ckpt = os.path.join(full, "checkpoint.json")
        if os.path.isfile(ckpt):
            _pfx = dn.split("-")[0]
            mode = _pfx if _pfx in ("ea", "tp", "ft", "run", "fault") else "v5"
            mtime = os.path.getmtime(ckpt)

            with _cache_lock:
                cached = _ckpt_preview_cache.get(ckpt)
            if cached and cached.get("mtime") == mtime:
                preview = cached["preview"]
            else:
                preview = _read_checkpoint_preview(ckpt, mode)

                with _cache_lock:
                    _ckpt_preview_cache[ckpt] = {"preview": preview, "mtime": mtime}
            result.append(
                {
                    "dir": full,
                    "checkpoint": ckpt,
                    "mode": mode,
                    "name": dn,
                    "mtime": datetime.fromtimestamp(mtime).isoformat(
                        timespec="seconds"
                    ),
                    "preview": preview,
                }
            )
    return result[:20]


_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[mGKHFABCDEFsuhl]")


def _stream_subprocess(job_id: str, proc: subprocess.Popen):
    """Read stdout+stderr from *proc* and push lines to the job log buffer."""
    try:
        for raw in iter(proc.stdout.readline, b""):
            line = raw.decode("utf-8", errors="replace").rstrip()
            line = _ANSI_RE.sub("", line)
            _job_log(job_id, line)
        proc.wait()
    except Exception as exc:
        _job_log(job_id, f"[stream error] {exc}")
    finally:
        rc = proc.returncode if proc.returncode is not None else proc.wait()
        job = _jobs.get(job_id)
        if job:
            job["status"] = "done" if rc == 0 else "error"
            job["returncode"] = rc
            job["finished_at"] = _now()
        _job_log(job_id, f"[process exited with code {rc}]")
        _processes.pop(job_id, None)


def _launch_job(job_id: str, cmd: List[str], cwd: str = ".") -> bool:
    """Spawn subprocess and start a reader thread. Returns False if already running."""
    if _processes.get(job_id) is not None:
        return False
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=cwd,
        )
        _processes[job_id] = proc
        _jobs[job_id]["status"] = "running"
        _jobs[job_id]["pid"] = proc.pid
        _jobs[job_id]["started_at"] = _now()
        t = threading.Thread(
            target=_stream_subprocess, args=(job_id, proc), daemon=True
        )
        t.start()
        return True
    except Exception as exc:
        _job_log(job_id, f"[launch error] {exc}")
        _jobs[job_id]["status"] = "error"
        return False


def _stop_job(job_id: str):
    proc = _processes.get(job_id)
    if proc is None:
        return
    try:
        proc.terminate()
        time.sleep(1)
        if proc.poll() is None:
            proc.kill()
    except Exception:
        pass
    _jobs[job_id]["status"] = "stopped"
    _jobs[job_id]["finished_at"] = _now()


@asynccontextmanager
async def _lifespan(app_: FastAPI):
    """FastAPI lifespan: start cache watcher after server binds."""
    _start_cache_watcher()
    yield


app = FastAPI(title="GSOD UI", version="1.0", lifespan=_lifespan)


@app.get("/api/instruments")
def list_instruments(base: str = Query(default=INSTRUMENT_BASE)):
    return _scan_instruments(base)


@app.get("/api/instruments/{name}/checkpoints")
def instrument_checkpoints(name: str, prefix: str = Query(default="")):
    return _find_checkpoints(name, prefix=prefix)


class CreateJobRequest(BaseModel):
    mode: str
    operator: str

    profile: str = ""
    checkpoint: str = ""
    testplan_checkpoint: str = ""
    fault_types: str = "crash"
    fault_manager_url: str = ""
    max_rounds: int = 0
    extra_args: List[str] = []


@app.get("/api/jobs")
def list_jobs():
    return list(_jobs.values())


@app.post("/api/jobs", status_code=201)
def create_job(req: CreateJobRequest):
    job_id = str(uuid.uuid4())[:8]

    profile = req.profile
    if not profile:
        candidate = os.path.join(DATA_BASE, req.operator, "runner.yaml")
        if os.path.isfile(candidate):
            profile = candidate
    job = {
        "id": job_id,
        "mode": req.mode,
        "operator": req.operator,
        "profile": profile,
        "checkpoint": req.checkpoint,
        "status": "pending",
        "created_at": _now(),
        "started_at": None,
        "finished_at": None,
        "pid": None,
        "returncode": None,
        "fault_types": req.fault_types,
        "fault_manager_url": req.fault_manager_url,
        "max_rounds": req.max_rounds,
        "testplan_checkpoint": req.testplan_checkpoint,
        "extra_args": req.extra_args,
    }
    _jobs[job_id] = job
    _log_buffers[job_id] = deque(maxlen=2000)
    return job


@app.post("/api/jobs/{job_id}/start")
def start_job(job_id: str):
    if job_id not in _jobs:
        raise HTTPException(404, "Job not found")
    job = _jobs[job_id]
    if job["status"] == "running":
        raise HTTPException(400, "Job already running")

    mode = job["mode"]
    profile = job["profile"]
    cmd = [PYTHON, "-u", MAIN_PY]

    if profile:
        cmd += ["--profile", profile]

    cmd.append(mode)

    if job["checkpoint"]:
        cmd += ["--checkpoint", job["checkpoint"]]

    if mode == "fault":
        if not job.get("testplan_checkpoint"):
            raise HTTPException(400, "fault mode requires testplan_checkpoint")
        cmd += ["--testplan-checkpoint", job["testplan_checkpoint"]]
        cmd += ["--fault-types", job["fault_types"] or "crash"]
        if job.get("fault_manager_url"):
            cmd += ["--fault-manager-url", job["fault_manager_url"]]
        if job.get("max_rounds"):
            cmd += ["--max-rounds", str(job["max_rounds"])]

    elif mode == "run":
        if not job.get("testplan_checkpoint"):
            raise HTTPException(400, "run mode requires testplan_checkpoint")
        cmd += ["--testplan-checkpoint", job["testplan_checkpoint"]]
        if job.get("max_rounds"):
            cmd += ["--max-rounds", str(job["max_rounds"])]

    elif mode == "testplan":
        if job.get("max_rounds"):
            cmd += ["--max-rounds", str(job["max_rounds"])]

    elif mode == "explore-all":
        pass

    cmd += job.get("extra_args", [])

    _launch_job(job_id, cmd, cwd=os.path.dirname(MAIN_PY))
    return _jobs[job_id]


@app.post("/api/jobs/{job_id}/stop")
def stop_job(job_id: str):
    if job_id not in _jobs:
        raise HTTPException(404, "Job not found")
    _stop_job(job_id)
    return _jobs[job_id]


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str, lines: int = Query(default=200)):
    if job_id not in _jobs:
        raise HTTPException(404, "Job not found")
    buf = list(_log_buffers.get(job_id, []))
    return {**_jobs[job_id], "log_tail": buf[-lines:]}


@app.get("/api/jobs/{job_id}/stream")
async def stream_job_logs(job_id: str):
    """SSE endpoint — streams log lines as they arrive."""
    if job_id not in _jobs:
        raise HTTPException(404, "Job not found")

    async def event_generator():
        buf = _log_buffers.get(job_id, deque())
        sent = 0
        while True:
            current = list(buf)
            for line in current[sent:]:
                yield f"data: {json.dumps(line)}\n\n"
                sent += 1
            job = _jobs.get(job_id, {})
            if job.get("status") in ("done", "error", "stopped") and sent >= len(
                current
            ):
                yield "event: end\ndata: done\n\n"
                break
            await asyncio.sleep(0.3)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/api/jobs/{job_id}/report")
def job_report(job_id: str):
    """Return the path to the HTML report for this job's workdir."""
    if job_id not in _jobs:
        raise HTTPException(404, "Job not found")
    job = _jobs[job_id]
    ckpt = job.get("checkpoint", "")
    if ckpt and os.path.isfile(ckpt):
        ckpt_dir = os.path.dirname(ckpt)
        for fn in ("testplan_report.html", "explore_all_report.html", "report.html"):
            rp = os.path.join(ckpt_dir, fn)
            if os.path.isfile(rp):
                return FileResponse(rp)
    raise HTTPException(404, "Report not found")


@app.get("/api/instrument_info")
def view_instrument_info(
    path: str = Query(..., description="Path to instrument_info.json"),
):
    """Return a structured summary of instrument_info.json for the UI viewer."""
    if not os.path.isfile(path):
        raise HTTPException(404, "instrument_info.json not found")
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    branch_points = raw.get("branch_points", [])

    files: dict = {}
    for bp in branch_points:
        fp = bp.get("File") or bp.get("FilePath") or ""
        if fp not in files:
            files[fp] = {"file": fp, "branch_count": 0, "branches": []}
        entry = files[fp]
        entry["branch_count"] += 1
        exprs = bp.get("Expressions") or []
        fmt_list = [
            e.get("fmt") or e.get("raw") or e.get("Fmt") or ""
            for e in exprs
            if isinstance(e, dict)
        ]
        entry["branches"].append(
            {
                "idx": bp.get("BranchIndex", bp.get("Index", 0)),
                "line": bp.get("BranchLine") or bp.get("Line") or "",
                "fmts": [f for f in fmt_list if f],
            }
        )

    return {
        "total_branches": len(branch_points),
        "total_files": len(files),
        "meta": {k: v for k, v in raw.items() if k != "branch_points"},
        "files": sorted(files.values(), key=lambda x: x["file"]),
    }


@app.get("/api/field_relations")
def view_field_relations(
    path: str = Query(..., description="Path to field_relations.json"),
):
    if not os.path.isfile(path):
        raise HTTPException(404, "field_relations.json not found")
    with open(path) as f:
        return json.load(f)


@app.get("/api/branch_source")
def view_branch_source(
    instrument_dir: str = Query(..., description="Instrumented operator directory"),
    branch_index: int = Query(..., description="BranchIndex to look up"),
    project_path: str = Query(default="", description="Operator source root"),
):
    """Return source context for a branch using libinstrument.so via source.py."""
    try:
        import sys as _sys

        _gsod = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if _gsod not in _sys.path:
            _sys.path.insert(0, _gsod)
        from instrumentation.source import _get_branch_source_context

        src = _get_branch_source_context(
            project_path=project_path,
            instrument_dir=instrument_dir,
            branch_index=branch_index,
        )
        return {"ok": bool(src), "source": src, "branch_index": branch_index}
    except Exception as exc:
        return {"ok": False, "source": "", "error": str(exc)}


@app.get("/api/ckpt/testplan")
def view_testplan_ckpt(
    path: str = Query(..., description="Path to testplan checkpoint.json"),
    tc_limit: int = Query(200, description="Max TCs to return"),
):
    """Return testcases + per-TC branch coverage from a testplan checkpoint."""
    if not os.path.isfile(path):
        raise HTTPException(404, "checkpoint not found")
    with open(path, encoding="utf-8") as f:
        ckpt = json.load(f)

    tp = ckpt.get("testplan", {})
    testcases: dict = tp.get("testcases", {})
    coverage_map: dict = tp.get("coverage_map", {})
    targets: dict = tp.get("targets", {})


    bm: dict = {}
    for bi_key, bi_val in (ckpt.get("branch_meta") or {}).items():
        bm[str(bi_key)] = bi_val

    covered = sum(1 for v in coverage_map.values() if v)
    resolved = sum(
        1 for t in targets.values() if isinstance(t, dict) and t.get("resolved")
    )


    def _tc_sort(tc_id: str):
        return int(tc_id) if tc_id.isdigit() else tc_id

    tc_ids = sorted(testcases.keys(), key=_tc_sort)[:tc_limit]

    rows = []
    for tc_id in tc_ids:
        tc = testcases[tc_id]
        involved = sorted(set(tc.get("involved_branches", [])))
        branch_fmts = []
        for bi in involved:
            bm_e = bm.get(str(bi), {})
            fmt = bm_e.get("Fmt") or bm_e.get("Raw") or ""
            branch_fmts.append({"bi": bi, "fmt": fmt})
        rows.append(
            {
                "id": tc_id,
                "frequency": tc.get("frequency", 0),
                "has_new_branch": tc.get("has_new_branch", True),
                "branch_count": len(involved),
                "involved_branches": branch_fmts,
                "cr": tc.get("cr") or "",
            }
        )

    return {
        "total_tc": len(testcases),
        "covered_branches": covered,
        "total_branches": len(coverage_map),
        "resolved_targets": resolved,
        "total_targets": len(targets),
        "round_n": tp.get("round_n", 0),
        "testcases": rows,
        "truncated": len(testcases) > tc_limit,
    }


@app.get("/api/jobs/{job_id}/progress")
def get_job_progress(job_id: str):
    """Return structured live-dashboard data by reading the latest checkpoint for the job."""
    if job_id not in _jobs:
        raise HTTPException(404, "Job not found")
    job = _jobs[job_id]
    op = job.get("operator", "")
    mode = job.get("mode", "")
    mode_key = {"explore-all": "ea", "testplan": "tp", "fault": "ft"}.get(mode, "v5")

    op_name = os.path.basename(op.rstrip("/\\")) if op else ""


    ckpt_path_from_log: str = ""
    log_buf = list(_log_buffers.get(job_id, []))
    for line in reversed(log_buf):
        idx = line.find("Checkpoint:")
        if idx != -1:
            candidate = line[idx + len("Checkpoint:") :].strip()
            if not os.path.isabs(candidate):
                candidate = os.path.join(_GSOD_ROOT, candidate)
            if os.path.isfile(candidate):
                ckpt_path_from_log = candidate
                break


    if not ckpt_path_from_log:
        all_ckpts = _find_checkpoints(op_name)
        ckpts = [c for c in all_ckpts if c["mode"] == mode_key]
        if ckpts:
            ckpt_path_from_log = max(ckpts, key=lambda c: c["mtime"])["checkpoint"]

    if not ckpt_path_from_log:
        workdir_exists = os.path.isdir(WORKDIR_BASE)
        candidates = (
            sorted(os.listdir(WORKDIR_BASE), reverse=True)[:10]
            if workdir_exists
            else []
        )
        return {
            "ok": False,
            "reason": "checkpoint not found yet",
            "debug": {
                "workdir_base": WORKDIR_BASE,
                "workdir_exists": workdir_exists,
                "op_name": op_name,
                "mode_key": mode_key,
                "log_lines_scanned": len(log_buf),
                "dir_sample": candidates,
            },
        }

    try:
        with open(ckpt_path_from_log, encoding="utf-8") as _f:
            ckpt = json.load(_f)
    except Exception as e:
        return {"ok": False, "reason": str(e)}

    ckpt_mtime = datetime.fromtimestamp(os.path.getmtime(ckpt_path_from_log)).isoformat(
        timespec="seconds"
    )
    result: dict = {
        "ok": True,
        "checkpoint_path": ckpt_path_from_log,
        "checkpoint_mtime": ckpt_mtime,
        "mode": mode,
    }

    if mode == "explore-all":
        ea = ckpt.get("explore_all", {})
        field_relations = ckpt.get("field_relations", {})
        completed = ea.get("completed_fields", [])
        mut_log = ea.get("mutation_log", [])

        result["phase"] = "Explore-All"
        result["fields_done"] = len(completed)
        result["relations_count"] = len(field_relations)
        result["baseline_collected"] = ea.get("baseline_collected", False)


        fields_total = 0
        for _ln in log_buf:
            _idx2 = _ln.find("CRD 字段总数:")
            if _idx2 != -1:
                try:
                    fields_total = int(
                        _ln[_idx2 + len("CRD 字段总数:") :].strip().split(",")[0]
                    )
                except ValueError:
                    pass
                break
        result["fields_total"] = fields_total


        current_field = ""
        for _ln in reversed(log_buf):
            if "[explore-all" in _ln and "字段:" in _ln:
                _fi = _ln.find("字段:")
                if _fi != -1:
                    current_field = _ln[_fi + len("字段:") :].strip()
                break
        result["current_field"] = current_field

        result["recent_mutations"] = [
            {
                "field": m.get("field", ""),
                "status": m.get("status", ""),
                "diff_summary": m.get("diff_summary") or {},
                "cr_changed_fields": (m.get("cr_changed_fields") or [])[:4],
                "timing": {
                    "total_sec": round(
                        (m.get("timing") or {}).get("total_sec") or 0, 1
                    ),
                    "llm_sec": round((m.get("timing") or {}).get("llm_sec") or 0, 1),
                },
            }
            for m in mut_log[-10:]
        ]


        _branch_map: dict = {}
        for _fld, _fv in field_relations.items():
            if not _fv:
                continue
            for _bi in _fv.get("branch_indices") or []:
                _bkey = str(_bi)
                if _bkey not in _branch_map:
                    _branch_map[_bkey] = {
                        "branch_index": _bi,
                        "fields": [],
                        "expressions": [],
                        "variables": [],
                    }
                _be = _branch_map[_bkey]
                if _fld not in _be["fields"]:
                    _be["fields"].append(_fld)
                for _e in _fv.get("expression_fmts") or []:
                    if _e not in _be["expressions"]:
                        _be["expressions"].append(_e)
                for _var in _fv.get("variable_mappings") or {}:
                    if _var not in _be["variables"]:
                        _be["variables"].append(_var)
        result["branch_relations"] = [
            {
                "branch_index": _b["branch_index"],
                "fields": _b["fields"][:6],
                "field_count": len(_b["fields"]),
                "expressions": _b["expressions"][:3],
                "variables": _b["variables"][:4],
            }
            for _b in sorted(_branch_map.values(), key=lambda x: x["branch_index"])
        ]


        seed_cr_yaml = ""
        _profile = job.get("profile", "")
        _instr_dir = ""
        _proj_path = ""
        if _profile:
            try:
                _pf = (
                    _profile
                    if os.path.isabs(_profile)
                    else os.path.join(_GSOD_ROOT, _profile)
                )
                with open(_pf, encoding="utf-8") as _pff:
                    _prof = _yaml.safe_load(_pff) if _yaml else {}
                _bcr = (_prof.get("common") or {}).get("base_cr", "")
                if _bcr:
                    _bcr_abs = (
                        _bcr if os.path.isabs(_bcr) else os.path.join(_GSOD_ROOT, _bcr)
                    )
                    if os.path.isfile(_bcr_abs):
                        with open(_bcr_abs, encoding="utf-8") as _bf:
                            seed_cr_yaml = _bf.read()
                _instr_dir = (_prof.get("testplan") or {}).get(
                    "instrument_dir", ""
                ) or (_prof.get("explore-all") or {}).get("instrument_dir", "")
                _proj_path = (_prof.get("testplan") or {}).get("project_path", "") or (
                    _prof.get("explore-all") or {}
                ).get("project_path", "")
            except Exception:
                pass
        result["seed_cr_yaml"] = seed_cr_yaml
        result["instrument_dir"] = _instr_dir
        result["project_path"] = _proj_path

        cur_cr = ea.get("current_cr_yaml", "") or ""
        result["current_cr_yaml"] = cur_cr

    elif mode == "testplan":
        tp = ckpt.get("testplan", {})
        field_relations = ckpt.get("field_relations", {})
        coverage_map = tp.get("coverage_map", {})
        testcases = tp.get("testcases", {})
        branch_history = tp.get("branch_history", [])

        result["phase"] = "TestPlan"
        result["round"] = len(branch_history)
        result["testcases_count"] = len(testcases)
        result["branches_covered"] = sum(1 for v in coverage_map.values() if v)
        result["branches_total"] = len(coverage_map)
        result["relations_count"] = len(field_relations)

        targets = tp.get("targets", {})
        result["targets_resolved"] = sum(
            1 for t in targets.values() if t.get("resolved")
        )
        result["targets_total"] = len(targets)

    elif mode == "fault":
        ft = ckpt.get("fault_test", {})
        result["phase"] = "Fault"
        result["rounds"] = ft.get("rounds", 0)
        result["failures"] = ft.get("failures", 0)
        result["fault_counts"] = ft.get("fault_counts", {})
        result["results_tail"] = (ft.get("results") or [])[-5:]

    return result


@app.get("/api/jobs/{job_id}/current-tc")
def get_current_tc(job_id: str):
    """Return the test case CR that is currently being applied (or was last applied)."""
    if job_id not in _jobs:
        raise HTTPException(404, "Job not found")
    job = _jobs[job_id]
    mode = job.get("mode", "")


    ckpt_path: str = ""
    log_buf = list(_log_buffers.get(job_id, []))
    for line in reversed(log_buf):
        idx = line.find("Checkpoint:")
        if idx != -1:
            candidate = line[idx + len("Checkpoint:") :].strip()
            if not os.path.isabs(candidate):
                candidate = os.path.join(_GSOD_ROOT, candidate)
            if os.path.isfile(candidate):
                ckpt_path = candidate
                break

    if not ckpt_path:
        return {"ok": False, "reason": "checkpoint not found yet"}

    try:
        with open(ckpt_path, encoding="utf-8") as _f:
            ckpt = json.load(_f)
    except Exception as e:
        return {"ok": False, "reason": str(e)}


    if mode in ("run", "e2e"):
        e2e = ckpt.get("e2e_test", {})
        results = e2e.get("results", [])
        rounds_done = e2e.get("rounds", 0)

        current_tc_id = None
        current_cr_yaml = ""
        if results:
            last = results[-1]
            current_tc_id = last.get("tc_id", "")

        tp = ckpt.get("testplan", {})
        testcases = tp.get("testcases", {})
        if current_tc_id and current_tc_id in testcases:
            current_cr_yaml = testcases[current_tc_id].get("cr", "")
        return {
            "ok": True,
            "mode": mode,
            "round": rounds_done,
            "tc_id": current_tc_id,
            "cr_yaml": current_cr_yaml,
        }
    elif mode == "fault":
        ft = ckpt.get("fault_test", {})
        results = ft.get("results", [])
        rounds_done = ft.get("rounds", 0)
        current_tc_id = None
        current_cr_yaml = ""
        if results:
            last = results[-1]
            current_tc_id = last.get("tc_id", "")
        tp = ckpt.get("testplan", {})
        testcases = tp.get("testcases", {})
        if current_tc_id and current_tc_id in testcases:
            current_cr_yaml = testcases[current_tc_id].get("cr", "")
        return {
            "ok": True,
            "mode": mode,
            "round": rounds_done,
            "tc_id": current_tc_id,
            "cr_yaml": current_cr_yaml,
        }
    return {"ok": False, "reason": f"mode '{mode}' does not have test cases"}


@app.get("/api/branch_source")
def get_branch_source(
    instrument_dir: str = Query(
        ..., description="Instrument directory (parent of instrument_info.json)"
    ),
    branch_index: int = Query(..., description="BranchIndex"),
    project_path: str = Query("", description="Operator source root (optional)"),
):
    """Return source code context for a branch via LocateBranchSource."""
    try:
        import sys as _sys

        _root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if _root not in _sys.path:
            _sys.path.insert(0, _root)
        from instrumentation.source import _get_branch_source_context
    except ImportError as e:
        raise HTTPException(501, f"instrumentation.source unavailable: {e}")
    if not os.path.isdir(instrument_dir):
        raise HTTPException(404, "instrument_dir not found")
    source = _get_branch_source_context(
        project_path=project_path or "",
        instrument_dir=instrument_dir,
        branch_index=branch_index,
    )
    return {"source": source, "ok": bool(source), "branch_index": branch_index}


@app.get("/api/branch_relations")
def view_branch_relations(
    instrument_path: str = Query(..., description="Path to instrument_info.json"),
    relations_path: str = Query(..., description="Path to field_relations.json"),
):
    """Return a branch-centric view: branch_idx → {fmt, file, line, fields:[{path,vars}]}."""
    if not os.path.isfile(instrument_path):
        raise HTTPException(404, "instrument_info.json not found")
    if not os.path.isfile(relations_path):
        raise HTTPException(404, "field_relations.json not found")

    with open(instrument_path, encoding="utf-8") as f:
        info = json.load(f)
    branch_meta: dict = {bp["BranchIndex"]: bp for bp in info.get("branch_points", [])}

    with open(relations_path, encoding="utf-8") as f:
        field_relations: dict = json.load(f)


    reverse: dict = {}
    for fp, fdata in field_relations.items():
        vm = fdata.get("variable_mappings", {})
        ef = fdata.get("expression_fmts", {})
        for bi in fdata.get("branch_indices", []):
            bi_key = str(bi)
            if bi_key not in reverse:
                bm = branch_meta.get(bi, {})
                exprs = bm.get("Expressions", [])
                fmt = ""
                if exprs:
                    fmt = (
                        exprs[0].get("fmt")
                        or exprs[0].get("raw")
                        or exprs[0].get("Fmt")
                        or ""
                    )
                reverse[bi_key] = {
                    "idx": bi,
                    "fmt": fmt,
                    "file": bm.get("File", bm.get("FilePath", "")),
                    "line": bm.get("BranchLine", bm.get("Line", "")),
                    "fields": [],
                }
            entry = reverse[bi_key]

            vars_for_bi = []
            if bi_key in vm:
                for vinfo in vm[bi_key].values():
                    if isinstance(vinfo, dict) and vinfo.get("variable_fmt"):
                        vars_for_bi.append(vinfo["variable_fmt"])
            if bi_key in ef:
                vars_for_bi.extend(ef[bi_key])

            seen = set()
            unique_vars = [v for v in vars_for_bi if not (v in seen or seen.add(v))]
            entry["fields"].append({"path": fp, "vars": unique_vars})


    return dict(sorted(reverse.items(), key=lambda x: int(x[0])))


_UI_STATE_FILE = "ui_state.json"


def _default_project_state() -> dict:
    return {
        "steps": {
            "instrumentation": {"status": "completed"},
            "relation_analysis": {"status": "idle", "checkpoint": ""},
            "test_plan": {"status": "idle", "checkpoint": ""},
            "e2e_test": {"status": "idle", "checkpoint": ""},
            "fault_test": {"status": "idle", "checkpoint": ""},
        },
        "stats": {
            "total_duration_sec": 0,
            "test_cases_run": 0,
            "fault_injections": 0,
            "bugs_found": 0,
        },
    }


def _scan_projects() -> List[dict]:
    """Scan data/ directory for projects that have a runner.yaml."""
    result = []
    if not os.path.isdir(DATA_BASE):
        return result
    for name in sorted(os.listdir(DATA_BASE)):
        subdir = os.path.join(DATA_BASE, name)
        if not os.path.isdir(subdir):
            continue
        runner = os.path.join(subdir, "runner.yaml")
        if not os.path.isfile(runner):
            continue
        state_file = os.path.join(subdir, _UI_STATE_FILE)
        result.append(
            {
                "name": name,
                "has_runner": True,
                "has_state": os.path.isfile(state_file),
            }
        )
    return result


def _read_project_state(name: str) -> dict:
    state_file = os.path.join(DATA_BASE, name, _UI_STATE_FILE)
    if os.path.isfile(state_file):
        try:
            with open(state_file, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return _default_project_state()


def _write_project_state(name: str, state: dict):
    state_file = os.path.join(DATA_BASE, name, _UI_STATE_FILE)
    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


@app.get("/api/projects")
def list_projects():
    return _scan_projects()


@app.get("/api/projects/{name}/state")
def get_project_state(name: str):
    proj_dir = os.path.join(DATA_BASE, name)
    if not os.path.isdir(proj_dir):
        raise HTTPException(404, "Project not found")
    return _read_project_state(name)


@app.put("/api/projects/{name}/state")
def save_project_state(name: str, state: dict):
    proj_dir = os.path.join(DATA_BASE, name)
    if not os.path.isdir(proj_dir):
        raise HTTPException(404, "Project not found")
    _write_project_state(name, state)
    return {"ok": True}


_COLLECTOR_URL = os.environ.get(
    "GSOD_COLLECTOR_URL",
    "",
)


def _resolve_cluster_info(project: str = "") -> dict:
    """Return {'cluster_name': str, 'kubeconfig': str, 'operator_namespace': str} from runner.yaml or env."""
    kc_env = os.environ.get("GSOD_KUBECONFIG", "")
    cluster_env = os.environ.get("GSOD_CLUSTER_NAME", "")
    result = {
        "cluster_name": cluster_env,
        "kubeconfig": kc_env,
        "operator_namespace": "",
    }
    if project and _yaml is not None:
        runner_yaml = os.path.join(DATA_BASE, project, "runner.yaml")
        if os.path.isfile(runner_yaml):
            try:
                with open(runner_yaml, encoding="utf-8") as f:
                    ry = _yaml.safe_load(f) or {}
                common = ry.get("common") or {}
                cluster_name = common.get("reuse_cluster", "")
                if cluster_name and not result["cluster_name"]:
                    result["cluster_name"] = cluster_name
                if cluster_name and not result["kubeconfig"]:
                    result["kubeconfig"] = os.path.join(
                        os.path.expanduser("~"), ".kube", f"kind-{cluster_name}"
                    )
                result["operator_namespace"] = common.get("operator_namespace", "")
            except Exception:
                pass
    return result


def _resolve_kubeconfig(project: str = "") -> str:
    """Derive kubeconfig path for a project from runner.yaml or env."""
    return _resolve_cluster_info(project).get("kubeconfig", "")


def _resolve_operator_deploy_info(project: str = "") -> dict:
    """Parse config.deploy steps to find the operator Deployment's namespace+label selector.

    Reads runner.yaml → common.config → deploy.steps, finds the step with
    operator:true, opens that YAML file, and extracts the first Deployment's
    namespace and spec.selector.matchLabels.

    Returns {'namespace': str, 'label_selector': str, 'deploy_name': str}
    or empty strings if not found.
    """
    if not project or _yaml is None:
        return {"namespace": "", "label_selector": "", "deploy_name": ""}
    runner_yaml = os.path.join(DATA_BASE, project, "runner.yaml")
    if not os.path.isfile(runner_yaml):
        return {"namespace": "", "label_selector": "", "deploy_name": ""}
    try:
        with open(runner_yaml, encoding="utf-8") as f:
            ry = _yaml.safe_load(f) or {}
        config_path = (ry.get("common") or {}).get("config", "")
        if not config_path:
            return {"namespace": "", "label_selector": "", "deploy_name": ""}

        if not os.path.isabs(config_path):
            config_path = os.path.join(_GSOD_ROOT, config_path)
        with open(config_path, encoding="utf-8") as f:
            cfg = json.load(f)

        operator_file = ""
        for step in (cfg.get("deploy") or {}).get("steps", []):
            apply = step.get("apply") or {}
            if apply.get("operator"):
                operator_file = apply.get("file", "")
                break
        if not operator_file:
            return {"namespace": "", "label_selector": "", "deploy_name": ""}
        if not os.path.isabs(operator_file):
            operator_file = os.path.join(_GSOD_ROOT, operator_file)
        if not os.path.isfile(operator_file):
            return {"namespace": "", "label_selector": "", "deploy_name": ""}

        with open(operator_file, encoding="utf-8") as f:
            for doc in _yaml.safe_load_all(f):
                if not isinstance(doc, dict):
                    continue
                if doc.get("kind") != "Deployment":
                    continue
                meta = doc.get("metadata") or {}
                spec = doc.get("spec") or {}
                ns = meta.get("namespace", "")
                name = meta.get("name", "")
                match_labels = (spec.get("selector") or {}).get("matchLabels") or {}
                label_sel = ",".join(f"{k}={v}" for k, v in match_labels.items())
                return {
                    "namespace": ns,
                    "label_selector": label_sel,
                    "deploy_name": name,
                }
    except Exception:
        pass
    return {"namespace": "", "label_selector": "", "deploy_name": ""}


def _kubectl_cmd(kubeconfig: str = "", project: str = "") -> list:
    """Build base kubectl command with optional kubeconfig."""
    kc = kubeconfig or _resolve_kubeconfig(project)
    cmd = ["kubectl"]
    if kc and os.path.isfile(kc):
        cmd += ["--kubeconfig", kc]
    return cmd


def _get_node_ip(kubeconfig: str = "", project: str = "") -> str:
    """Return the first ready node's InternalIP (for NodePort access)."""
    cmd = _kubectl_cmd(kubeconfig, project) + [
        "get",
        "nodes",
        "-o",
        "jsonpath={.items[0].status.addresses[?(@.type=='InternalIP')].address}",
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=8)
        return r.stdout.strip()
    except Exception:
        return ""


def _find_free_port() -> int:
    with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        s.listen(1)
        return s.getsockname()[1]


def _collector_portfwd_get(
    path: str,
    cluster_name: str,
    kubeconfig: str = "",
    timeout: int = 8,
) -> dict:
    """Fetch JSON from collector via kubectl port-forward (mirrors port_forward_context_v2)."""
    import signal as _signal

    local_port = _find_free_port()
    kc = kubeconfig or os.path.join(
        os.path.expanduser("~"), ".kube", f"kind-{cluster_name}"
    )
    cmd = [
        "kubectl",
        "--kubeconfig",
        kc,
        "port-forward",
        "-n",
        "data-service",
        "svc/data-collection-service-external",
        f"{local_port}:80",
    ]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        preexec_fn=os.setsid if os.name != "nt" else None,
    )

    deadline = time.time() + 10
    ready = False
    while time.time() < deadline:
        try:
            s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
            s.settimeout(1)
            if s.connect_ex(("localhost", local_port)) == 0:
                ready = True
                s.close()
                break
            s.close()
        except Exception:
            pass
        if proc.poll() is not None:
            _, err = proc.communicate()
            raise HTTPException(502, f"Collector port-forward exited: {err[:200]}")
        time.sleep(0.3)
    if not ready:
        proc.terminate()
        raise HTTPException(
            502, f"Collector port-forward did not become ready (cluster={cluster_name})"
        )
    try:
        url = f"http://localhost:{local_port}" + path
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read())
    except Exception as exc:
        raise HTTPException(502, f"Collector unreachable via port-forward: {exc}")
    finally:
        if os.name != "nt":
            try:
                os.killpg(os.getpgid(proc.pid), _signal.SIGTERM)
            except Exception:
                proc.terminate()
        else:
            proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()


def _collector_get(
    path: str,
    collector_url: str = "",
    project: str = "",
    kubeconfig: str = "",
    timeout: int = 8,
) -> dict:
    """Fetch JSON from the collector: use explicit URL if given, else port-forward."""
    base = collector_url or _COLLECTOR_URL
    if base:
        if not base.startswith("http"):
            raise HTTPException(502, f"Invalid collector URL: {base!r}")
        url = base.rstrip("/") + path
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                return json.loads(resp.read())
        except Exception as exc:
            raise HTTPException(502, f"Collector unreachable: {exc}")

    info = _resolve_cluster_info(project)
    cluster_name = info.get("cluster_name", "")
    kc = kubeconfig or info.get("kubeconfig", "")
    if not cluster_name:
        raise HTTPException(
            502,
            "Collector URL not set and cluster name unknown. "
            "Select a project or pass collector_url.",
        )
    return _collector_portfwd_get(path, cluster_name, kc, timeout)


@app.get("/api/collector_url")
def get_collector_url(
    project: str = Query(default=""),
    kubeconfig: str = Query(default=""),
):
    """Return the best collector URL: env override, then NodePort auto-detect."""
    if _COLLECTOR_URL:
        return {"url": _COLLECTOR_URL, "source": "env"}
    node_ip = _get_node_ip(kubeconfig, project)
    if node_ip:
        url = f"http://{node_ip}:30080"
        return {"url": url, "source": "nodeport", "node_ip": node_ip}

    info = _resolve_cluster_info(project)
    cluster_name = info.get("cluster_name", "")
    if cluster_name:
        return {
            "url": "",
            "source": "portfwd",
            "cluster_name": cluster_name,
            "message": f"Will use kubectl port-forward to cluster '{cluster_name}' automatically",
        }
    return {
        "url": "",
        "source": "none",
        "error": "could not determine node IP or cluster name",
    }


@app.get("/api/fault/round_logs")
def get_fault_round_logs(
    n: int = Query(default=20),
    resource: str = Query(default=""),
    fault_only: bool = Query(default=False),
    collector_url: str = Query(default=""),
    project: str = Query(default=""),
    kubeconfig: str = Query(default=""),
):
    """Proxy recent per-reconcile round logs from the collector service."""
    params = f"?n={n}&fault_only={'true' if fault_only else 'false'}"
    if resource:
        params += f"&resource={urllib.parse.quote(resource)}"
    return _collector_get(f"/round_logs{params}", collector_url, project, kubeconfig)


@app.get("/api/fault/round_logs/latest")
def get_latest_round_log(
    resource: str = Query(default=""),
    collector_url: str = Query(default=""),
    project: str = Query(default=""),
    kubeconfig: str = Query(default=""),
):
    """Proxy the latest round log entry from the collector service."""
    params = f"?resource={urllib.parse.quote(resource)}" if resource else ""
    return _collector_get(
        f"/round_logs/latest{params}", collector_url, project, kubeconfig
    )


@app.get("/api/fault/events")
def get_fault_events(
    n: int = Query(default=50),
    since_ms: int = Query(default=0),
    collector_url: str = Query(default=""),
    project: str = Query(default=""),
    kubeconfig: str = Query(default=""),
):
    """Proxy fault injection events reported directly by the proxy to the collector."""
    params = f"?n={n}&since_ms={since_ms}"
    return _collector_get(f"/fault_events{params}", collector_url, project, kubeconfig)


@app.get("/api/operator_deploy")
def get_operator_deploy(project: str = Query(default="")):
    """Return the operator Deployment name, namespace, and label selector for a project."""
    return _resolve_operator_deploy_info(project)


@app.get("/api/pods")
def get_pods(
    namespace: str = Query(default=""),
    label_selector: str = Query(default=""),
    kubeconfig: str = Query(default=""),
    project: str = Query(default=""),
):
    """Query pod status via kubectl; returns list of pod summaries."""
    cmd = _kubectl_cmd(kubeconfig, project) + ["get", "pods", "-o", "json"]
    if namespace:
        cmd += ["-n", namespace]
    else:
        cmd += ["--all-namespaces"]
    if label_selector:
        cmd += ["-l", label_selector]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    except FileNotFoundError:
        raise HTTPException(503, "kubectl not found in PATH")
    except subprocess.TimeoutExpired:
        raise HTTPException(504, "kubectl timed out")
    if result.returncode != 0:
        raise HTTPException(500, result.stderr[:500])
    try:
        raw = json.loads(result.stdout)
    except json.JSONDecodeError:
        raise HTTPException(500, "kubectl returned non-JSON output")

    EXCLUDED_NS = {"kube-system", "kube-public", "kube-node-lease"}
    pods = []
    for item in raw.get("items", []):
        meta = item.get("metadata", {})
        spec = item.get("spec", {})
        status = item.get("status", {})
        if meta.get("namespace", "") in EXCLUDED_NS:
            continue

        cs_list = []
        for cs in status.get("containerStatuses", []):
            state = cs.get("state", {})
            st_key = next(iter(state), "unknown")
            reason = (state.get(st_key) or {}).get("reason", "")
            cs_list.append(
                {
                    "name": cs.get("name", ""),
                    "ready": cs.get("ready", False),
                    "restartCount": cs.get("restartCount", 0),
                    "state": st_key,
                    "reason": reason,
                    "image": cs.get("image", ""),
                }
            )
        pods.append(
            {
                "name": meta.get("name", ""),
                "namespace": meta.get("namespace", ""),
                "labels": meta.get("labels") or {},
                "phase": status.get("phase", "Unknown"),
                "podIP": status.get("podIP", ""),
                "nodeName": spec.get("nodeName", ""),
                "startTime": status.get("startTime", ""),
                "conditions": [
                    {"type": c.get("type"), "status": c.get("status")}
                    for c in status.get("conditions", [])
                ],
                "containers": cs_list,
            }
        )
    op_deploy = _resolve_operator_deploy_info(project)
    return {"count": len(pods), "pods": pods, "operator_deploy": op_deploy}


_TEMPLATE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "gsod_ui_template.html"
)


@app.get("/", response_class=HTMLResponse)
def ui_root():
    with open(_TEMPLATE_PATH, encoding="utf-8") as f:
        return HTMLResponse(content=f.read())


_HTML_LEGACY_REMOVED = True


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="GSOD v5 Web UI")
    parser.add_argument("--port", type=int, default=7860, help="监听端口 (默认 7860)")
    parser.add_argument("--host", default="0.0.0.0", help="监听地址 (默认 0.0.0.0)")
    parser.add_argument(
        "--instrument-base", default=INSTRUMENT_BASE, help="插桩程序根目录"
    )
    args = parser.parse_args()

    INSTRUMENT_BASE = args.instrument_base

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")