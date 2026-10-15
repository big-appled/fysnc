"""智能文件迁移 - 后端服务（FastAPI + WebSocket + 静态资源托管）。

浏览器无法访问本地文件系统，因此真正的扫描 / 拷贝 / 校验全部由本后端完成，
前端只负责展示与下发指令。前后端同源部署，无需处理 CORS。
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from engine import engine, longpath

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")


# --------------------------------------------------------------------------- #
# WebSocket 广播中心
# --------------------------------------------------------------------------- #
class Hub:
    def __init__(self) -> None:
        self.clients: set[WebSocket] = set()

    async def register(self, ws: WebSocket) -> None:
        self.clients.add(ws)

    async def unregister(self, ws: WebSocket) -> None:
        self.clients.discard(ws)

    async def broadcast(self, payload: dict) -> None:
        if not self.clients:
            return
        text = json.dumps(payload, ensure_ascii=False)
        for ws in list(self.clients):
            try:
                await ws.send_text(text)
            except Exception:  # noqa: BLE001
                self.clients.discard(ws)


hub = Hub()
_last_log_seq = engine.log_seq


async def _pusher() -> None:
    """以 ~3Hz 推送任务进度与增量日志，避免高频刷新拖垮前端。"""
    global _last_log_seq
    while True:
        await asyncio.sleep(0.35)
        if not hub.clients:
            _last_log_seq = engine.log_seq
            continue
        _last_log_seq = engine.log_seq
        await hub.broadcast({
            "type": "update",
            "ts": time.time(),
            "tasks": engine.snapshot(),
            "aggregate": engine.aggregate(),
            "logs": engine.logs_since(_last_log_seq, limit=200),
        })


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_pusher())
    try:
        yield
    finally:
        task.cancel()
        await asyncio.sleep(0.05)
        engine.flush()


app = FastAPI(title="智能文件迁移", lifespan=lifespan)


@app.middleware("http")
async def no_cache(request, call_next):
    """禁用静态资源缓存，避免前端改动后浏览器仍使用旧文件。"""
    resp = await call_next(request)
    path = request.url.path
    if path == "/" or path.endswith((".js", ".css", ".html")):
        resp.headers["Cache-Control"] = "no-store, must-revalidate"
    return resp


# --------------------------------------------------------------------------- #
# 数据模型
# --------------------------------------------------------------------------- #
class CreateTaskReq(BaseModel):
    name: str = ""
    source: str
    dest: str
    options: dict | None = None


# --------------------------------------------------------------------------- #
# 校验
# --------------------------------------------------------------------------- #
def validate_paths(source: str, dest: str) -> tuple[bool, str]:
    if not source or not dest:
        return False, "源路径与目标路径不能为空"
    s = os.path.abspath(source)
    d = os.path.abspath(dest)
    if not os.path.exists(longpath(s)):
        return False, f"源路径不存在: {s}"
    if not os.path.isdir(longpath(s)):
        return False, f"源路径不是目录: {s}"
    if s == d:
        return False, "目标路径不能与源路径相同"
    if d.startswith(s.rstrip("\\/") + os.sep):
        return False, "目标路径不能位于源路径内部"
    return True, ""


def _get(tid: str):
    task = engine.tasks.get(tid)
    if task is None:
        return None, JSONResponse({"error": "任务不存在"}, status_code=404)
    return task, None


# --------------------------------------------------------------------------- #
# REST 接口
# --------------------------------------------------------------------------- #
@app.get("/api/tasks")
async def list_tasks():
    return {"tasks": engine.snapshot(), "aggregate": engine.aggregate()}


@app.post("/api/tasks")
async def create_task(req: CreateTaskReq):
    ok, msg = validate_paths(req.source, req.dest)
    if not ok:
        return JSONResponse({"error": msg}, status_code=400)
    task = engine.create(req.name, req.source, req.dest, req.options)
    return JSONResponse(task.snapshot(), status_code=201)


@app.post("/api/tasks/{tid}/pause")
async def pause_task(tid: str):
    task, err = _get(tid)
    if err:
        return err
    engine.pause(task)
    engine.log("INFO", "任务已暂停", task)
    return task.snapshot()


@app.post("/api/tasks/{tid}/resume")
async def resume_task(tid: str):
    task, err = _get(tid)
    if err:
        return err
    if task.status == "interrupted":
        # 进程中断遗留的任务，resume 实际上是 retry（重跑从 0 开始更安全）
        engine.retry(task)
        engine.log("INFO", "从 interrupted 状态重新启动任务", task)
    else:
        engine.resume(task)
        engine.log("INFO", "任务已恢复", task)
    return task.snapshot()


@app.post("/api/tasks/{tid}/cancel")
async def cancel_task(tid: str):
    task, err = _get(tid)
    if err:
        return err
    engine.cancel(task)
    return task.snapshot()


@app.post("/api/tasks/{tid}/retry")
async def retry_task(tid: str):
    task, err = _get(tid)
    if err:
        return err
    engine.retry(task)
    return task.snapshot()


@app.delete("/api/tasks/{tid}")
async def delete_task(tid: str):
    task, err = _get(tid)
    if err:
        return err
    engine.remove(tid)
    return {"ok": True}


@app.get("/api/logs")
async def get_logs(after: int = 0, task_id: str = "", level: str = "", limit: int = 400):
    return {"logs": engine.logs_since(after, task_id, level, limit)}


@app.get("/api/list-dirs")
async def list_dirs(path: str = ""):
    """供「新建任务」目录选择器使用。"""
    if not path:
        path = os.path.expanduser("~")
    p = os.path.abspath(path)
    dirs: list[str] = []
    try:
        with os.scandir(longpath(p)) as it:
            for entry in it:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        dirs.append(entry.name)
                except OSError:
                    continue
    except OSError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    dirs.sort(key=str.lower)
    return {"path": p, "parent": os.path.dirname(p.rstrip("\\/")) or p, "dirs": dirs}


# --------------------------------------------------------------------------- #
# WebSocket
# --------------------------------------------------------------------------- #
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    await hub.register(ws)
    try:
        await ws.send_text(json.dumps({
            "type": "init",
            "tasks": engine.snapshot(),
            "aggregate": engine.aggregate(),
            "logs": engine.logs_since(0, limit=300),
        }, ensure_ascii=False))
        while True:
            await ws.receive_text()  # 心跳
    except WebSocketDisconnect:
        pass
    except Exception:  # noqa: BLE001
        pass
    finally:
        await hub.unregister(ws)


# --------------------------------------------------------------------------- #
# 前端静态资源
# --------------------------------------------------------------------------- #
@app.get("/")
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")


if __name__ == "__main__":
    import uvicorn

    host = os.environ.get("FSSYNC_HOST", "127.0.0.1")
    port = int(os.environ.get("FSSYNC_PORT", "8000"))
    print(f"智能文件迁移已启动: http://{host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="warning")
