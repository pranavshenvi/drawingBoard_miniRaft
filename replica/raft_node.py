import asyncio
from aiohttp import web
import json

log = []

# ================= STORAGE =================

async def handle_append(request):
    data = await request.json()
    log.append(data)

    print(f"[PY] Stored entry. Total logs: {len(log)}", flush=True)

    return web.json_response({
        "status": "ok",
        "log_length": len(log)
    })


async def handle_get_logs(request):
    return web.json_response({
        "logs": log,
        "count": len(log)
    })


async def handle_health(request):
    return web.json_response({"status": "healthy"})


# ================= SERVER =================

async def create_app():
    app = web.Application()

    app.router.add_post('/append', handle_append)
    app.router.add_get('/logs', handle_get_logs)
    app.router.add_get('/health', handle_health)

    return app


async def main():
    app = await create_app()

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, '0.0.0.0', 6000)
    await site.start()

    print("[PY] Storage service running on port 6000", flush=True)

    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())