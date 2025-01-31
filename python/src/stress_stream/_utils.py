from fastapi import FastAPI
from prometheus_client import make_asgi_app
from uvicorn import Config, Server


async def metrics_server():
    app = FastAPI()
    metrics_app = make_asgi_app()
    app.mount("/metrics", metrics_app)
    config = Config(app=app, host="0.0.0.0", port=8000)
    server = Server(config)
    await server.serve()
