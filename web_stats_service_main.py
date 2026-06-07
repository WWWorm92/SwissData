import os

import uvicorn

from web_stats_service.app import app


def main():
    host = str(os.getenv("SWISS_STATS_HOST", "0.0.0.0")).strip() or "0.0.0.0"
    try:
        port = int(str(os.getenv("SWISS_STATS_PORT", "18080")).strip() or "18080")
    except Exception:
        port = 18080
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
