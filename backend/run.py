import os

import uvicorn


def _as_bool(value):
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)
