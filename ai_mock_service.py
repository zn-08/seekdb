#!/usr/bin/env python3
"""A mock AI service for the AI function tests.

The tests of the AI function challenge talk to an HTTP service that answers the
OpenAI compatible, the SiliconFlow rerank and the Ollama endpoints.  This
service implements exactly those endpoints with deterministic answers, so the
tests can run without a real model.

Usage:
    python3 ai_mock_service.py [--host 0.0.0.0] [--port 8000]
"""

from __future__ import annotations

import argparse
import hashlib
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DEFAULT_DIMENSION = 768


def _vector(text: str, dimension: int) -> list[float]:
    """Return a deterministic vector for one text."""

    digest = hashlib.sha256(text.encode("utf-8")).digest()
    values = []
    for index in range(dimension):
        values.append(((digest[index % len(digest)] + index) % 1000) / 1000.0)
    return values


def _text_of(document) -> str:
    if isinstance(document, str):
        return document
    if isinstance(document, dict):
        for key in ("text", "content", "id"):
            if key in document:
                return str(document[key])
    return json.dumps(document, ensure_ascii=False, sort_keys=True)


class Handler(BaseHTTPRequestHandler):
    """Answer the endpoints that the AI function tests use."""

    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # noqa: A003 - the base class defines it
        print("[mock-ai] " + (fmt % args), flush=True)

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {}

    def _send(self, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):  # noqa: N802 - the base class defines it
        request = self._read_body()
        path = self.path.split("?")[0].rstrip("/")
        if path.endswith("/chat/completions"):
            self._send({"choices": [{"message": {"content": "mock completion"}}]})
        elif path.endswith("/embeddings"):
            self._send(self._embeddings(request))
        elif path.endswith("/rerank"):
            self._send(self._rerank(request))
        elif path.endswith("/api/generate"):
            self._send({"response": "mock ollama completion"})
        elif path.endswith("/api/embed"):
            self._send(self._ollama_embeddings(request))
        else:
            self._send({"error": "unsupported endpoint {}".format(path)})

    def _embeddings(self, request: dict) -> dict:
        inputs = request.get("input")
        if isinstance(inputs, str):
            inputs = [inputs]
        if not isinstance(inputs, list):
            inputs = []
        dimension = int(request.get("dimensions") or DEFAULT_DIMENSION)
        return {"data": [{"embedding": _vector(str(item), dimension)} for item in inputs]}

    def _ollama_embeddings(self, request: dict) -> dict:
        inputs = request.get("input")
        if isinstance(inputs, str):
            inputs = [inputs]
        if not isinstance(inputs, list):
            inputs = []
        return {"embeddings": [_vector(str(item), DEFAULT_DIMENSION) for item in inputs]}

    def _rerank(self, request: dict) -> dict:
        documents = request.get("documents")
        if not isinstance(documents, list):
            documents = []
        query = str(request.get("query") or "")
        scored = []
        for index, document in enumerate(documents):
            text = _text_of(document)
            # The first document that shares a word with the query scores best,
            # so the answer keeps the order of the documents.
            score = 0.9 - 0.01 * index
            if query and query.lower() in text.lower():
                score = 1.0 - 0.01 * index
            scored.append({"index": index, "relevance_score": score})
        scored.sort(key=lambda item: -item["relevance_score"])
        return {"results": scored}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print("[mock-ai] listening on {}:{}".format(args.host, args.port), flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
