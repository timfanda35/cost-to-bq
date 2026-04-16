import logging
import os
from flask import Flask, jsonify
from src.pipeline import run_pipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/run", methods=["POST"])
def run():
    try:
        result = run_pipeline()
        logger.info("Pipeline complete: %s", result)
        return jsonify(result), 200
    except Exception as exc:
        logger.exception("Pipeline failed")
        return jsonify({"error": str(exc)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
