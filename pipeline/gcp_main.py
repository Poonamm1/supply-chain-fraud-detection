"""
pipeline.gcp_main — Unified Dataflow Entrypoint
===============================================
Single entrypoint for both batch and streaming Flex Templates.

Usage:
    Batch mode:
      python gcp_main.py --mode=batch \
        --wms_input=gs://bucket/wms.jsonl \
        --erp_input=gs://bucket/erp.jsonl \
        --bq_dataset=project:dataset \
        --runner=DataflowRunner

    Streaming mode:
      python gcp_main.py --mode=streaming \
        --wms_subscription=projects/PROJECT/subscriptions/wms-sub \
        --erp_subscription=projects/PROJECT/subscriptions/erp-sub \
        --bq_dataset=project:dataset \
        --runner=DataflowRunner

This allows a SINGLE Flex Template Docker image to support both modes
by passing --mode as a template parameter.
"""
import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger(__name__)


def main():
    """Parse --mode and delegate to the appropriate main module."""
    parser = argparse.ArgumentParser(
        description="Unified Dataflow entrypoint for batch and streaming modes"
    )
    parser.add_argument(
        "--mode",
        choices=["batch", "streaming"],
        required=True,
        help="Pipeline mode: 'batch' (GCS) or 'streaming' (Pub/Sub)"
    )
    
    # Parse only the --mode flag, pass the rest to the sub-module
    args, remaining = parser.parse_known_args()
    
    if args.mode == "batch":
        log.info("🚀 Launching BATCH pipeline (GCS → BigQuery)")
        from pipeline.gcp_batch_main import run
        run(remaining)
    
    elif args.mode == "streaming":
        log.info("🚀 Launching STREAMING pipeline (Pub/Sub → BigQuery)")
        from pipeline.gcp_stream_main import run
        run(remaining)
    
    else:
        log.error(f"Unknown mode: {args.mode}")
        sys.exit(1)


if __name__ == "__main__":
    main()
