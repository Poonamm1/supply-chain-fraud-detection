"""
pipeline.main — Step 4.1: Skeleton Pass-Through
================================================
Read JSONL files -> parse to typed events -> print to console.
No fraud logic yet. We're only proving plumbing works.

Run from the project root with the venv active:
    python -m pipeline.main
"""
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from .config import PipelineConfig
from .transforms import ParseErpLine, ParseWmsLine


def run() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s :: %(message)s",
    )
    cfg = PipelineConfig()

    opts = PipelineOptions()
    opts.view_as(StandardOptions).runner = "DirectRunner"

    with beam.Pipeline(options=opts) as p:

        # --- WMS Receiving stream ---
        wms = (
            p
            | "ReadWms"  >> beam.io.ReadFromText(cfg.wms_input_path)
            | "ParseWms" >> beam.ParDo(ParseWmsLine())
                              .with_outputs(ParseWmsLine.DEAD_LETTER, main="ok")
        )

        # --- ERP Invoice stream ---
        erp = (
            p
            | "ReadErp"  >> beam.io.ReadFromText(cfg.erp_input_path)
            | "ParseErp" >> beam.ParDo(ParseErpLine())
                              .with_outputs(ParseErpLine.DEAD_LETTER, main="ok")
        )

        # --- Print parsed records (Step 4.1 only — swap for real sinks later) ---
        wms.ok | "PrintWms" >> beam.Map(lambda e: print(f"WMS: {e}"))
        erp.ok | "PrintErp" >> beam.Map(lambda e: print(f"ERP: {e}"))


if __name__ == "__main__":
    run()
