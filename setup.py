"""setup.py — required by Dataflow to package our `pipeline/` module
onto worker VMs. Without this, workers can't import pipeline.transforms."""
import setuptools

setuptools.setup(
    name="supply_chain_fraud_pipeline",
    version="0.1.0",
    packages=setuptools.find_packages(),
    install_requires=[
        "apache-beam[gcp]>=2.73.0",
        "google-cloud-bigquery>=3.0",
    ],
)
