from setuptools import setup, find_packages

setup(
    name = "telemetry-schema-transpiler",
    version = "0.1",
    packages = find_packages(),
    install_requires=[],
    extras_require={
        "test": ["pytest"]
    }
)
