"""Install configuration for the refinement package"""
from setuptools import setup

setup(
    name="refinement",
    version="0.1.0",
    where="src",
    include="refinement",
    setup_requires=[
        "networkx>=3.2.0",
        "tqdm>4.6.0",
        "numpy>=1.26.0",
        "geopy>=2.4.0",
    ],
)
