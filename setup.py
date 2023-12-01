#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-circleci",
    version="0.1.0",
    description="Singer.io tap for extracting data from circle ci",
    author="Sisu Data",
    url="http://sisu.ai",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_circle_ci"],
    install_requires=[
        "singer-python>=5.0.12",
        "requests"
    ],
    extras_require={
        'tests': [
            "mock",
            "pytest"
        ]},
    entry_points="""
    [console_scripts]
    tap-circleci=tap_circle_ci:main
    """,
    packages=["tap_circle_ci"],
    package_data={
        "schemas": ["tap_circle_ci/schemas/*.json"]
    },
    include_package_data=True,
)
