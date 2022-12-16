#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-woocommerce",
    version="0.1.4",
    description="Singer.io tap for extracting data",
    author="Safedigit",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_woocommerce"],
    install_requires=[
        'attrs==18.1.0',
        'backoff==1.8.0',
        'python-dateutil==2.7.3',
        "pendulum",
        'singer-python==5.12.2',
        'woocommerce==3.0.0'
    ],
    entry_points="""
    [console_scripts]
    tap-woocommerce=tap_woocommerce:main
    """,
    packages=["tap_woocommerce"],
    package_data={
        "schemas": ["tap_woocommerce/schemas/*.json"]
    },
    include_package_data=True,
)
