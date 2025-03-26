from setuptools import find_packages, setup

setup(
    name="vvs_dagster",
    packages=find_packages(exclude=["vvs_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-postgres",
        "dagster-docker",
        "dagster-cloud",
        "dagster-aws",
        "pandas"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
