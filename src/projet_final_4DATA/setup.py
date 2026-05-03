from setuptools import find_packages, setup

setup(
    name="projet_final_4DATA",
    packages=find_packages(exclude=["projet_final_4DATA_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
