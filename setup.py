from typing import Dict

from setuptools import find_packages, setup  # type: ignore


def get_version() -> str:
    version: Dict[str, str] = {}
    with open("lull_dagster_dbt/version.py") as fp:
        exec(fp.read(), version)  # pylint: disable=W0122

    return version["__version__"]


if __name__ == "__main__":
    ver = get_version()
    setup(
        name="lull-dagster-dbt",
        version=ver,
        author="Lull",
        author_email="dataeng@lull.com",
        description="Package for Lull dbt Dagster framework solid and resource components.",
        url="https://github.com/LullCode/lull-dagster-dbt",
        classifiers=[
            "Programming Language :: Python :: 3.6",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
            "Operating System :: OS Independent",
        ],
        packages=find_packages(exclude=["tests"]),
        install_requires=[
            "dagster==0.13.12",
            "packaging",
            "requests",
            "pytest-cov"
        ],
        extras_require={
            "test": ["moto>=2.2.8", "requests-mock"],
        },
        zip_safe=False,
    )