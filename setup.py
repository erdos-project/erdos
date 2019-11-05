from setuptools import find_packages, setup

setup(
    name="erdos",
    version="0.1.3",
    author="ERDOS Team",
    description=("A platform for developing robots and autonomous vehicles."),
    long_description=open("README.md").read(),
    url="https://github.com/erdos-project/erdos",
    packages=find_packages(exclude=("examples*", "tests*",)),
    license="Apache 2.0",
    install_requires=[
        "absl-py",
        "ray",
        "rospkg",
        "setproctitle",
    ],
)
