from setuptools import find_packages, setup

setup(
    name = "erdos",
    version = "0.1",
    packages = find_packages(exclude = ("examples*", "tests*",)),
    license = "Apache 2.0",
    url = "https://github.com/erdos-project/erdos",
    long_description = open("README.md").read(),
    install_requires = [
        "absl-py",
        "ray",
        "rospkg",
    ],
)

