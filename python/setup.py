import os

from setuptools import setup, find_packages
from setuptools_rust import Binding, RustExtension

package_dir = os.path.dirname(__file__)

# Put all package dependencies here
requires = ["fire", "numpy"]

setup(
    name="erdos",
    version="0.2.0",
    rust_extensions=[
        RustExtension("erdos.internal",
                      path="Cargo.toml",
                      features=["python"],
                      binding=Binding.PyO3)
    ],
    packages=find_packages("python"),
    package_dir={"": package_dir},
    # rust extensions are not zip safe, just like C-extensions.
    zip_safe=False,
    install_requires=requires)
