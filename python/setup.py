import os

from setuptools import setup
from setuptools_rust import Binding, RustExtension

package_dir = os.path.dirname(__file__)

# Put all package dependencies here
requires = []

setup(
    name="erdos",
    version="0.0.1",
    rust_extensions=[
        RustExtension("erdos.internal",
                      path="Cargo.toml",
                      debug=False,
                      features=["python"],
                      binding=Binding.PyO3)
    ],
    package_dir={"": package_dir},
    # rust extensions are not zip safe, just like C-extensions.
    zip_safe=False,
    install_requires=requires
)
