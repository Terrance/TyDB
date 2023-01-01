import os

from setuptools import find_packages, setup


ROOT = os.path.abspath(os.path.dirname(__file__))


with open(os.path.join(ROOT, "requirements.txt")) as f:
    install_requires = f.read().splitlines()


setup(
    name="TyDB",
    python_requires=">=3.7",
    install_requires=install_requires,
    platforms=["Any"],
    packages=find_packages(),
)
