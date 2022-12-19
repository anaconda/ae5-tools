import setuptools

setuptools.setup(
    name="anaconda.enterprise.sdk",
    version="0.1.0",
    package_dir={"": "src"},
    packages=setuptools.find_namespace_packages(where="src"),
    author="Joshua C. Burt",
    description="Anaconda Enterprise SDK",
    long_description="Anaconda Enterprise SDK",
    include_package_data=True,
    zip_safe=False,
)
