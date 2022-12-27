import setuptools

setuptools.setup(
    name="anaconda.enterprise.server.sdk",
    version="0.2.0",
    package_dir={"": "src"},
    packages=setuptools.find_namespace_packages(where="src"),
    author="Joshua C. Burt",
    description="Anaconda Enterprise Server SDK",
    long_description="Anaconda Enterprise Server SDK",
    include_package_data=True,
    zip_safe=False,
)
