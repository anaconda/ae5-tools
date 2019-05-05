import setuptools
import versioneer

setuptools.setup(
    name="ae5-tools",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    url="https://github.com/mcg1969/ae5-tools",
    author="Anaconda, Inc.",
    description="A pluggable framework for AE5 administration CLI tools.",
    long_description="A pluggable framework for AE5 administration CLI tools.",
    packages=setuptools.find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points={
        "console_scripts": [
            'ae5=ae5_tools.cli.main:main',
        ]
    }
)
