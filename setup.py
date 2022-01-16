from setuptools import setup
from setuptools import find_packages


def load(path):
    return open(path, 'r').read()


numerbay_version = '0.1.4'


classifiers = [
    "Development Status :: 3 - Alpha",
    "Environment :: Console",
    "Intended Audience :: Science/Research",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Topic :: Scientific/Engineering"]


if __name__ == "__main__":
    setup(
        name="numerbay",
        version=numerbay_version,
        maintainer="numerbay",
        maintainer_email="admin@numerbay.ai",
        description="Programmatic interaction with numerbay.ai - the Numerai community marketplace",
        long_description=load('README.md'),
        long_description_content_type='text/markdown',
        url='https://github.com/councilofelders/numerbay-api',
        project_urls={
            'Documentation': 'https://docs.numerbay.ai/',
        },
        platforms="OS Independent",
        classifiers=classifiers,
        license='MIT License',
        package_data={'numerai': ['LICENSE', 'README.md']},
        packages=find_packages(exclude=['tests']),
        install_requires=["requests", "pytz", "python-dateutil",
                          "tqdm>=4.29.1", "click>=7.0", "pandas>=1.1.0"],
        entry_points={
          'console_scripts': [
              'numerbay = numerbay.cli:cli'
          ]
          },
        )
