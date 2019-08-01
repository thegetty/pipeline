from setuptools import setup
import sys

if (sys.version_info[0:2] < (2,7)):
    install_requires =['ordereddict', 'future']
else:
    install_requires = []

setup(
    name = 'getty-pipeline',
    packages = ['pipeline'],
    test_suite="tests",
    version = '0.0.1',
    description = 'Getty CIDOC-CRM Pipeline',
    author = 'Rob Sanderson',
    author_email = 'rsanderson@getty.edu',
    url = 'https://github.com/thegetty/pipeline',
    install_requires=install_requires,
    classifiers = [
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 2",
        "License :: OSI Approved :: Apache Software License",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ]
)
