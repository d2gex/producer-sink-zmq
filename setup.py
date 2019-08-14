import setuptools
import producer_sink


def get_long_desc():
    with open("README.rst", "r") as fh:
        return fh.read()


setuptools.setup(
    name="producer-sink-zmq",
    version=producer_sink.__version__,
    author="Dan G",
    author_email="daniel.garcia@d2garcia.com",
    description="A tiny library that implements the Asynchronous Producer-Sink communication pattern using "
                "ZeroMQ PUSH and PULL sockets",
    long_description=get_long_desc(),
    long_description_content_type="text/x-rst",
    url="https://github.com/d2gex/pushpull_zmq",
    packages=['producer_sink'],
    python_requires='>=3.6',
    install_requires=['pyzmq>=18.1.0'],
    tests_require=['pytest>=5.0.1', 'pymulproc>=0.1.1'],
    platforms='any',
    zip_safe=True,
    classifiers=[
            'Environment :: Console',
            'Environment :: Web Environment',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
