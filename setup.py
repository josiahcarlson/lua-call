
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import lua_call

setup(
    name="lua_call",
    version=lua_call.VERSION,
    author="Josiah Carlson",
    author_email="josiah.carlson@gmail.com",
    url="http://github.com/josiahcarlson/lua-call/",
    download_url="http://pypi.python.org/pypi/lua_call/",
    py_modules=["lua_call"],
    description="Call Lua scripts from other Lua scripts in Redis",
    long_description=lua_call.__doc__,
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Topic :: Software Development :: Libraries",
        "Topic :: Utilities",
    ]
)
