from distutils.core import setup, Extension
from Cython.Distutils import build_ext
from Cython.Build import cythonize

import numpy
import os

try:
   os.remove("cy_utils.c")
except FileNotFoundError:
   pass
extension = Extension(
    name="cy_utils",
    sources = ["cy_utils.pyx"]
    )
setup(
    name='mfun',
    ext_modules=cythonize([extension])
)
