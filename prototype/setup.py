from distutils.core import setup, Extension
from Cython.Distutils import build_ext
from Cython.Build import cythonize

import numpy
import os

try:
    os.remove("mfun.c")
except FileNotFoundError:
    pass
extension = Extension(
    name="mfun",
    sources = ["mfun.pyx"] #, ida_dir+"/ida.c",ida_dir+"/ida_band.c",ida_dir+"/ida_dense.c",ida_dir+"/ida_direct.c",ida_dir+"/ida_ic.c",ida_dir+"/ida_io.c",
    			#	ida_dir+"/nvector_serial.c",ida_dir+"/sundials_band.c",ida_dir+"/sundials_dense.c",ida_dir+"/sundials_direct.c",ida_dir+"/sundials_math.c",ida_dir+"/sundials_nvector.c"],
    # sources=["P2D_fd.pyx"],
    #library_dirs=['ida'],
    #include_dirs=[numpy.get_include(), ida_dir])
    )
setup(
    name='mfun',
    # cmdclass={'build_ext': build_ext},
    ext_modules=cythonize([extension])
)
