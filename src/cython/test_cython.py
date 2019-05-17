try:
    from cy_utils import cos
except ImportError:
    "cy_utils not found - have you run setup.py yet?"
import numpy as np

def test_cy_utils():
    """Make sure that the cython code matches a numpy implementation of the dot product, given two lists of inds and vals, where the inds are the indices of a sparse vector and the vals are the magnitudes."""
    a = np.arange(0,40).astype(np.int32)
    b = np.arange(5,70).astype(np.int32)
    c = np.arange(0,40).astype(np.float64)
    d = np.arange(5,70).astype(np.float64)
    data_store = np.zeros((2,200),type=np.int32)

    def cos_np(inds1,vals1,inds2,vals2):
        i1 = np.where(np.isin(inds1,inds2))
        i2 = np.where(np.isin(inds2,inds1))
        product = np.sum(vals1[i1]*vals2[i2])
        return product/np.linalg.norm(vals1)/np.linalg.norm(vals2)
    assert np.almost_equal(cos_np(a,c,b,d), cos(a,c,b,d,data_store))
    return




