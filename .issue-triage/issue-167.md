=== ISSUE #167 | Transforming dataset to a gridded dataset | labels:  | comments: [{"author":{"login":"sjperkins"},"authorAssociation":"MEMBER","body":"Hi @miguelcarcamov. I'll be back in the office next week and will reply in more detail then. In the meantime, you may want to take a look at pfb-clean https://github.com/ratt-ru/pfb-clean/blob/master/pfb/operators/gridder.py. /cc @landmanbester ","createdAt":"2021-10-05T07:29:55Z","id":"IC_kwDOBh8_I843reNN","includesCreatedEdit":false,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/167#issuecomment-934142797","viewerDidAuthor":false},{"author":{"login":"landmanbester"},"authorAssociation":"COLLABORATOR","body":"I guess the short answer here is no. All the gridding actually happens inside the wgridder in [ducc0](https://gitlab.mpcdf.mpg.de/mtr/ducc) which does not expose the grids. From the user's perspective you have images going in and visibilities coming out. All w-corrections, padding, oversampling etc. is taken care of under the hood. If I understand correctly, what you are suggesting might not actually give you a smaller data product in the end because you will have to store a grid roughly the size of the image per baseline. Note that the gridder operator above is a bit outdated, I keep it there for legacy reasons. All gridding related operations now happen [here](https://github.com/ratt-ru/pfb-clean/tree/stokes_products/pfb/workers/grid) but I am not sure if that is useful to you because these workers really just do the data handling. There are dask wrappers for the wgridder in codex-africanus [here](https://github.com/ska-sa/codex-africanus/tree/master/africanus/gridding/wgridder) ","createdAt":"2021-10-05T09:05:28Z","id":"IC_kwDOBh8_I843rvru","includesCreatedEdit":false,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/167#issuecomment-934214382","viewerDidAuthor":false},{"author":{"login":"miguelcarcamov"},"authorAssociation":"NONE","body":"@landmanbester  I disagree with this - I mean, the size of the product will depend on the size of the grid and your pixel size (in Fourier space), but also on the size of your convolution kernels. The worst case would be to have a really big kernel and then... Yes, you can end up with a grid with the size of the image per baseline. Although if we are using dask, there is no much to be worried about that. To get to some point I have two questions:\r\n\r\n1) It is possible to loop baselines in a faster way? - Take into account that we are not using the gridding to use it in clean but also to reduce data so it can be used in other optimization algorithms - This algorithms might take non-gridded or gridded data. If not, I guess we might need to lose the baseline information on gridded data and recover it once degridding is done.\r\n\r\n2) Can I use joblib to loop the baselines? If dask data can be accessed in parallel, that would speed up things and I could end up doing the gridding on parallel per each baseline.\r\n\r\n3) As I said, if this is not possible then I will just drop the baseline info for gridded data and then recover if a degridding process is made.\r\n\r\nCheers, \r\n","createdAt":"2021-10-05T19:04:19Z","id":"IC_kwDOBh8_I843tj0Q","includesCreatedEdit":true,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/167#issuecomment-934690064","viewerDidAuthor":false},{"author":{"login":"sjperkins"},"authorAssociation":"MEMBER","body":" >     1. It is possible to loop baselines in a faster way? - Take into account that we are not using the gridding to use it in clean but also to reduce data so it can be used in other optimization algorithms - This algorithms might take non-gridded or gridded data. If not, I guess we might need to lose the baseline information on gridded data and recover it once degridding is done.\r\n\r\nMuch of the performance impact of handling each baseline's data separately is that data for each baseline is not contiguously ordered in a monotically increasing TIME ordered Measurement Set, which is the order that newer interferometers will almost certainly choose. Instead, each baseline's data is striped over the entire Measurement Set which leads to poor Disk I/O performance. If I recall correctly, WSClean performs a reordering step where data is separated out into separate baselines in order to avoid these issues. This reordering step should be efficient because the data can be read and written contiguously.\r\n\r\nThe other alternative, as @landmanbester points out is, for each dask chunk, to maintain grids for each baseline separately. Obviously, the dominant issues here are the size of the grid and the number of baselines. E.g. for single precision complex valued MeerKAT data: `8192 x 8192 x 2016 x 8 = ~1TB` per chunk of data and one would need at least 2 of those in memory (and probably a few more) to perform a parallel reduction.\r\n\r\nBut, I'm interested in your intention behind gridding -- it sounds as if you're gridding data with the intention of degridding it again to produce data with a higher time resolution. Is this the case? Perhaps you may be intending to average complex visibility data instead? We have some experience with this too and have an application called xova which performs this process:\r\n\r\n- https://github.com/ska-sa/xova\r\n- It wraps averaging routines from [codex-africanus](https://codex-africanus.readthedocs.io/en/latest/averaging-api.html)\r\n\r\n>     2. Can I use joblib to loop the baselines? If dask data can be accessed in parallel, that would speed up things and I could end up doing the gridding on parallel per each baseline.\r\n\r\nI haven't tried using joblib, but this should, in principle, be possible.\r\n \r\n>     3. As I said, if this is not possible then I will just drop the baseline info for gridded data and then recover if a degridding process is made.\r\n\r\nI think the answer to this question will lie in your response to my response in (1) :-)\r\n","createdAt":"2021-10-11T14:38:31Z","id":"IC_kwDOBh8_I844CKaI","includesCreatedEdit":false,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/167#issuecomment-940091016","viewerDidAuthor":false},{"author":{"login":"miguelcarcamov"},"authorAssociation":"NONE","body":"Hi @sjperkins , I have just come back from holidays.\r\n\r\n> Much of the performance impact of handling each baseline's data separately is that data for each baseline is not contiguously ordered in a monotically increasing TIME ordered Measurement Set, which is the order that newer interferometers will almost certainly choose. Instead, each baseline's data is striped over the entire Measurement Set which leads to poor Disk I/O performance. If I recall correctly, WSClean performs a reordering step where data is separated out into separate baselines in order to avoid these issues. This reordering step should be efficient because the data can be read and written contiguously.\r\n\r\nIs it possible to do this reordering using dask-ms? Maybe I can test doing this reordering and trying to check if the gridding per baseline goes faster....\r\n\r\n> The other alternative, as @landmanbester points out is, for each dask chunk, to maintain grids for each baseline separately. Obviously, the dominant issues here are the size of the grid and the number of baselines. E.g. for single precision complex valued MeerKAT data: 8192 x 8192 x 2016 x 8 = ~1TB per chunk of data and one would need at least 2 of those in memory (and probably a few more) to perform a parallel reduction.\r\n\r\nCorrect. Although this would decrease a bit since not all pixels in the grid will have non-zero values (some of them will be zero) - and that's what I am planning to store. So also depends on the convolution kernel and your pixel size in uv-space - I am trying to replicate the original dataset structure but with gridded data instead of ungridded data. Obviously those zero value data points in the grid won't be part of the gridded dataset.\r\n\r\n> But, I'm interested in your intention behind gridding -- it sounds as if you're gridding data with the intention of degridding it again to produce data with a higher time resolution.\r\n\r\nNot exactly... I am planning to grid to decrease the size of the original data - As I said the amount of this decrease depends on the convolution kernels and the pixel-size in uv-space. But let's say that we will end up with a smaller dataset. \r\n\r\nThis dataset then will pass through a reconstruction algorithm, which will give us the image/s and will add a model column to the dataset.\r\n\r\nThis dataset will still be gridded, but it will be possible to apply a degridding in order to obtain data points of the data columns (data and model) in the original (non-gridded) uv points.\r\n\r\nHope that this explains better what I am trying to do.\r\n\r\nCheers!","createdAt":"2021-11-03T18:29:17Z","id":"IC_kwDOBh8_I845NYVO","includesCreatedEdit":true,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/167#issuecomment-959808846","viewerDidAuthor":false}] | created: 2021-10-03T12:13:43Z ===
* dask-ms version:0.2.6
* Python version: 3.9.7
* Operating System: Manjaro LInux

### Description

Hello, is anyone working on transforming the dask-ms dataset to a gridded dataset? I am working on it using dask and the function bincount which works well. However, I would like to have the same structure as the original MS - or Dask-ms dataset. Say, less rows (due to the gridding) but per baseline and per spw.

Is not a problem looping the ms inside and ms_list. However, the problem is when trying to loop the baselines inside that ms_list (this takes a lot of computing time, because I guess each time that I do a compute(), the file is opened and read). I will share my code to see if you can give me a hand.
```
from .transformer import Transformer
from ..reconstruction.convolution import CKernel
from ..reconstruction import Image
from ..units import lambdas_equivalencies
from typing import List, Union
import astropy.units as un
import xarray as xr
import dask.array as da
from astropy.units import Quantity
from ..base import MS
import numpy as np
from abc import ABC


def bincount(weights, x):
    return np.bincount(x, weights)


def complex_bincount(weights, x):
    real_w = weights.real
    imag_w = weights.imag
    return np.bincount(x, real_w) + 1j * np.bincount(x, imag_w)


def calculate_pix(uvw, uvcellsize, imsize, ref_freq):
    m = imsize[0]
    n = imsize[1]
    duv = uvcellsize.to(un.m, equivalencies=lambdas_equivalencies(ref_freq))
    uv = uvw[:, 0:2] / duv
    uv = xr.apply_ufunc(lambda x: x.value, uv, dask="parallelized", output_dtypes=[uv.dtype]).round().astype(np.int32)
    u_pix = uv[:, 0] + m // 2
    v_pix = uv[:, 1] + n // 2
    idx = n * v_pix + u_pix
    return idx


class Gridding(Transformer, ABC):
    def __init__(self, image: Image = None, imsize: Union[List[int], int] = None, uvcellsize: Quantity = None,
                 ckernel_object: CKernel = None, **kwargs):
        """
        Class that represents the interferometric gridding
        :param imsize: Image size
        :param uvcellsize: Cell size in Fourier-space
        :param ckernel_object: Convolution Kernel
        :param kwargs: Transformer arguments
        """
        super().__init__(**kwargs)
        self.ckernel_object = ckernel_object
        self.image = image
        if image is None:
            if isinstance(imsize, int):
                self.imsize = [imsize, imsize]
            else:
                self.imsize = imsize

            if isinstance(uvcellsize, Quantity):
                if uvcellsize.shape[0] > 1:
                    self.uvcellsize = uvcellsize
                else:
                    self.uvcellsize = Quantity([uvcellsize, uvcellsize, uvcellsize])
            else:
                raise TypeError("uvcellsize must be a Quantity instance")
        else:
            self.imsize = image.imsize
            self.uvcellsize = image.cellsize.to(un.lambdas, equivalencies=lambdas_equivalencies()) / self.imsize

    def grid_weights(self, ms: MS = None):
        if ms is not None:

            ref_freq = ms.spectral_window.ref_frequency
            uvw = ms.visibilities.uvw
            weight = ms.visibilities.weight
            idx = calculate_pix(uvw, self.uvcellsize, self.imsize, ref_freq)
            bincount_m = idx.data.max().compute() + 1
            bin_count = da.apply_along_axis(bincount, 0, weight.data, idx.data, shape=(bincount_m,),
                                            dtype=weight.data.dtype)
            w_k = bin_count[idx]  # Gridded weight for each non-gridded u,v coordinate
            return idx, bin_count, w_k

        else:
            raise ValueError("MS cannot be Nonetype")

    # This does the convolutional gridding
    def transform(self) -> None:

        for ms in self.input_data.ms_list:
            ref_freq = ms.spectral_window.ref_frequency
            uvw = ms.visibilities.uvw
            weight = ms.visibilities.weight
            data = ms.visibilities.data
            flag = ms.visibilities.flag

            idx = calculate_pix(uvw, self.uvcellsize, self.imsize, ref_freq)
            ncorrs = ms.polarization.ncorrs
            nchans = ms.spectral_window.nchans
            # chans = ms.spectral_window.chans.compute()
            baselines = da.unique(ms.visibilities.baseline.data).compute()
            weight_list = []
            data_list = []
            for baseline_id in baselines:
                row_id = da.argwhere(ms.visibilities.baseline.data == baseline_id).squeeze().compute()
                idx_per_baseline = idx[row_id]
                weight_per_baseline = weight[row_id]
                flag_per_baseline = flag[row_id]  # (rows, nchans, ncorrs)
                data_per_baseline = data[row_id]
                bincount_m = idx_per_baseline.data.max().compute() + 1
                # Grid the weights (rows, ncorrs)
                bin_count_weights = da.apply_along_axis(bincount, 0, weight_per_baseline.data, idx_per_baseline.data,
                                                        shape=(bincount_m,),
                                                        dtype=weight_per_baseline.data.dtype)
                gridded_weights = bin_count_weights[idx_per_baseline]

                # Grid the complex data (rows, nchans, ncorrs) (w * data)
                weight_broadcast = da.tile(weight_per_baseline, nchans).reshape(
                    (len(weight_per_baseline), nchans, ncorrs))
                weight_broadcast = da.where(~flag_per_baseline, weight_broadcast, 0.0)
                visibility_data_per_baseline = weight_broadcast * data_per_baseline
                bin_count_visibilities = da.apply_along_axis(complex_bincount, 0, visibility_data_per_baseline.data,
                                                             idx_per_baseline.data,
                                                             shape=(bincount_m,),
                                                             dtype=visibility_data_per_baseline.data.dtype)

                bin_count_weight_broadcast = da.apply_along_axis(bincount, 0, weight_broadcast,
                                                             idx_per_baseline.data,
                                                             shape=(bincount_m,),
                                                             dtype=weight_broadcast.dtype)
                gridded_data = bin_count_visibilities[idx_per_baseline] / bin_count_weight_broadcast[idx_per_baseline]
                weight_list.append(gridded_weights)
                data_list.append(gridded_data)
            break # This break is here because we have just tested with one ms from the ms-list
```
