=== ISSUE #277 | Parallelize or map chunks of an MS and an image to compute chi2 gradient | labels:  | comments: [{"author":{"login":"sjperkins"},"authorAssociation":"MEMBER","body":"Hi @valegui. Thanks for the issue. I need some more time to think about what you're asking.\r\n\r\nInitially, I would say that the fact that the code is mapping every visibility to every pixel means that there is a many-to-many communication pattern which is very challenging for dask schedulers. This is probably what leads to the Out of Memory situations.\r\n\r\nOne way of \"solving\" this might be to assume that the image is small enough such that it can [cloned](https://docs.dask.org/en/stable/graph_manipulation.html#dask.graph_manipulation.clone) and fed into each visibility chunk.\r\n\r\nBut I'll try find some more time to think about your code in more detail.","createdAt":"2023-04-04T13:12:18Z","id":"IC_kwDOBh8_I85ZKl9Z","includesCreatedEdit":false,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/277#issuecomment-1495949145","viewerDidAuthor":false},{"author":{"login":"landmanbester"},"authorAssociation":"COLLABORATOR","body":"Computing the gradient (also the negative of the residual image) using a direct Fourier transform (DFT) is never going to be fast or memory efficient. Have you considered using a degridder (eg. the wgridder)? There are some utilities in codex-africanus that will do this for you, especially the residual function \r\n\r\nhttps://github.com/ratt-ru/codex-africanus/blob/master/africanus/gridding/wgridder/im2residim.py\r\n\r\nYou can see how to call it by looking at this test\r\n\r\nhttps://github.com/ratt-ru/codex-africanus/blob/7a756a9bd3fe8bfadb2a02add9b3f180be42696d/africanus/gridding/wgridder/tests/test_wgridder.py#L159\r\n\r\nBasically the idea is that, since the image is usually much smaller than the visibilities, you hold the full image in memory and chunk over the visibilities along row and chan axes (the latter needs to be aligned with the number of imaging bands you are using). You can get almost arbitrarily accurate (up to 1e-12) results with this approach as long as your pixels lie on a grid. If they do not, I would compute the residual visibilities and just apply the DFT to those. You can use the vis_to_im function (also in codex-africanus)\r\n\r\nhttps://github.com/ratt-ru/codex-africanus/blob/7a756a9bd3fe8bfadb2a02add9b3f180be42696d/africanus/dft/dask.py#L60\r\n\r\nfor that. All the for loops for this implementation are in the numba layer so you shouldn't be afraid of them. It is still going to be very slow compared to the gridded version ","createdAt":"2023-04-04T13:35:14Z","id":"IC_kwDOBh8_I85ZKvm2","includesCreatedEdit":false,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/277#issuecomment-1495988662","viewerDidAuthor":false},{"author":{"login":"valegui"},"authorAssociation":"NONE","body":"Hi @landmanbester , thank you for your answer. \r\nI believe your answer is not exactly what I need, as I'm looking to compute the gradient with respect to the image using every pixel. I do not want to apply gridding.\r\nThis is similar to what is done in the `vis_to_im` function. It's not exactly the same, but the code you suggested to use has served as a guide to continue looking at what can be done with `blockwise`.\r\nTo provide further context, I've already used a degridder to obtain the residual visibilities. The degridding is done using `map_blocks` over the uv array, with the entire image in memory. As you know, this does not cause memory nor time problems due to only indexing/using a minimal slice of the image for each uv pair. The problem starts in the `ms_gradient` function (in the op code) when uv is broadcasted, especially if the image has a number of pixels equal to or greater than 1024x1024.","createdAt":"2023-04-17T16:22:10Z","id":"IC_kwDOBh8_I85aGqZc","includesCreatedEdit":false,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/277#issuecomment-1511695964","viewerDidAuthor":false},{"author":{"login":"sjperkins"},"authorAssociation":"MEMBER","body":"> The degridding is done using `map_blocks` over the uv array, with the entire image in memory. As you know, this does not cause memory nor time problems due to only indexing/using a minimal slice of the image for each uv pair.\r\n\r\nYou've managed to solve the problem in the degridding step by passing the image in to each chunk, now we just need to apply the same idea in the other direction.\r\n\r\n>The problem starts in the `ms_gradient` function (in the op code) when uv is broadcasted, especially if the image has a number of pixels equal to or greater than 1024x1024.\r\n\r\nWhen broadcasting, far more memory is allocated than is strictly needed for computing the result. I've tried to re-express the general outline of your problem by:\r\n\r\n1. Minimising the amount of broadcasting in the dask layer\r\n2. And in the blockwise function layer by implementing it as a numba function with nested loops, rather than NumPy array broadcasting syntax.\r\n\r\nThe following keeps 16 cores busy adding 1's at a fairly stable 8GB of memory usage, but you'll probably want to make it do the actual gradient calculation. If you want to increase your image size, you may need to correspondingly decrease your row chunk size in order to keep the total amount of  used memory stable. Experimenting with chunk sizes is somewhat of an art.\r\n\r\n```python\r\nimport astropy.units as un\r\nimport dask.array as da\r\nimport numba\r\nimport numpy as np\r\n\r\n\r\nif __name__ == \"__main__\":\r\n    chunks = {\r\n        \"row\": (50,) * 2000,\r\n        \"chan\": (64,) * 8,\r\n        \"corr\": (4,),\r\n        \"uvw\": (3,),\r\n    }\r\n\r\n    shapes = {k: sum(v) for k, v in chunks.items()}\r\n    shape = lambda dims: tuple(shapes[d.strip()] for d in dims.split(\",\"))\r\n    chunk = lambda dims: tuple(chunks[d.strip()] for d in dims.split(\",\"))\r\n\r\n    uvw = da.random.random(shape(\"row,uvw\"), chunks=chunk(\"row,uvw\")) * 10_000\r\n    flag = da.random.choice([0, 1], shape(\"row,chan,corr\"), chunks=chunk(\"row,chan,corr\"))\r\n    model = (da.random.random(shape(\"row,chan,corr\"), chunks=chunk(\"row,chan,corr\")) +\r\n             da.random.random(shape(\"row,chan,corr\"), chunks=chunk(\"row,chan,corr\"))*1j)\r\n    observed = (da.random.random(shape(\"row,chan,corr\"), chunks=chunk(\"row,chan,corr\")) +\r\n                da.random.random(shape(\"row,chan,corr\"), chunks=chunk(\"row,chan,corr\"))*1j)\r\n\r\n    image = np.zeros((1024, 1024))\r\n\r\n    @numba.njit(nogil=True)\r\n    def block_gradient(uvw, model, observed, flag, image, deltax=None, deltay=None):\r\n        assert uvw.ndim == 2\r\n        assert flag.ndim == model.ndim == observed.ndim == 3\r\n        assert image.ndim == 2\r\n        nrow, nchan, ncorr = model.shape\r\n        nx, ny = image.shape\r\n\r\n        gradient = np.zeros((nrow, nx, ny))\r\n\r\n        for r in range(nrow):\r\n            u = uvw[r, 0]\r\n            v = uvw[r, 1]\r\n            w = uvw[r, 2]\r\n\r\n            for c in range(nchan):\r\n                for co in range(ncorr):\r\n                    residual = model[r, c, co] - observed[r, c, co]\r\n\r\n                    for yi in range(ny):\r\n                        y = deltay * (yi - ny // 2)\r\n                        for xi in range(nx):\r\n                            x = deltax * (xi - nx // 2)\r\n                            ux = u * x\r\n                            vy = v * y\r\n                            uv = ux + vy\r\n\r\n                            gradient[r, xi, yi] += 1\r\n\r\n        return gradient\r\n\r\n\r\n    grad = da.blockwise(block_gradient, (\"row\", \"x\", \"y\"),\r\n                        uvw, (\"row\", \"uvw\"),\r\n                        model, (\"row\", \"chan\", \"corr\"),\r\n                        observed, (\"row\", \"chan\", \"corr\"),\r\n                        flag, (\"row\", \"chan\", \"corr\"),\r\n                        image, (\"x\", \"y\"),\r\n                        deltax=(-1*un.rad).value,\r\n                        deltay=(-1*un.rad).value,\r\n                        concatenate=True,\r\n                        meta=np.empty((0,)*3, np.complex64))\r\n\r\n    grad.sum().compute()\r\n```\r\n\r\nI hope this bare outline captures your idea and is enough of a base for you to take the solution further.\r\n","createdAt":"2023-04-19T12:45:34Z","id":"IC_kwDOBh8_I85aSBD5","includesCreatedEdit":false,"isMinimized":false,"minimizedReason":"","reactionGroups":[],"url":"https://github.com/ratt-ru/dask-ms/issues/277#issuecomment-1514672377","viewerDidAuthor":false}] | created: 2023-04-01T00:38:34Z ===
- dask 2022.12.1
- dask-ms 0.2.15
- numba 0.56.4
- Python 3.10.9
- Ubuntu 18.04.6

Hi there, I'm working with Dask Array and numba to implement the gradient of $\chi²$ in a chunked/parallel way, but I'm having some memory related problems when working with a "big" image.
The $\chi²$ gradient equation is:
```math
[\nabla\chi^2]_i = \sum_{k=0}^{Z-1} w * \left( Re(V_k^R \cos(2\pi (x_i \cdot u_k + y_i \cdot v_k))) - Im(V_k^R \sin(2\pi (x_i \cdot u_k + y_i \cdot v_k))) \right)
```
, where $V_k^R$ is the $k$ -th residual visibility, $(x_i, y_i)$ is the image coordinate of the $i$ -th pixel according to the direction cosines of the phase tracking center, and $(u_k, v_k)$ is the sampling coordinate of the $k$ -th visibility.
The current version that works for images of size 256x256, uses `blockwise` over blocks of data that are aligned on the ROW coordinate. While the image (or its indices which are the ones really used) can be a dask array, it's treated as an in-memory numpy array, and the data from the MS is broadcasted to add the 2 dimensions of the image. That is why this version does not work for bigger images, like 2048x2048 or 1024x1024. While working in a jupyter notebook, the kernel just dies because it runs out of memory.
One of the problems faced to parallelize the proposed solution (with a chunked image) is that for every pixel/cell of the image array, we have to compute the gradient with every visibility, but the blocks of the data from the MS are not aligned to those of the image. This is a problem because we cannot use `map_blocks`or `blockwise` to work in every chunk of the image per every chunk of the data.
From the current state of the solution, I need to find a way to divide the load per chunks of the MS arrays but also per chunk of image, **ideally without falling back to the use of a for-loop** to iterate in every chunk of the image.

## What I have tried until now

Here's a simplified version of the strategy that has been working without problems for images of size 256x256. The steps are the following:

1. Broadcast and multiply the u/v arrays with the index array, so the first two dimensions are from the visibility and freq channel.
2. Use blockwise to map the calculation on every block.
3. Use a numba vectorized function for a faster and parallelized computation.

This works because the size of each block of the broadcasted array is small enough to fit in memory. Changing the size of the image with this version does not work due to memory limits  (the process gets killed).

```python
import astropy.units as un
import dask
import dask.array as da
import numpy as np
import xarray as xr
from dask.diagnostics import ProgressBar
from daskms import xds_from_ms
from numba import float32, guvectorize

def block_gradient(uv, w, vis):
    wvis_r = w[0] * vis[0].real
    wvis_i = w[0] * vis[0].imag
    dchi2_broad = dchi2_row_chan(uv, wvis_r, wvis_i)
    return dchi2_broad

@guvectorize(
    [(float32[:, :], float32[:], float32[:], float32[:, :])],
    '(n,m),(i),(i)->(n,m)',
    nopython=True,
)
def dchi2_row_chan(uv, wvis_r, wvis_i, res):
    uv_pi = 2 * np.pi * uv
    cos_uv = np.cos(uv_pi)
    sin_uv = np.sin(uv_pi)
    for i in range(wvis_r.shape[0]):
        res += wvis_r[i] * cos_uv - wvis_i[i] * sin_uv

def ms_gradient(ms, delta, imshape):
    uvw = ms.UVW.data.astype(np.float32)
    w = ms.WEIGHT.data
    flag = ms.FLAG.data
    vm = ms.MODEL.data
    vo = ms.DATA.data
    vr = vm - vo

    nchan = ms.DATA.shape[1]

    w_broadcast = da.repeat(w[:, np.newaxis, :], nchan, axis=1)
    w_broadcast *= ~flag

    uvw_broadcast = da.repeat(uvw[:, np.newaxis, :], nchan, axis=1)
    u = uvw_broadcast[:, :, 0]
    v = uvw_broadcast[:, :, 0]

    x_ind, y_ind = da.indices(imshape)

    x = (x_ind - imshape[0] // 2) * delta[0].to(un.rad).value
    y = (y_ind - imshape[1] // 2) * delta[1].to(un.rad).value

    ux = u[:, :, None, None] * x[None, None, :, :]
    vy = v[:, :, None, None] * y[None, None, :, :]
    uv = (ux + vy).astype(np.float32)

    # Equivalent to Re(Vr * exp(2j*pi*uv))
    data = da.blockwise(
        block_gradient,
        'ijnm',
        uv,
        'ijnm',
        w_broadcast,
        'ijk',
        vr,
        'ijk',
        dtype=np.float32,
    )

    res = da.sum(data, axis=[0, 1])
    return res


def dchi2(ms_list, imshape):
    dchi2 = da.zeros(imshape, dtype=np.float32)
    for ms in ms_list:
        dchi2 += ms_gradient(ms, [-1, 1]*un.rad, imshape)
    return dchi2

ms = "/home/datasets/ms0/ms0.ms"

data_xdsl = xds_from_ms(
        ms,
        group_cols=["FIELD_ID", "DATA_DESC_ID"],
        index_cols=["SCAN_NUMBER", "TIME", "ANTENNA1", "ANTENNA2"],
        columns=["UVW", "WEIGHT", "FLAG", "DATA"],
        chunks={'row': 1000}
    )

# Replace data with ones
# Create model with 0.5
data_xdsl = [
    xds.assign(
        {
            "DATA": (
                xds.DATA.dims,
                da.ones(
                    xds.DATA.data.shape,
                    dtype=np.complex64,
                    chunks=xds.DATA.data.chunksize
                )
            ),
            "MODEL": (
                xds.DATA.dims,
                da.ones(
                    xds.DATA.data.shape,
                    dtype=np.complex64,
                    chunks=xds.DATA.data.chunksize
                ) / 2
            )
        }
    ) for xds in data_xdsl
]

# Call the gradient function

dchi2_arr = dchi2(data_xdsl, (256, 256))

# Get results

with ProgressBar():
    result, *_ = da.compute(dchi2_arr, scheduler="threads", n_workers=20)

print(result)
```

## What do I  need

The expected result is to get the resulting image, without memory problems.
One option is to modify what has been working with smaller images, but as I mentioned before, the idea is to avoid a for-loop over the image/image-chunks, and I don't know how to do it without the loop.
Any suggestion or modification on the existing code is greatly appreciated.
