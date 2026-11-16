"""Reproduce issue 347: per-group chunks ignored when group_cols="__row__"."""

import os
import tempfile

import numpy as np
import casacore.tables as ct

from daskms import xds_from_storage_table

spw_chan_freqs = (
    np.linspace(0.856e9, 2 * 0.856e9, 8),
    np.linspace(0.856e9, 2 * 0.856e9, 16),
    np.linspace(0.856e9, 2 * 0.856e9, 32),
)


def make_spw(dirname):
    pass
    fn = os.path.join(dirname, "SPECTRAL_WINDOW")
    q = """
    CREATE TABLE %s
    [NUM_CHAN I4,
     CHAN_FREQ R8 [NDIM=1]]
    LIMIT %d
    """ % (
        fn,
        len(spw_chan_freqs),
    )
    with ct.taql(q) as spw:
        spw.putvarcol(
            "NUM_CHAN", {"r%d" % i: s.shape[0] for i, s in enumerate(spw_chan_freqs)}
        )
        spw.putvarcol(
            "CHAN_FREQ", {"r%d" % i: s[None, :] for i, s in enumerate(spw_chan_freqs)}
        )
    return fn


def main():
    d = tempfile.mkdtemp()
    spw = make_spw(d)

    chunks = [
        {"__row__": 1, "chan": (4, 4)},
        {"__row__": 1, "chan": (4,) * 4},
        {"__row__": 1, "chan": (4,) * 8},
    ]

    try:
        xds = xds_from_storage_table(
            spw, group_cols="__row__", chunks=chunks, table_schema="MS"
        )
    except Exception as e:
        print(f"347 RAISED on construction: {type(e).__name__}: {e}")
        return

    print(f"347 n datasets = {len(xds)}")
    for i, ds in enumerate(xds):
        print(
            f"  dataset {i}: chan chunks = {ds.CHAN_FREQ.chunks}  (requested {chunks[i]['chan']})"
        )

    ok = all(
        tuple(ds.CHAN_FREQ.chunks[1]) == chunks[i]["chan"] for i, ds in enumerate(xds)
    )
    print("347 HONOURED_PER_GROUP_CHUNKS =", ok)


if __name__ == "__main__":
    main()
