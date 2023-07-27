import pandera as pa
from pandera import check_output, Column, Check

households_out_schema = pa.DataFrameSchema(
    {
        "BLDGSZ": Column(int, Check.isin(range(-1, 10))),
        "HHID": Column(str, nullable=False),
        "HHT": Column(int, Check.isin(range(-1, 8))),
        "NOC": Column(int, Check(lambda s: s >= 0)),
        "PERSONS": Column(int, Check(lambda s: s > 0)),
        "PUMA5": Column(int, nullable=False),
        "SERIALNO": Column(int, nullable=False),
        "TAZ": Column(int, nullable=False),
        "TENURE": Column(int, Check.isin(range(-1, 5))),
        "UNITTYPE": Column(int),
        "VEHICL": Column(int, Check.isin(range(-1, 7))),
        # "bucketBin"
        "h0004": Column(int, Check(lambda s: s >= 0)),
        "h0511": Column(int, Check(lambda s: s >= 0)),
        "h1215": Column(int, Check(lambda s: s >= 0)),
        "h1617": Column(int, Check(lambda s: s >= 0)),
        "h1824": Column(int, Check(lambda s: s >= 0)),
        "h2534": Column(int, Check(lambda s: s >= 0)),
        "h3549": Column(int, Check(lambda s: s >= 0)),
        "h5064": Column(int, Check(lambda s: s >= 0)),
        "h6579": Column(int, Check(lambda s: s >= 0)),
        "h80up": Column(int, Check(lambda s: s >= 0)),
        "hNOCcat": Column(int, Check.isin([0, 1])),
        "hadkids": Column(int),
        "hadnwst": Column(int, Check.isin([0, 1])),
        "hadwpst": Column(int, Check.isin([0, 1])),
        "hinccat1": Column(int, Check.isin(range(1, 5))),
        "hinccat2": Column(int, Check.isin(range(1, 10))),
        "hmultiunit": Column(int, Check.isin([0, 1])),
        "hownrent": Column(int, Check.isin([0, 1, 2])),  # 1 - own; 2 - rent, 0 = ?
        "hsizecat": Column(int, Check.isin([1, 2, 3, 4])),
        "htypdwel": Column(int),
        "hunittype": Column(int),
        "hwrkrcat": Column(int, Check.isin(range(-1, 4))),
        "income": Column(int, Check(lambda s: s >= 0)),
        "workers": Column(int, Check(lambda s: s >= 0)),
        "MAZ": Column(int, nullable=False),
        "is_mf": Column(int, Check.isin([0, 1])),
        # "hhagecat", "hfamily", "hwork_f", "hwork_p", "huniv", "hnwork", "hretire", "hpresch", "hschpred", "hschdriv",
        "originalPUMA": Column(int, nullable=False)
    }
)
# @check_output(households_out_schema)