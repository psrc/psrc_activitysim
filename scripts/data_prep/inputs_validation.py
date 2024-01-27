import pandera as pa
from pandera import Column, Check


households_out_schema = pa.DataFrameSchema(
    {
        "BLDGSZ": Column(int, Check.isin(range(-1, 11)), nullable=True), # FIXME: nullable?
        "HHID": Column(int, nullable=False),
        "HHT": Column(int, Check.isin(range(-1, 8)), nullable=True),
        "NOC": Column(int),
        "PERSONS": Column(int, Check(lambda s: s > 0)),
        "PUMA5": Column(int, nullable=False),
        "SERIALNO": Column(int, nullable=False),
        "TAZ": Column(int, nullable=False),
        "TENURE": Column(int, Check.isin(range(-1, 5))),
        "UNITTYPE": Column(int),
        "VEHICL": Column(int, Check.isin(range(-1, 7))),
        # "bucketBin"
        # "h004": Column(int, Check(lambda s: s >= 0)),
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
        "MAZ": Column(int, nullable=False, coerce=True),
        "is_mf": Column(int, Check.isin([0, 1]), coerce=True),
        # "hhagecat", "hfamily", "hwork_f", "hwork_p", "huniv", "hnwork", "hretire", "hpresch", "hschpred", "hschdriv",
        "originalPUMA": Column(int, nullable=False)
    }
)


persons_out_schema = pa.DataFrameSchema(
    {
        "PERID": Column(int, nullable=False),
        "PNUM": Column(int, nullable=False),
        "age": Column(int, Check(lambda s: s >= 0)),
        "household_id": Column(int, nullable=False),
        "pemploy": Column(int, Check.isin([1,2,3,4])),
        "pstudent": Column(int, Check.isin([1,2,3])),
        "ptype": Column(int, Check.isin(range(1,9))),
        "sex": Column(int, Check.isin([-1,1,2])),
        # "RELATE","ESR","GRADE","PAUG","DDP","WEEKS","HOURS","MSP","POVERTY","EARNS","pagecat","padkid"
    }
)

landuse_out_schema = pa.DataFrameSchema(
    columns={
        "AGE0004": Column(int, Check(lambda s: s >= 0)),
        "AGE0519": Column(int, Check(lambda s: s >= 0)),
        "AGE2044": Column(int, Check(lambda s: s >= 0)),
        "AGE4564": Column(int, Check(lambda s: s >= 0)),
        "AGE65P": Column(int, Check(lambda s: s >= 0)),
        "TOTACRE": Column(float, Check(lambda s: s >= 0.0)),
        "CIACRE": Column(float, Check(lambda s: s >= 0.0)),
        "RESACRE": Column(float, Check(lambda s: s >= 0.0)),
        "COLLFTE": Column(float, Check(lambda s: s >= 0.0)),
        "COLLPTE": Column(float, Check(lambda s: s >= 0.0)),
        "COUNTY": Column(int, Check.isin([1,2,3,4])),
        # "DISTRICT"
        "EMPRES": Column(int),
        "RETEMPN": Column(int, Check(lambda s: s >= 0)),
        "FOOEMPN": Column(int, Check(lambda s: s >= 0), coerce=True),
        "AGREMPN": Column(int, Check(lambda s: s >= 0)),
        "FPSEMPN": Column(int, Check(lambda s: s >= 0)),
        "HEREMPN": Column(int, Check(lambda s: s >= 0)),
        "MWTEMPN": Column(int, Check(lambda s: s >= 0)),
        "OTHEMPN": Column(int, Check(lambda s: s >= 0)),
        "HHINCQ1": Column(int, Check(lambda s: s >= 0)),
        "HHINCQ2": Column(int, Check(lambda s: s >= 0)),
        "HHINCQ3": Column(int, Check(lambda s: s >= 0)),
        "HHINCQ4": Column(int, Check(lambda s: s >= 0)),
        "HHPOP": Column(int, Check(lambda s: s >= 0)),
        "HSENROLL": Column(float, Check(lambda s: s >= 0.0)),
        # "MFDU"
        # "OPRKCST"
        "PRKCST": Column(float, Check(lambda s: s >= 0.0)),
        # "SD"
        # "SFDU"
        "SHPOP62P": Column(float, Check(lambda s: (s >= 0.0) & (s <= 1.0))),
        "TAZ": Column(int, nullable=False),
        "TERMINAL": Column(float, Check(lambda s: s >= 0.0)),
        # "TOPOLOGY"
        "TOTEMP": Column(int, Check(lambda s: s >= 0)),
        "TOTHH": Column(int, Check(lambda s: s >= 0)),
        "TOTPOP": Column(int, Check(lambda s: s >= 0)),
        "ZERO": Column(int, Check(lambda s: s == 0)),
        "area_type": Column(int, Check.isin([0, 1, 2, 3, 4, 5])),
        # "gqpop"
        # "hhlds"
        # "sftaz"
        "MAZ": Column(int, nullable=False, coerce=True),
        "GSENROLL": Column(int, Check(lambda s: s >= 0), coerce=True),
        "transit_score": Column(float),
        "transit_score_scaled": Column(float),
        "sfunits": Column(float, Check(lambda s: s >= 0.0)),
        "mfunits": Column(float, Check(lambda s: s >= 0.0)),
        "percent_mf": Column(float, Check(lambda s: (s >= 0.0) & (s <= 1.0))),
        "mixed_use2_1": Column(float),
        "density_index": Column(float),
        "buff_density_index": Column(float),
        "density": Column(float),
        "mixed_use3_1": Column(float),
        "hh_1": Column(float),
        "emptot_1": Column(float),
        "hh_2": Column(float),
        "emptot_2": Column(float),
        "access_dist_transit": Column(float)
    },
    checks=Check(lambda df: df['TOTEMP'] == df[["RETEMPN", "AGREMPN", "FPSEMPN", "HEREMPN", "MWTEMPN", "OTHEMPN", "FOOEMPN"]].\
                 sum(axis=1))
)
