# Calibrate auto ownership model using drivers/vehicles
# 1. Automatically calculate new coefficient targets based on ACS versus model
# 2. Update coefficient file with new value
# 3. Run Activitysim with updated coefficient
# 4. Compare, create new coefficients and re-run as needed
import os, sys
import toml
import numpy as np
import pandas as pd
import calibration_data_input

config = toml.load(
    os.path.join(
        os.getcwd(), "scripts", "calibration", "calibration_configuration.toml"
    )
)


def compute_distribution(df):
    df_tot = df.groupby("num_drivers").sum()[["hh_weight"]].reset_index()
    df_tot.rename(columns={"hh_weight": "totals_by_drivers"}, inplace=True)
    df = df.merge(df_tot, on="num_drivers", how="left")
    df["share_by_drivers"] = df["hh_weight"] / df["totals_by_drivers"]

    return df


# Load ACS data
# FIXME: Refine the process to generate this file. Used RSG's scripts at \\modelstation2\c$\Workspace\2023_calibration\RSG_tools\calibration_setup\Auto Ownership\
df = pd.read_csv(config["acs_data"])
df = df[
    (df["1_year_source"] == int(config["acs_1_year_source"]))
    & (df["5_year_source"] == str(config["acs_5_year_source"]))
]
df.rename(
    columns={
        "potential_drivers": "num_drivers",
        "vehicles": "auto_ownership",
        "households": "hh_weight",
    },
    inplace=True,
)
df["source"] = "ACS"

# Load model data and calculate distribution of drivers and vehicles
hh_data = calibration_data_input.get_households_data(
    [
        "home_zone_id",
        "auto_ownership",
        "hhsize",
        "num_workers",
        "num_adults",
        "num_drivers",
        "income",
    ]
)
hh_data = hh_data[hh_data["source"] == "model results"]
for col in ["auto_ownership", "num_drivers"]:
    hh_data.loc[hh_data[col] >= 4, col] = 4
model_df = (
    hh_data.groupby(["auto_ownership", "num_drivers"]).sum()["hh_weight"].reset_index()
)

model_df["source"] = "model"

survey_df = df[["num_drivers", "auto_ownership", "hh_weight", "source"]]

# Drop case where # drivers (people ages 16+) equal to zero
survey_df = survey_df[survey_df["num_drivers"] > 0]
model_df = model_df[model_df["num_drivers"] > 0]

# Scale the survey data by the Activitysim output to get distributions for comparison
survey_tot_df = survey_df.groupby("num_drivers").sum()[["hh_weight"]]
model_tot_df = model_df.groupby("num_drivers").sum()[["hh_weight"]]

for driver_count in [1, 2, 3, 4]:
    # Scale survey data to activitysim totals by # drivers columns
    scale_factor = (
        model_tot_df.loc[driver_count] / survey_tot_df.loc[driver_count]
    ).values[0]
    _filter = survey_df["num_drivers"] == driver_count
    survey_df.loc[_filter, "hh_weight"] = (
        survey_df.loc[_filter, "hh_weight"] * scale_factor
    )

# Compute distribution of vehicles for each categories of drivers
survey_df = compute_distribution(survey_df)
model_df = compute_distribution(model_df)

# Survey weight totals across vehicle categories should be the same
survey_tot_df = survey_df.groupby("num_drivers").sum()[["hh_weight"]]
model_tot_df = model_df.groupby("num_drivers").sum()[["hh_weight"]]
assert survey_tot_df["hh_weight"].sum().astype("int") == model_tot_df[
    "hh_weight"
].sum().astype("int")

# Compare the distributions
col_list = ["auto_ownership", "num_drivers", "share_by_drivers"]
df = model_df[col_list].merge(
    survey_df[col_list],
    on=["auto_ownership", "num_drivers"],
    suffixes=["_model", "_survey"],
    how="left",
)

# Calculate the ratio of differences bewteen survey and model
df["ratio"] = df["share_by_drivers_survey"] / df["share_by_drivers_model"]

# Create a column for the base case of each scenario
for veh_count in [1, 2, 3, 4]:
    base_ratio = df.loc[
        (df["auto_ownership"] == veh_count) & (df["num_drivers"] == veh_count), "ratio"
    ].values[0]
    df.loc[df["auto_ownership"] == veh_count, "base_ratio"] = base_ratio

df = df[df["auto_ownership"] > 0]

# Calculate adjustment factor
# (LN(ratio-base_case_ratio))*dampening factor
# FIXME: should be able to adjust damp factor for each coefficient
df["adjustment"] = (
    np.log(df["ratio"]) - np.log(df["base_ratio"]) * config["damp_factor"]
)

# Load coefficients file
df_coef = pd.read_csv(
    os.path.join(config["configs_dir"], "auto_ownership_coefficients.csv")
)

# Load expression file for model to get variable names
df_expr = pd.read_csv(os.path.join(config["configs_dir"], "auto_ownership.csv"))

# Make sure df is sorted by auto_ownership and num_drivers categories
df = df.sort_values(["auto_ownership", "num_drivers"])

for veh_cat in ["2", "3", "4_up"]:
    _df = df_expr[df_expr["Label"] == "util_drivers_" + str(veh_cat)][
        ["cars1", "cars2", "cars3", "cars4"]
    ].T
    veh_count = int(veh_cat.split("_")[0])
    df.loc[df["num_drivers"] == veh_count, "coefficient_name"] = _df[
        veh_count - 1
    ].values

_df = df_expr[df_expr["Label"] == "util_drivers_calibration_only"][
    ["cars1", "cars2", "cars3", "cars4"]
].T
df.loc[df["num_drivers"] == 1, "coefficient_name"] = _df[0].values

# Merge to get coefficient values
df = df.merge(df_coef, on="coefficient_name", how="right")

# TODO: get coefficient file and re-write to file
df["new_coeff"] = df["value"] + df["adjustment"]
df["new_coeff"] = df["new_coeff"].fillna(df["value"])

# Write updated coefficient to file
df.drop("value", axis=1, inplace=True)
df.rename(columns={"new_coeff": "value"}, inplace=True)
df[["coefficient_name", "value", "constrain"]].to_csv(
    config["output_dir"] + "/auto_ownership_coefficients.csv"
)
