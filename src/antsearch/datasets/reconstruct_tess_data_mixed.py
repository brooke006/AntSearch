import pandas as pd
import lightkurve as lk

# Grabs a CSV from TAP that grabs "toipfx" (star ID), "tfopwg_disp" (designation of object), "tid" (TESS object ID), and "pl_orbper" (Object orbital period) from the TESS Project Candidates database.
tap_url = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query=select+toipfx,tfopwg_disp,tid,pl_orbper+from+toi&format=csv"

raw_csv = pd.read_csv(tap_url)

# Only keep confirmed and known planets, and false positives and alerts, everything else is is not confirmed. Split into two separate dataframes to maintain a 50/50 ratio.
raw_csv_false = raw_csv[(raw_csv["tfopwg_disp"] == "FA") | (raw_csv["tfopwg_disp"] == "FP")]
raw_csv_false = raw_csv_false.drop_duplicates(subset = ["toipfx"])
raw_csv_false = raw_csv_false.head(100)

raw_csv = raw_csv[(raw_csv["tfopwg_disp"] == "CP") | (raw_csv["tfopwg_disp"] == "KP")]
raw_csv = raw_csv.drop_duplicates(subset = ["toipfx"])
raw_csv = raw_csv.head(100)

raw_csv = pd.concat([raw_csv, raw_csv_false], ignore_index = True)

pd.set_option("display.min_rows", 20)
print(raw_csv)

# Convert confirmed and known planets into "1", which is converted to True in a ".astype()" conversion.
raw_csv["tfopwg_disp"] = raw_csv["tfopwg_disp"].mask((raw_csv["tfopwg_disp"] == "CP") | (raw_csv["tfopwg_disp"] == "KP"), "1")

# Convert false positives and alerts into "\0", which is converted to False in a ".astype()" conversion.
raw_csv["tfopwg_disp"] = raw_csv["tfopwg_disp"].mask((raw_csv["tfopwg_disp"] == "FA") | (raw_csv["tfopwg_disp"] == "FP"), "\0")

raw_csv["tfopwg_disp"] = raw_csv["tfopwg_disp"].astype(bool)

def count_sectors(tic_id):
	return len(lk.search_lightcurve(f"TIC {tic_id}", mission = "TESS", author = "SPOC", exptime = 120))

raw_csv["tid"] = raw_csv["tid"].apply(count_sectors)

raw_csv = raw_csv.rename(columns = {"toipfx": "star_id", "tfopwg_disp": "has_planet", "tid": "num_sectors", "pl_orbper": "period"})

print(raw_csv)

csv_output_location = input("Please enter the directory (relative to this one) and name of\nthe file to save the CSV file to (do not enter extension): ")
raw_csv.to_csv(csv_output_location + ".csv", index = False)
