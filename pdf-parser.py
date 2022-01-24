import pdfplumber
import pandas as pd
import sys

def extract_data_from_column(column, col_num):
    
    table_setting = {
        "vertical_strategy": "explicit", 
        "horizontal_strategy": "text",
        "explicit_vertical_lines": [ 
            42+180*col_num, 
            60+180*col_num, 
            70+180*col_num, 
            190+180*col_num, 
            218+180*col_num 
        ],
        "snap_tolerance": 0,
        "snap_x_tolerance": 0,
        "snap_y_tolerance": 0,
        "join_tolerance": 0,
        "join_x_tolerance": 0,
        "join_y_tolerance": 0,
        "edge_min_length": 0,
        "min_words_vertical": 0,
        "min_words_horizontal": 1,
        "keep_blank_chars": False,
        "text_tolerance": 3,
        "text_x_tolerance": 40,
        "text_y_tolerance": 3,
        "intersection_tolerance": 3,
        "intersection_x_tolerance": 3,
        "intersection_y_tolerance": 3,
    }
       
    # Extract the raw data from column
    data = (
        pd
        .DataFrame(
            column
            .extract_table(
                table_setting
            )
        )
        .rename(columns={0: "place", 1: "result", 2: "name", 3: "points"})
        .loc[
            lambda x: x["name"] != ""
        ]
        .assign(level = lambda df: df["name"].apply(lambda y: y.split()[-1]))
        .reset_index()
        [["place", "result", "name", "points", "level"]]
    )
    
    # Add additional relevant info
    added_info = []
    fight_round = 0
    fighter = None
    for i, r in data.iterrows():
        #print(r["name"], r["place"])
        if r["place"] != "":
            fight_round = 0
            fighter = " ".join(r["name"].split()[:-1])
            added_info_dict = {
                "placed_fighter": None,
                "fight_round": 0
            }
        else:
            added_info_dict = {
                "placed_fighter": fighter,
                "fight_round": fight_round
            }
        #print(fight_round)
        added_info.append(added_info_dict)
        fight_round += 1
    
    # Put them together and return final data
    final_data = (
        data
        .join(
            pd.DataFrame(added_info),
            how="left"
        )
    )
    return final_data

def get_pdf_data(pdf_path):
    pdf = pdfplumber.open(pdf_path)
    page_dfs = []
    for page in pdf.pages:
        columns = [ page.crop((40 + 180 * x, 75, 220 + 180 * x, 720)) for x in range(0,3) ]
        page_data = pd.concat([ extract_data_from_column(c, i) for i, c in enumerate(columns) ])
        page_dfs.append(page_data)    
    pdf_data = pd.concat(page_dfs)
    return pdf_data

if __name__ == "__main__":
    for arg in sys.argv[1:]:
        if arg[-4:].lower() == ".pdf":
            paths = [ arg ]
        else:
            paths = sorted(glob.glob(os.path.join(arg, "*.pdf")))

        for i, path in enumerate(paths):
            fname = path.rsplit("/", 1)[-1]
            sys.stderr.write("\n--- {} ---\n".format(fname))
            parsed_data = get_pdf_data(path)
            
            places = parsed_data.loc[
                lambda x: (x["place"] != "")
            ].copy()

            bouts = parsed_data.loc[
                lambda x: (x["place"] == "")
            ].copy()
            
            places[[
                "place", 
                "name", 
                "points", 
                "level"
            ]].to_csv("./data/" + fname[:-4] + "_results_places.csv", index=None)
            
            bouts[[
                "result", 
                "name", 
                "level", 
                "points", 
                "placed_fighter", 
                "fight_round"
            ]].to_csv("./data/" + fname[:-4] + "_results_bouts.csv", index=None)