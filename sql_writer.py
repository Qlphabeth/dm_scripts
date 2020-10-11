import sqlite3

from indicatoren_VSV_parser import indicatoren_VSV_parser
from indicatoren_parser import indicatoren_parser


def write_data(filename, file_type="indicatoren", db_path="./db/ROCA12.db"):
    """

    :param data: a dictionary of data that has to be added.
    :param db_path: the path to the db.
    :param file_type: the type of file that is being read in.
            Can be: "indicatoren" or "job
    :return: integer corresponding to an error message.
    """

    db = sqlite3.connect(db_path)
    crsr = db.cursor()

    if len(crsr.execute("SELECT * FROM added_data_files WHERE "
                        "data_file_name = ?;", (filename,)).fetchall()) != 0:
        return 3
    else:
        crsr.execute("INSERT INTO added_data_files (data_file_name) VALUES "
                     "(?);", (filename,))

    # Create the filepath (files should be put in ./DATA/
    filepath = "./DATA/{}".format(filename)

    if file_type == "indicatoren":
        data = indicatoren_parser(filepath)
        for key in data:
            row = data[key]
            overwrite = False

            brin_nr = row.get("Brinnr")
            comp_id = row.get("Vgl Groep")

            # Pull brinnumber to see if it exists
            results = crsr.execute("SELECT * FROM school WHERE brinnumber = ?;",
                                   (brin_nr,)).fetchall()
            # If brinnumber does not exist, add school to db
            if len(results) == 0:
                crsr.execute("INSERT INTO school (brinnumber, `name`, "
                             "kind_of_instance, address, city, compID) "
                             "VALUES (?, ?, ?, ?, ?, ?);",
                             (brin_nr, row.get("Naam Instelling"),
                              row.get("Soort Instelling"), row.get("Adres"),
                              row.get("Plaats"), comp_id))

            # Pull comp_id
            results = crsr.execute("SELECT * FROM comparison_group WHERE "
                                   "compID = ?;", (comp_id,)).fetchall()
            # If comp_id does not exist, add school to db
            if len(results) == 0:
                crsr.execute("INSERT INTO comparison_group (compID) VALUES "
                             "(?);", (comp_id,))

            # Get the data dicts for year, diploma and dropout results
            year_res = row.get("Jaarresultaat")
            dipl_res = row.get("Diplomaresultaat")

            # Check what the highest year in the file is.
            high_year = max(year_res.get("Inst.").keys())

            # If the highest year in the file is not in the db, file must be
            # newer, so overwriting is allowed.
            results = crsr.execute("SELECT * FROM school_year WHERE "
                                   "school_year = ?", (high_year,)).fetchall()
            if len(results) == 0:
                overwrite = True

            # For each year insert data into table
            for years in year_res.get("Inst."):
                # School data
                results = crsr.execute("SELECT * FROM school_score WHERE "
                                       "brinnumber = ? and school_year = ?;",
                                       (brin_nr, years)).fetchall()
                if len(results) == 0:
                    cur_yres = year_res.get("Inst.").get(years)
                    cur_dipres = dipl_res.get("Inst.").get(years)
                    crsr.execute("INSERT INTO school_score (brinnumber, "
                                 "school_year, year_results, `graduation%`) "
                                 "VALUES (?, ?, ?, ?);",
                                 (brin_nr, years, cur_yres, cur_dipres))
                elif overwrite:
                    cur_yres = year_res.get("Inst.").get(years)
                    cur_dipres = dipl_res.get("Inst.").get(years)
                    crsr.execute("UPDATE school_score SET year_results = ?, "
                                 "`graduation%` = ? WHERE brinnumber = ? AND "
                                 "school_year = ?;", (cur_yres, cur_dipres,
                                                      brin_nr, years))

                # Comp_data
                results = crsr.execute("SELECT * FROM comparison_score WHERE "
                                       "compID = ? and school_year = ?;",
                                       (comp_id, years)).fetchall()

                # Only insert if it did not exist yet
                if len(results) == 0:
                    cur_yres = year_res.get("Vgl Grp").get(years)
                    cur_dipres = dipl_res.get("Vgl Grp").get(years)
                    crsr.execute("INSERT INTO comparison_score (compID, "
                                 "school_year, year_results, `graduation%`) "
                                 "VALUES (?, ?, ?, ?);",
                                 (comp_id, years, cur_yres, cur_dipres))
                elif overwrite:
                    cur_yres = year_res.get("Vgl Grp").get(years)
                    cur_dipres = dipl_res.get("Vgl Grp").get(years)
                    crsr.execute("UPDATE comparison_score SET "
                                 "year_results = ?, `graduation%` = ? WHERE "
                                 "compID = ? AND school_year = ?;",
                                 (cur_yres, cur_dipres, comp_id, years))

                # Pull year from db
                results = crsr.execute("SELECT * FROM school_year WHERE "
                                       "school_year = ?", (years,)).fetchall()
                # If year does not exist, add year to db
                if len(results) == 0:
                    crsr.execute("INSERT INTO school_year (school_year) "
                                 "VALUES (?)", (years,))

        data = indicatoren_VSV_parser(filepath)
        # IMPORTANT!!! Always run the vsv writer AFTER the indicatoren writer,
        # otherwise the rows that have to be altered are not yet created.
        for key in data:
            row = data[key]

            brin_nr = row.get("Brinnr")
            comp_id = row.get("Vgl Groep")

            perc_vsv = row.get("Percentage nieuwe VSV-ers")

            for years in perc_vsv.get("Inst."):
                # Check if rows that have to be altered exist.
                inst = crsr.execute("SELECT * FROM school_score WHERE "
                                    "brinnumber = ? AND school_year = ?;",
                                    (brin_nr, years)).fetchall()
                vgl = crsr.execute("SELECT `dropout%` FROM comparison_score "
                                   "WHERE compID = ? AND school_year = ?;",
                                   (comp_id, years)).fetchall()

                # Execute table alteration if needed.
                # TODO this is not correct yet for older files i think, fix overwriting
                if len(inst) == 1:
                    crsr.execute("UPDATE school_score SET `dropout%` = ? WHERE brinnumber = ? AND school_year = ?;", (perc_vsv.get("Inst.").get(years), brin_nr, years))

                if vgl == [(None,)]:
                    crsr.execute("UPDATE comparison_score SET `dropout%` = ? WHERE compID = ? AND school_year = ?;", (perc_vsv.get("Vgl Grp").get(years), comp_id, years))

        # db.commit()
        db.close()
        return 0

    elif file_type == "job":
        # TODO
        pass

    else:
        return 98
