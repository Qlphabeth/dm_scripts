import sqlite3

# Global variable: name of the database which we are connecting to
db_name = "group23.db"


def write_row(row, file_type="indicatoren"):
    """

    :param row: dictionary passed by reader function.
    :param file_type: the type of file that is being read in.
            Can be: "indicatoren" or "VSV" or "job" or TODO Prob "internal"
    :return:
    """

    db = sqlite3.connect(db_name)
    crsr = db.cursor()

    if file_type == "indicatoren":
        brin_nr = row.get("Brinnr")
        comp_id = row.get("Vgl Groep")

        # Pull BrinNr to see if it exists
        results = crsr.execute("SELECT * FROM school Where brin_nr = ?",
                               brin_nr).fetchall()
        # If BrinNr does not exist, add school to db, otherwise school must
        # already been added
        if len(results) == 0:
            # TODO Check if the values in the row.get statements are correct!
            crsr.execute("INSERT INTO school (brin_nr, `name`, instance_type,"
                         "address, city, comp_id) VALUES (?, ?, ?, ?, ?)",
                         (brin_nr, row.get("Naam Instelling"),
                          row.get("Soort Instelling"), row.get("Adres"),
                          row.get("Plaats"), comp_id))

        # Get the data dicts for year, diploma and dropout results
        year_res = row.get("Jaarresultaat")
        dipl_res = row.get("Diplomaresultaat")
        # TODO dropouts should be added to the parser.
        dropout = row.get("Percentage nieuwe VSV-ers")

        # For each year insert data into table
        for years in year_res.get("Inst."):
            # TODO if data exists, dont insert data

            # School data
            cur_yres = year_res.get("Inst.").get(years)
            cur_dipres = dipl_res.get("Inst.").get(years)
            cur_dropres = dropout.get("Inst.").get(years)
            crsr.execute("INSERT INTO school_score (school_brin, school_year, "
                         "year_results, grad_perc, dropout_perc) VALUES "
                         "(?, ?, ?, ?, ?)", (brin_nr, years, cur_yres,
                                             cur_dipres, cur_dropres))

            # Comp_data
            cur_yres = year_res.get("Vgl Grp").get(years)
            cur_dipres = dipl_res.get("Vgl Grp").get(years)
            cur_dropres = dropout.get("Vgl Grp").get(years)
            crsr.execute("INSERT INTO comparison_score (comp_group_id, "
                         "school_year, year_results, grad_perc, dropout_perc)"
                         "VALUES (?, ?, ?, ?, ?)",
                         (comp_id, years, cur_yres, cur_dipres,
                          cur_dropres))

        db.commit()
        db.close()
        return True

    else:
        return False
