import sqlite3

# Global variable: name of the database which we are connecting to
db_name = "group23.db"


def write_row(row, file_type="indicatoren"):
    """

    :param row: dictionary passed by reader function.
    :param file_type: the type of file that is being read in.
            Can be: "indicatoren" or TODO
    :return:
    """

    db = sqlite3.connect(db_name)
    crsr = db.cursor()

    if file_type == "indicatoren":
        # TODO: check with quinten in which way he passes the dict.
        brin_nr = row.get("Brinnr")
        comp_id = row.get("Vgl Groep")

        # Pull BrinNr to see if it exists
        results = crsr.execute("SELECT * FROM school Where brin_nr = ?",
                               brin_nr).fetchall()
        # If BrinNr does not exist, add school to db, otherwise school must
        # already been added
        if len(results) == 0:
            # TODO add the school to the db. Else continue.
            pass

        year_res = row.get("Jaarresultaat")
        dipl_res = row.get("Diplomaresultaat")
        dropout = row.get("Percentage nieuwe VSV-ers")

        # For each year insert data into table
        for years in year_res:
            # TODO if data exists, dont insert data
            cur_yres = year_res.get(years)
            cur_dipres = dipl_res.get(years)
            cur_dropres = dropout.get(years)
            # School data
            crsr.execute("INSERT INTO school_score (school_brin, school_year, "
                         "year_results, grad_perc, dropout_perc) VALUES "
                         "(?, ?, ?, ?, ?)", (brin_nr, years, cur_yres[0],
                                             cur_dipres[0], cur_dropres[0]))
            # Comp_data
            crsr.execute("INSERT INTO comparison_score (comp_group_id, "
                         "school_year, year_results, grad_perc, dropout_perc)"
                         "VALUES (?, ?, ?, ?, ?)",
                         (comp_id, years, cur_yres[1], cur_dipres[1],
                          cur_dropres[1]))

        db.commit()
        db.close()
        return True

    else:
        return False
