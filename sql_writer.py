import sqlite3

from JOB_parser_v2 import JOB_parser
from indicatoren_VSV_parser import indicatoren_VSV_parser
from indicatoren_parser import indicatoren_parser
from studenten_aantallen_parser import aantallen_parser
from datetime import datetime


def write_data(filename, file_type="indicatoren", db_path="./db/ROCA12.db"):
    """

    :param data: a dictionary of data that has to be added.
    :param db_path: the path to the db.
    :param file_type: the type of file that is being read in.
            Can be: "indicatoren" or "job" or "studentenaantallen"
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

        db.commit()
        db.close()
        return 0

    elif file_type == "job":
        data = JOB_parser(filepath)

        # HARDCODED, I don't know how to pull this from the file
        year = 2020
        brinnr = "25PM"

        # Check latest studentID in db
        latest_nr = db.execute("SELECT studentID FROM grading_student "
                               "ORDER BY studentID DESC LIMIT 1;").fetchall()

        # If no studentIDs, start at 0
        if len(latest_nr) == 0:
            latest_nr = [0]
        latest_nr = latest_nr[0]

        for rows in data:
            row = data[rows]
            latest_nr += 1

            # Check if location exists
            result = db.execute("SELECT * FROM location WHERE locationID = ?;",
                                (row.get("Locatie"),)).fetchall()
            # If not exists, add
            if len(result) == 0:
                address = row.get("Locatie").split("; ")
                db.execute("INSERT INTO location (locationID, city, address, "
                           "brinnumber) VALUES (?, ?, ?, ?);",
                           (row.get("Locatie"),
                            address[0],
                            address[1],
                            brinnr))

            # Check if teaching team exists
            result = db.execute("SELECT * FROM teaching_team WHERE "
                                "locationID = ? AND team_name = ?;",
                                (row.get("Locatie"),
                                 row.get("Team"))).fetchall()
            # If not exists, add
            if len(result) == 0:
                db.execute("INSERT INTO teaching_team (locationID, team_name) "
                           "VALUES (?, ?);", (row.get("Locatie"),
                                              row.get("Team")))

            # Check if course exists in db
            course = db.execute("SELECT * FROM course WHERE courseID = ? "
                                "AND course_level = ? AND team_name = ? "
                                "AND locationID = ?;",
                                (row.get("CREBO"),
                                 row.get("Niveau"),
                                 row.get("Team"),
                                 row.get("Locatie"))).fetchall()

            # If does not exist, add to db
            if len(course) == 0:
                db.execute("INSERT INTO course (courseID, course_level, "
                           "course_name, study_domain, team_name, "
                           "locationID) VALUES (?, ?, ?, ?, ?, ?);",
                           (row.get("CREBO"),
                            row.get("Niveau"),
                            row.get("Crebonaam"),
                            row.get("Domein"),
                            row.get("Team"),
                            row.get("Locatie")))
            # elif, check for domain
            elif len(course) == 1:
                domain = db.execute("SELECT study_domain FROM course WHERE "
                                    "courseID = ? AND course_level = ? "
                                    "AND team_name = ? AND locationID = ?;",
                                    (row.get("CREBO"),
                                     row.get("Niveau"),
                                     row.get("Team"),
                                     row.get("Locatie"))).fetchall()

                # Set domain if it was not set before
                if len(domain) == 0:
                    db.execute("UPDATE course SET study_domain = ? WHERE "
                               "courseID = ? AND course_level = ? "
                               "AND team_name = ? AND locationID = ?;",
                               (row.get("Domein"),
                                row.get("CREBO"),
                                row.get("Niveau"),
                                row.get("Team"),
                                row.get("Locatie")))

            # Add data to grading_student table
            db.execute("INSERT INTO grading_student (studentID, calender_year,"
                       " sex, age_group, learning_path, class, weight_factor, "
                       "courseID, course_level, team_name, locationID) VALUES "
                       "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                       (latest_nr, year,
                        row.get("Geslacht"),
                        row.get("Leeftijd"),
                        row.get("Leerweg"),
                        row.get("Klas"),
                        row.get("weegfac1"),
                        row.get("CREBO"),
                        row.get("Niveau"),
                        row.get("Team"),
                        row.get("Locatie")))

            # Add data to lessen table
            db.execute("INSERT INTO lessen (Lessen_01, Lessen_02, Lessen_03,"
                       "studentID, calender_year) VALUES (?, ?, ?, ?, ?);",
                       (row.get("Wat vind je van de lessen Nederlands?"),
                        row.get("Wat vind je van de lessen rekenen?"),
                        row.get("Wat vind je van de lessen Engels?"),
                        latest_nr, year))

            # Add data to onderwijs_en_begeleiding table
            db.execute("INSERT INTO onderwijs_en_begeleiding "
                       "(onderwijs_en_begeleiding_01, "
                       "onderwijs_en_begeleiding_02, "
                       "onderwijs_en_begeleiding_03, "
                       "onderwijs_en_begeleiding_04, "
                       "onderwijs_en_begeleiding_05, "
                       "onderwijs_en_begeleiding_06, studentID, calender_year)"
                       " VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
                       (row.get("Wat vind je van de manier waarop school "
                                "je helpt om goed te presteren?"),
                        row.get("Wat vind je van hoe jouw docenten lesgeven?"),
                        row.get("Wat vind je van de begeleiding tijdens je "
                                "opleiding?"),
                        row.get("Wat vind je van hoe je school luistert naar "
                                "jouw mening?"),
                        row.get("Wat vind je van het nut van de lessen voor"
                                " jouw toekomst?"),
                        row.get("Na de opleiding moet je kiezen: verder leren "
                                "of gaan werken. Hoe helpt je school je bij "
                                "deze keuze?"), latest_nr, year))

            # Add data to informatie table
            db.execute("INSERT INTO informatie (informatie_01, informatie_02, "
                       "informatie_03, informatie_04, informatie_05, "
                       "studentID, calender_year) VALUES "
                       "(?, ?, ?, ?, ?, ?, ?);",
                       (row.get("Wat vind je van hoe school je op de hoogte "
                                "houdt over hoe het met je gaat in je "
                                "studie?"),
                        row.get("Wat vind je van de informatie die school je"
                                " geeft over je rechten en plichten?"),
                        row.get("Wat vind je van hoe school omgaat "
                                "met klachten?"),
                        row.get("Vragen de docenten wat je van de lessen"
                                " vindt?"),
                        row.get("Wat vind je van de informatie die je kreeg "
                                "over de hoogte van de schoolkosten?"),
                        latest_nr, year))

            # Add data to omgeving_sfeer_en_veiligheid table
            db.execute("INSERT INTO `omgeving,_sfeer_en_veiligheid` "
                       "(omgeving_sfeer_en_veiligheid_01, "
                       "omgeving_sfeer_en_veiligheid_02, "
                       "omgeving_sfeer_en_veiligheid_03, "
                       "omgeving_sfeer_en_veiligheid_04, "
                       "omgeving_sfeer_en_veiligheid_05, studentID, "
                       "calender_year) VALUES (?, ?, ?, ?, ?, ?, ?);",
                       (row.get("Wat vind je van de sfeer binnen jouw "
                                "opleiding?,"),
                        row.get("Ga je met plezier naar school?"),
                        row.get("Voel je je veilig op school?"),
                        row.get("Wat vind je van de werkplekken op school "
                                "waar je rustig kunt studeren?"),
                        row.get("Wat vind je van de kantine op school?"),
                        latest_nr, year))

            # Add data to lesmateriaal_en_boeken table
            db.execute("INSERT INTO lesmateriaal_en_toetsen ("
                       "lesmateriaal_en_toetsen_01, "
                       "lesmateriaal_en_toetsen_02, "
                       "lesmateriaal_en_toetsen_03, "
                       "lesmateriaal_en_toetsen_04,"
                       "lesmateriaal_en_toetsen_05, studentID, calender_year) "
                       "VALUES (?, ?, ?, ?, ?, ?, ?)",
                       (row.get("Wat vind je van het lesmateriaal en "
                                "de boeken?"),
                        row.get("Je hebt boeken en lesmaterialen moeten kopen "
                                "voor school. Gebruik je deze boeken en "
                                "lesmaterialen?"),
                        row.get("Wat vind je van hoe goed toetsen aansluiten "
                                "bij wat je hebt geleerd?"),
                        row.get("Wat vind je van de keuzedelen waaruit je "
                                "kunt kiezen?"),
                        row.get("Wat vind je van de inhoud van de "
                                "keuzedelen?"),
                        latest_nr, year))

            # Add data to bvp/stage(bol_studenten) table
            db.execute("INSERT INTO `bvp/stage(bol_studenten)` (stage_bol_01, "
                       "stage_bol_02, stage_bol_school_01, "
                       "stage_bol_school_02, stage_bol_school_03, "
                       "stage_bol_leerbedrijf_01, stage_bol_leerbedrijf_02, "
                       "studentID, calender_year) VALUES "
                       "(?, ?, ?, ?, ?, ?, ?, ?, ?);",
                       (row.get("Heb je stage/bpv gelopen of doe je dat op "
                                "dit moment?"),
                        row.get("Was/Is het moeilijk voor je om een stage-/"
                                "bpv-plaats te vinden?"),
                        row.get("Wat vind je van hoe school jou heeft "
                                "voorbereid op je stage/bpv?"),
                        row.get("Wat vind je van hoe school je begeleidt/heeft"
                                " begeleid tijdens je stage/bpv?"),
                        row.get("Wat vind je van hoe opdrachten van school "
                                "aansluiten bij het werk dat je doet op je "
                                "stage-/bpv-plek?"),
                        row.get("Wat vind je van hoe het leerbedrijf je "
                                "begeleidt/heeft begeleid tijdens je "
                                "stage/bpv?"),
                        row.get("Wat vind je van wat je leert op je "
                                "stage/bpv?"),
                        latest_nr, year))

            # Add data to werkplek(bbl_studenten) table
            db.execute("INSERT INTO `werkplek(bbl_studenten)` "
                       "(werkplek_bbl_school_01, werkplek_bbl_school_02, "
                       "werkplek_bbl_school_03, werkplek_bbl_school_04, "
                       "werkplek_bbl_school_05, studentID, calender_year) "
                       "VALUES (?, ?, ?, ?, ?, ?, ?);",
                       (row.get("Wat vind je van de mogelijkheid om jouw "
                                "werkervaring te bespreken op school?"),
                        row.get("Wat vind je van hoe opdrachten van school "
                                "aansluiten bij het werk dat je doet op je "
                                "werkplek?"),
                        row.get("Hoe moeilijk was het voor jou om een werkplek"
                                " te vinden voor je opleiding?"),
                        row.get("Wat vind je van de manier waarop het "
                                "leerbedrijf je begeleidt?"),
                        row.get("Wat vind je van wat je leert op je "
                                "werkplek?"), latest_nr, year))

            # Add data to studeren_met_een_beperking table
            db.execute("INSERT INTO studeren_met_een_beperking (beperking_01, "
                       "beperking_02, beperking_03, studentID, calender_year)"
                       " VALUES (?, ?, ?, ?, ?);",
                       (row.get("Heb je leerproblemen, beperkingen "
                                "of blijvende ziektes?"),
                        row.get("Weten jouw docenten dat "
                                "je een beperking hebt??"),
                        row.get("Wat vind je van de hulpmiddelen en "
                                "aanpassingen op school voor jouw beperking?"),
                        latest_nr, year))

            # Add data to medezeggenschap table
            db.execute("INSERT INTO medezeggenschap (medezeggenschap_01, "
                       "medezeggenschap_02, studentID, calender_year) "
                       "VALUES (?, ?, ?, ?);",
                       (row.get("Is er een studentenraad op jouw school?"),
                        row.get("Zou je zelf willen meepraten over hoe het op"
                                " jouw school beter kan?"),
                        latest_nr, year))

            # Add data to eindoordeel/algemene_tevredenheid table
            db.execute("INSERT INTO `eindoordeel/algemene_tevredenheid` "
                       "(cijfer_opleiding, cijfer_school, studentID, "
                       "calender_year) VALUES (?, ?, ?, ?);",
                       (row.get("Welk rapportcijfer geef je jouw opleiding?"),
                        row.get("Welk rapportcijfer geef je jouw school?"),
                        latest_nr, year))

            # Add data to onderwijs_en_begeleiding(eigen_vragen_ROC_A12) table
            # TODO 8th question is missing
            db.execute("INSERT INTO "
                       "`onderwijs_en_begeleiding(eigen_vragen_ROC_A12)` "
                       "(`25PM_01`, `25PM_02`, `25PM_03`, `25PM_04`, "
                       "`25PM_05`, `25PM_06`, `25PM_07`, studentID, "
                       "calender_year) "
                       "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
                       (row.get("Wat vind je van de afwisseling in de manieren"
                                " waarop je les krijgt?"),
                        row.get("Wat vind je van de uitdaging in de lessen?"),
                        row.get("Wat vind je van het aantal uitgevallen "
                                "lessen?"),
                        row.get("Wat vind je van de mogelijkheid om in je "
                                "eigen tempo te leren?"),
                        row.get("Wat vind je van de begeleiding door de "
                                "studieloopbaanbegeleider?"),
                        row.get("Weet je waarom je bepaalde dingen moet "
                                "leren?"),
                        row.get("Wat vind je van de hoeveelheid stage/werk"
                                " tijdens je opleiding?"),
                        latest_nr, year))

        db.commit()
        db.close()

        return 0

    elif file_type == "studentenaantallen":
        data = aantallen_parser(filepath)

        # Check latest studentID in db
        latest_nr = db.execute("SELECT student_id FROM student "
                               "ORDER BY student_id DESC LIMIT 1;").fetchall()

        # If no studentIDs, start at 0
        if len(latest_nr) == 0:
            latest_nr = [0]
        latest_nr = latest_nr[0]

        # Loop through dict
        for years in data:
            # Check if year is present in db
            year_check = db.execute("SELECT * FROM calender_year WHERE "
                                    "calender_year = ?;", (years,)).fetchall()

            # If not present, add
            if len(year_check) == 0:
                db.execute("INSERT INTO calender_year (calender_year) VALUES "
                           "(?);", (years,))

            for rows in data[years]:
                row_dict = data[years][rows]
                level = row_dict.get("Niveau").lower()

                # See if the course is already present
                course = db.execute("SELECT * FROM course WHERE courseID = ? "
                                    "AND course_level = ? AND team_name = ? "
                                    "AND locationID = ?;",
                                    (row_dict.get("Crebo- / elementcode"),
                                     level,
                                     row_dict.get("Team"),
                                     row_dict.get("Locatie"))).fetchall()

                # If not, add it
                if len(course) == 0:
                    db.execute("INSERT INTO course (courseID, course_level, "
                               "course_name, team_name, "
                               "locationID) VALUES (?, ?, ?, ?, ?);",
                               (row_dict.get("Crebo- / elementcode"),
                                level,
                                row_dict.get("Opleiding"),
                                row_dict.get("Team"),
                                row_dict.get("Locatie")))

                # Increment studentID by one
                latest_nr += 1

                # Enter student into file
                db.execute("INSERT INTO student (student_id, calender_year,"
                           "sex, birth_date, age, nationality, learning_path,"
                           "learning_year, start_date, end_date,"
                           " planned_end_date, reason_study_end, funded,"
                           "study_intensity, pre_education, courseID,"
                           "course_level, team_name, locationID) VALUES "
                           "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                           "?, ?, ?, ?);",
                           (latest_nr, years,
                            row_dict.get("Geslacht"),
                            str(row_dict.get("Geboortedatum")),
                            row_dict.get("Leeftijd"),
                            row_dict.get("Nationaliteit 1"),
                            row_dict.get("Leerweg"),
                            row_dict.get("Lj"),
                            str(row_dict.get("Begindatum")),
                            str(row_dict.get("Einddatum")),
                            str(row_dict.get("Geplande einddatum")),
                            row_dict.get("Reden beëindigen"),
                            row_dict.get("Bekostigd"),
                            row_dict.get("Intensiteit"),
                            row_dict.get("Soort vooropleiding"),
                            row_dict.get("Crebo- / elementcode"),
                            row_dict.get("Niveau"),
                            row_dict.get("Team"),
                            row_dict.get("Locatie")))

        db.commit()
        db.close()
        return 0

    else:
        return 98


if __name__ == '__main__':
    testpath1 = "20200702_JOB-monitor_2020_databestand_v3.0.xlsx"
    testpath2 = "01-mbo-indicatoren-per-instelling-2020.xls"
    testpath3 = "studentaantallen_geanonimiseerd_wur.xlsx"
    testpath4 = "01-mbo-indicatoren-per-instelling-2019.xls"
    testpath5 = "01-mbo-indicatoren-per-instelling-2018.xlsx"
    result = write_data(testpath1, "job")
    print(result)
    result = write_data(testpath2, "indicatoren")
    print(result)
    result = write_data(testpath3, "studentenaantallen")
    print(result)
    result = write_data(testpath4, "indicatoren")
    print(result)
    result = write_data(testpath5, "indicatoren")
    print(result)
    print("DONE!")
