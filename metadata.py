import psycopg2
import csv
from datetime import datetime, timedelta
import os

# ============================================================
# PostgreSQL Connection Details
# ============================================================

db_params = {
    "dbname": "verve",
    "user": "postgres",
    "password": "Avis!123",
    "host": "192.168.160.229",
    "port": "5433"
}

# ============================================================
# REPORT DATE
# Change only this date whenever required
# Format: YYYYMMDD
# ============================================================

REPORT_DATE = "20260818"

# Convert report date
report_date = datetime.strptime(REPORT_DATE, "%Y%m%d")

# Next day for SQL upper boundary
next_date = report_date + timedelta(days=1)

# SQL date format
report_date_sql = report_date.strftime("%Y-%m-%d")
next_date_sql = next_date.strftime("%Y-%m-%d")

# FTP Path
report_date_yyyymmdd = report_date.strftime("%Y%m%d")

# Current execution time
current_time = datetime.now().strftime("%H%M%S")

# ============================================================
# CSV Filename
# Example: bharatpe_20260816_043609.csv
# ============================================================

csv_file = f"bharatpe_{report_date_yyyymmdd}_{current_time}.csv"

# ============================================================
# Database Variables
# ============================================================

conn = None
cursor = None

try:

    print("=" * 70)
    print("BHARATPE RECORDING REPORT")
    print("=" * 70)

    print(f"Report Date : {report_date_sql}")
    print(f"Next Date   : {next_date_sql}")
    print(f"FTP Path    : {report_date_yyyymmdd}")
    print(f"CSV File    : {csv_file}")

    print("")
    print("Connecting to PostgreSQL...")

    # ========================================================
    # Database Connection
    # ========================================================

    conn = psycopg2.connect(**db_params)

    cursor = conn.cursor()

    print("Database connected successfully.")

    # ========================================================
    # SQL Query
    # ========================================================

    query = """
    SELECT
        CASE
            WHEN COALESCE(
                crm.ticketid::text,
                c.uniqueid::text
            ) ~ '^[0-9]{10,}$'
            THEN ''
            ELSE COALESCE(
                crm.ticketid::text,
                c.uniqueid::text
            )
        END AS ticketId,

        %s AS ftpPath,

        r.recfilename AS fileName,

        r.accountcode AS key1,

        'COGENT' AS vendor,

        c.calltype AS callType,

        c.callduration AS callDuration,

        c.phonenumber AS ANI,

        c.callstartdate AS CREATED,

        u.name AS agentID,

        t.t1 AS T1,

        c.dnis AS DNIS,

        cam.name AS campaign,

        d.type AS disposition_type,

        d.name AS disposition_name

    FROM cr_recording_log r

    INNER JOIN cr_conn_cdr c

        ON r.accountcode = c.accountcode

        AND c.callstartdate >= %s::timestamp

        AND c.callstartdate < %s::timestamp


    LEFT JOIN (

        SELECT DISTINCT ON (phone1)

            phone1,

            ticketid

        FROM bharatpespeakerslow_1688622587882

        WHERE phone1 IS NOT NULL

        ORDER BY phone1

    ) crm

        ON c.phonenumber = crm.phone1


    LEFT JOIN ct_dispositions d

        ON c.dispoid = d.id


    LEFT JOIN ct_user u

        ON c.agentid = u.id


    LEFT JOIN ct_campaign cam

        ON c.campid = cam.id


    LEFT JOIN (

        SELECT t1
        FROM englishin_1688622587882_history

        UNION ALL

        SELECT t1
        FROM kannadain_1688622587882_history

        UNION ALL

        SELECT t1
        FROM malayalamin_1688622587882_history

        UNION ALL

        SELECT t1
        FROM hindiin_1688622587882_history

        UNION ALL

        SELECT t1
        FROM tamilin_1688622587882_history

        UNION ALL

        SELECT t1
        FROM teluguin_1688622587882_history

        UNION ALL

        SELECT t1
        FROM bengali_1688622587882_history

    ) t

        ON c.phonenumber = t.t1


    WHERE

        r.eventdate >= %s::timestamp

        AND r.eventdate < %s::timestamp

        AND c.calltype IN ('IN', 'OUT')

    ORDER BY c.callstartdate ASC;
    """

    # ========================================================
    # Execute Query
    # ========================================================

    print("")
    print("Fetching records...")

    cursor.execute(
        query,
        (
            report_date_yyyymmdd,
            report_date_sql,
            next_date_sql,
            report_date_sql,
            next_date_sql
        )
    )

    records = cursor.fetchall()

    print(f"Total records fetched: {len(records)}")

    # ========================================================
    # Create CSV
    # ========================================================

    print("")
    print("Creating CSV file...")

    with open(
        csv_file,
        mode="w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.writer(file)

        # ====================================================
        # CSV Header
        # ====================================================

        writer.writerow([
            "ticketId",
            "ftpPath",
            "fileName",
            "key1",
            "vendor",
            "callType",
            "callDuration",
            "ANI",
            "CREATED",
            "agentID",
            "T1",
            "fileSize",
            "DNIS",
            "campaign",
            "midnumber",
            "Disposition Type",
            "Disposition Name"
        ])

        # ====================================================
        # Process Records
        # ====================================================

        for row in records:

            (
                ticketId,
                ftpPath,
                fileName,
                key1,
                vendor,
                callType,
                callDuration,
                ANI,
                CREATED,
                agentID,
                T1,
                DNIS,
                campaign,
                disposition_type,
                disposition_name
            ) = row

            # =================================================
            # Ticket ID
            # =================================================

            ticketId = (
                str(ticketId).strip()
                if ticketId
                else ""
            )

            # Remove ticketId if numeric and >= 10 digits
            if ticketId.isdigit() and len(ticketId) >= 10:
                ticketId = ""

            # =================================================
            # File Name
            # =================================================

            fileName = (
                os.path.basename(fileName)
                if fileName
                else ""
            )

            # =================================================
            # Call Type
            # =================================================

            if callType == "OUT":

                callType = "OUTBOUND"

            elif callType == "IN":

                callType = "INBOUND"

            else:

                callType = callType or ""

            # =================================================
            # Call Duration
            # Convert seconds to HH:MM:SS
            # =================================================

            if callDuration is not None:

                try:

                    total_seconds = int(float(callDuration))

                    hours = total_seconds // 3600

                    minutes = (
                        total_seconds % 3600
                    ) // 60

                    seconds = total_seconds % 60

                    callDuration = (
                        f"{hours:02d}:"
                        f"{minutes:02d}:"
                        f"{seconds:02d}"
                    )

                except (
                    ValueError,
                    TypeError,
                    OverflowError
                ):

                    callDuration = "00:00:00"

            else:

                callDuration = "00:00:00"

            # =================================================
            # CREATED Date
            # =================================================

            if CREATED:

                try:

                    CREATED = CREATED.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )

                except Exception:

                    CREATED = str(CREATED)

            else:

                CREATED = ""

            # =================================================
            # Empty Fields
            # =================================================

            fileSize = ""

            midnumber = ""

            # =================================================
            # Write CSV
            # =================================================

            writer.writerow([
                ticketId,
                ftpPath or "",
                fileName,
                key1 or "",
                vendor,
                callType,
                callDuration,
                ANI or "",
                CREATED,
                agentID or "",
                T1 or "",
                fileSize,
                DNIS or "",
                campaign or "",
                midnumber,
                disposition_type or "",
                disposition_name or ""
            ])

    # ========================================================
    # Completed
    # ========================================================

    print("")
    print("=" * 70)
    print("CSV CREATED SUCCESSFULLY")
    print("=" * 70)

    print(f"Report Date   : {report_date_sql}")
    print(f"FTP Path      : {report_date_yyyymmdd}")
    print(f"Total Records : {len(records)}")
    print(f"CSV File      : {csv_file}")

    print("=" * 70)


except psycopg2.Error as db_error:

    print("")
    print("=" * 70)
    print("POSTGRESQL ERROR")
    print("=" * 70)

    print(db_error)


except Exception as e:

    print("")
    print("=" * 70)
    print("ERROR")
    print("=" * 70)

    print(e)


finally:

    # ========================================================
    # Close Cursor
    # ========================================================

    if cursor is not None:

        try:

            cursor.close()

        except Exception:

            pass

    # ========================================================
    # Close Database Connection
    # ========================================================

    if conn is not None:

        try:

            conn.close()

        except Exception:

            pass

    print("")
    print("Database connection closed.")
