import psycopg2
import csv
from datetime import datetime, timedelta
import os

# PostgreSQL Connection Details
db_params = {
    "dbname": "verve",
    "user": "postgres",
    "password": "Avis!123",
    "host": "192.168.160.229",
    "port": "5433"
}

# Get Current Date & Time
current_date = datetime.now().strftime("%Y%m%d")
current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# CSV Filename
csv_file = f"bharatpe_{current_timestamp}.csv"

# Connect to PostgreSQL
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# SQL Query
query = """
SELECT 
    c.uniqueid AS ticketId,
    c.phonenumber AS phonenumber,
    TO_CHAR(CURRENT_DATE, 'YYYYMMDD') AS ftpPath,
    r.recfilename AS fileName,
    r.accountcode AS key1,
    'COGENT' AS vendor,
    c.calltype AS callType,
    c.callduration AS callDuration,
    c.phonenumber AS ANI,
    TO_CHAR(c.callstartdate, 'YYYY-MM-DD HH24:MI:SS') AS CREATED,
    u.name AS agentID,
    COALESCE(
        hin.t1, 
        eng.t1, 
        tam.t1, 
        tel.t1, 
        kan.t1, 
        mal.t1, 
        ben.t1
    ) AS T1,
    COALESCE(
        hin.midnumber, 
        eng.midnumber, 
        tam.midnumber, 
        tel.midnumber, 
        kan.midnumber, 
        mal.midnumber, 
        ben.midnumber
    ) AS midnumber
FROM cr_recording_log r
JOIN cr_conn_cdr c 
    ON r.accountcode = c.accountcode 
    AND c.callstartdate::DATE = CURRENT_DATE
LEFT JOIN ct_user u 
    ON c.agentid = u.id
LEFT JOIN hindiin_1688622587882_history hin
    ON hin.accountcode = c.accountcode
LEFT JOIN englishin_1688622587882_history eng
    ON eng.accountcode = c.accountcode
LEFT JOIN tamilin_1688622587882_history tam
    ON tam.accountcode = c.accountcode
LEFT JOIN teluguin_1688622587882_history tel
    ON tel.accountcode = c.accountcode
LEFT JOIN kannadain_1688622587882_history kan
    ON kan.accountcode = c.accountcode
LEFT JOIN malayalamin_1688622587882_history mal
    ON mal.accountcode = c.accountcode
LEFT JOIN bengali_1688622587882_history ben
    ON ben.accountcode = c.accountcode
WHERE r.eventdate::DATE = CURRENT_DATE
  AND c.calltype != 'IN';   -- EXCLUDE INBOUND CALLS
"""

# Execute Query
cursor.execute(query)
records = cursor.fetchall()

# Write to CSV
with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)

    # Correct header
    writer.writerow([
        "ticketId", "phonenumber", "ftpPath", "fileName", "key1", "vendor", 
        "callType", "callDuration", "ANI", "CREATED", "agentID", "fileSize", "T1", "midnumber"
    ])

    # Write rows
    for row in records:
        (
            ticketId, phonenumber, ftpPath, fileName, key1, vendor,
            callType, callDuration, ANI, CREATED, agentID, T1, midnumber
        ) = row

        fileName = os.path.basename(fileName)
        callType = "OUTBOUND" if callType == "OUT" else "INBOUND" if callType == "IN" else callType
        callDuration = str(timedelta(seconds=int(callDuration))) if callDuration else "00:00:00"
        fileSize = ""  # Placeholder

        writer.writerow([
            ticketId, phonenumber, ftpPath, fileName, key1, vendor,
            callType, callDuration, ANI, CREATED, agentID, fileSize, T1, midnumber
        ])

# Close DB Connection
cursor.close()
conn.close()

print(f"✅ CSV file '{csv_file}' created successfully with T1 and midnumber columns, INBOUND excluded.")
