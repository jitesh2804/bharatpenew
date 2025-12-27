import psycopg2
import csv
from datetime import datetime, timedelta
import os

# PostgreSQL Connection Details
db_params = {
    "dbname": "verve",
    "user": "postgres",
    "password": "Avis!123",
    "host": "10.39.39.7",
    "port": "5433"
}

# Get Current Date & Time
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

    NULL AS T1,

    COALESCE(
        lgtel.merchantid, 
        lgtam.merchantid, 
        lgtelp2p3.merchantid, 
        lgtamp2p3.merchantid, 
        man.merchantid,
        lgkan.merchantid, 
        lgkanp2p3.merchantid, 
        lenkan.merchantid, 
        lentam.merchantid, 
        lentel.merchantid
    ) AS merchantid

FROM cr_recording_log r
JOIN cr_conn_cdr c 
    ON r.accountcode = c.accountcode 
    AND c.callstartdate::DATE = CURRENT_DATE
LEFT JOIN ct_user u 
    ON c.agentid = u.id

LEFT JOIN leadgenerationteluguhyd_1741691269141_history lgtel 
    ON lgtel.accountcode = c.accountcode

LEFT JOIN leadgenerationtamilhyd_1741691269141_history lgtam 
    ON lgtam.accountcode = c.accountcode

LEFT JOIN leadgenerationteluguhydp2p3_1741691269141_history lgtelp2p3 
    ON lgtelp2p3.accountcode = c.accountcode

LEFT JOIN leadgenerationtamilhydp2p3_1741691269141_history lgtamp2p3 
    ON lgtamp2p3.accountcode = c.accountcode

LEFT JOIN manual_1741691269141_history man 
    ON man.accountcode = c.accountcode

LEFT JOIN leadgenerationkannadahyd_1741691269141_history lgkan 
    ON lgkan.accountcode = c.accountcode

LEFT JOIN leadgenerationkannadahydp2p3_1741691269141_history lgkanp2p3 
    ON lgkanp2p3.accountcode = c.accountcode

LEFT JOIN lendingkannadahyd_1741691269141_history lenkan 
    ON lenkan.accountcode = c.accountcode

LEFT JOIN lendingtamilhyd_1741691269141_history lentam 
    ON lentam.accountcode = c.accountcode

LEFT JOIN lendingteluguhyd_1741691269141_history lentel 
    ON lentel.accountcode = c.accountcode

WHERE r.eventdate::DATE = CURRENT_DATE
  AND c.calltype != 'IN';
"""

# Execute Query
cursor.execute(query)
records = cursor.fetchall()

# Write to CSV
with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)

    writer.writerow([
        "ticketId", "phonenumber", "ftpPath", "fileName", "key1", "vendor",
        "callType", "callDuration", "ANI", "CREATED", "agentID", "fileSize", "T1", "merchantid"
    ])

    for row in records:
        (
            ticketId, phonenumber, ftpPath, fileName, key1, vendor,
            callType, callDuration, ANI, CREATED, agentID, T1, merchantid
        ) = row

        fileName = os.path.basename(fileName)
        callType = "OUTBOUND" if callType == "OUT" else "INBOUND" if callType == "IN" else callType
        callDuration = str(timedelta(seconds=int(callDuration))) if callDuration else "00:00:00"
        fileSize = ""  # Placeholder

        writer.writerow([
            ticketId, phonenumber, ftpPath, fileName, key1, vendor,
            callType, callDuration, ANI, CREATED, agentID, fileSize, T1, merchantid
        ])

# Close DB Connection
cursor.close()
conn.close()

print(f"✅ CSV file '{csv_file}' created successfully with merchantid column populated, INBOUND excluded.")
