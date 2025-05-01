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

# Modified SQL: ticketId first
query = f"""
SELECT 
    c.uniqueid AS ticketId,
    '{current_date}' AS ftpPath,
    r.recfilename AS fileName,
    r.accountcode AS key1,
    'COGENT' AS vendor,
    c.calltype AS callType,
    c.callduration AS callDuration,
    c.phonenumber AS ANI,
    c.callstartdate AS CREATED,
    u.name AS agentName
FROM cr_recording_log r
JOIN cr_conn_cdr c 
    ON r.accountcode = c.accountcode 
    AND c.callstartdate::DATE = CURRENT_DATE  
LEFT JOIN ct_user u 
    ON c.agentid = u.id
WHERE r.eventdate::DATE = CURRENT_DATE;
"""

cursor.execute(query)
records = cursor.fetchall()

# Write CSV
with open(csv_file, mode="w", newline="") as file:
    writer = csv.writer(file)

    # Updated header with ticketId first
    writer.writerow([
        "ticketId", "ftpPath", "fileName", "key1", "vendor", "callType",
        "callDuration", "ANI", "CREATED", "agentID", "fileSize"
    ])

    # Writing rows
    for row in records:
        ticketId, ftpPath, fileName, key1, vendor, callType, callDuration, ANI, CREATED, agentName = row

        fileName = os.path.basename(fileName)
        callType = "OUTBOUND" if callType == "OUT" else "INBOUND" if callType == "IN" else callType
        callDuration = str(timedelta(seconds=int(callDuration))) if callDuration else "00:00:00"
        fileSize = ""  # Placeholder

        writer.writerow([
            ticketId, ftpPath, fileName, key1, vendor, callType,
            callDuration, ANI, CREATED, agentName, fileSize
        ])

# Close connections
cursor.close()
conn.close()

print(f"CSV file '{csv_file}' has been created successfully with ticketId as the first column.")
