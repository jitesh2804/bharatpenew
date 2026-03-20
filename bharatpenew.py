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

# Date & Timestamp
current_date = datetime.now().strftime("%Y%m%d")
current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# CSV File Name
csv_file = f"bharatpe_{current_timestamp}.csv"

try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

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
        u.name AS agentID,
        t.t1 AS t1,   -- 👈 NEW COLUMN
        c.dnis AS DNIS,
        cam.name AS campaign
    FROM cr_recording_log r
    JOIN cr_conn_cdr c 
        ON r.accountcode = c.accountcode 
        AND c.callstartdate::DATE = CURRENT_DATE  

    LEFT JOIN ct_user u 
        ON c.agentid = u.id

    LEFT JOIN ct_campaign cam
        ON c.campid = cam.id

    LEFT JOIN (
        SELECT t1 FROM englishin_1688622587882_history
        UNION ALL
        SELECT t1 FROM kannadain_1688622587882_history
        UNION ALL
        SELECT t1 FROM malayalamin_1688622587882_history
        UNION ALL
        SELECT t1 FROM hindiin_1688622587882_history
        UNION ALL
        SELECT t1 FROM tamilin_1688622587882_history
        UNION ALL
        SELECT t1 FROM teluguin_1688622587882_history
        UNION ALL
        SELECT t1 FROM bengali_1688622587882_history
    ) t
    ON c.phonenumber = t.t1

    WHERE r.eventdate::DATE = CURRENT_DATE
    AND c.calltype IN ('IN', 'OUT');
    """

    cursor.execute(query)
    records = cursor.fetchall()

    with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        # ✅ Header updated (t1 added)
        writer.writerow([
            "ticketId", "ftpPath", "fileName", "key1", "vendor",
            "callType", "callDuration", "ANI", "CREATED",
            "agentID", "t1", "fileSize", "DNIS", "campaign"
        ])

        for row in records:
            ticketId, ftpPath, fileName, key1, vendor, callType, callDuration, ANI, CREATED, agentID, t1, DNIS, campaign = row

            fileName = os.path.basename(fileName)

            # Convert Call Type
            if callType == "OUT":
                callType = "OUTBOUND"
            elif callType == "IN":
                callType = "INBOUND"

            # Convert Duration
            if callDuration is not None:
                seconds = int(callDuration)
                callDuration = str(timedelta(seconds=seconds))
            else:
                callDuration = "00:00:00"

            fileSize = ""

            writer.writerow([
                ticketId, ftpPath, fileName, key1, vendor,
                callType, callDuration, ANI, CREATED,
                agentID, t1, fileSize, DNIS, campaign
            ])

    print(f"CSV file '{csv_file}' created successfully with {len(records)} records!")

except Exception as e:
    print("Error:", e)

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
