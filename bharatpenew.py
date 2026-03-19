import psycopg2
import csv
from datetime import datetime, timedelta
import os

# DB Connection
db_params = {
    "dbname": "verve",
    "user": "postgres",
    "password": "Avis!123",
    "host": "192.168.160.229",
    "port": "5433"
}

# Date & Time
current_date = datetime.now().strftime("%Y%m%d")
current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

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
        u.name AS agentId,   -- ✅ agent name but column agentId
        c.dnis AS DNIS,
        cam.name AS campaign,

        CASE 
            WHEN c.phonenumber = t1.englishin_1688622587882_history THEN 'ENGLISH'
            WHEN c.phonenumber = t1.kannadain_1688622587882_history THEN 'KANNADA'
            WHEN c.phonenumber = t1.malayalamin_1688622587882_history THEN 'MALAYALAM'
            WHEN c.phonenumber = t1.hindiin_1688622587882_history THEN 'HINDI'
            WHEN c.phonenumber = t1.tamilin_1688622587882 THEN 'TAMIL'
            WHEN c.phonenumber = t1.teluguin_1688622587882 THEN 'TELUGU'
            WHEN c.phonenumber = t1.bengali_1688622587882_histor THEN 'BENGALI'
            ELSE ''
        END AS language

    FROM cr_recording_log r

    JOIN cr_conn_cdr c 
        ON r.accountcode = c.accountcode 
        AND c.callstartdate::DATE = CURRENT_DATE  

    LEFT JOIN ct_user u 
        ON c.agentid = u.id

    LEFT JOIN ct_campaign cam
        ON c.campid = cam.id

    LEFT JOIN your_table t1
        ON c.phonenumber IN (
            t1.englishin_1688622587882_history,
            t1.kannadain_1688622587882_history,
            t1.malayalamin_1688622587882_history,
            t1.hindiin_1688622587882_history,
            t1.tamilin_1688622587882,
            t1.teluguin_1688622587882,
            t1.bengali_1688622587882_histor
        )

    WHERE r.eventdate::DATE = CURRENT_DATE
    AND c.calltype IN ('IN', 'OUT');
    """

    cursor.execute(query)
    records = cursor.fetchall()

    with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerow([
            "ticketId", "ftpPath", "fileName", "key1", "vendor",
            "callType", "callDuration", "ANI", "CREATED",
            "agentId", "fileSize", "DNIS", "campaign", "language"
        ])

        for row in records:
            (
                ticketId, ftpPath, fileName, key1, vendor,
                callType, callDuration, ANI, CREATED,
                agentId, DNIS, campaign, language
            ) = row

            fileName = os.path.basename(fileName)

            # Call Type Convert
            if callType == "OUT":
                callType = "OUTBOUND"
            elif callType == "IN":
                callType = "INBOUND"

            # Duration Convert
            if callDuration:
                callDuration = str(timedelta(seconds=int(callDuration)))
            else:
                callDuration = "00:00:00"

            fileSize = ""

            writer.writerow([
                ticketId, ftpPath, fileName, key1, vendor,
                callType, callDuration, ANI, CREATED,
                agentId, fileSize, DNIS, campaign, language
            ])

    print(f"CSV file '{csv_file}' created successfully with {len(records)} records!")

except Exception as e:
    print("Error:", e)

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
