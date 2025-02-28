import sqlite3
import pandas as pd
import numpy as np

# SQLite database file
db_file = "python-keystone.db"
password = "Keystone@sqllite"

def fetch_data(start_date, end_date):
    conn = None
    cursor = None

    try:
        # Connect to SQLite with encryption
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA key = '{password}';")  # Set encryption key

        # Query 1: Fetch daily reports
        query1 = """
            SELECT
                Incident_No_drm, ticketid_drm, IncidentType_drm,
                Assign_To_drm, EmpCode_drm, IncidentDate_drm,
                State_drm, poplocation_drm, ATADate_drm,
                TechCloseDateTime_drm, Status_drm, resolutionflag_drm,
                PausedTime_drm, ApprovedAmount_drm, ReportingManager_drm,
                AssetID_drm,
                traveldistance_edtl
            FROM dailyreportmst_drm
            JOIN engineerdailytravellogs_edtl ON ticketid_drm=ticketid_edtl
            WHERE IncidentDate_drm BETWEEN %s AND %s
            AND date_edtl BETWEEN %s AND %s;
        """
        cursor.execute(query1, (start_date, end_date,start_date,end_date))
        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

        # Query 2: Fetch engineer details
        query2 = """
            SELECT 
                d.ecode_em, 
                d.EngName, 
                CONCAT(e.fname_em, ' ', e.lname_em) AS RM, 
                d.poplocation_em, 
                d.multipoplocation_pm_em
            FROM (
                SELECT 
                    ecode_em, 
                    CONCAT(fname_em, ' ', lname_em) AS EngName, 
                    reportingmanager_em, 
                    poplocation_em, 
                    multipoplocation_pm_em
                FROM em
                WHERE 
                    CONCAT(fname_em, ' ', lname_em) IN (
                        SELECT DISTINCT Assign_To_drm FROM drm
                    ) 
                    -- AND resignedflag_em = 'N' 
                    -- AND deleteflag_em = 'N'
            ) AS d
            LEFT JOIN em AS e 
                ON d.reportingmanager_em = e.typeid_em
            WHERE role_rm_em != 0;
        """
        cursor.execute(query2)
        df2 = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

    except sqlite3.Error as e:
        print(f"Error: {e}")
        return None, None

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    ##################################### Data Cleaning ##########################################
    df = df.dropna(subset=['Assign_To_drm'])  # Remove NaN values
    df = df[df['Assign_To_drm'] != 'Service Desk']  # Remove 'Service Desk' rows

    # Remove extra spaces
    cols_to_strip = ['EmpCode_drm', 'poplocation_drm', 'Assign_To_drm']
    df[cols_to_strip] = df[cols_to_strip].apply(lambda col: col.str.strip())

    # Fill NaN values
    df['ApprovedAmount_drm'] = df['ApprovedAmount_drm'].fillna(0)
    df['IncidentDate_drm'] = pd.to_datetime(df['IncidentDate_drm'], errors='coerce')
    df['TechCloseDateTime_drm'] = pd.to_datetime(df['TechCloseDateTime_drm'], errors='coerce')

    df['IncidentDate_drm'] = df['IncidentDate_drm'].fillna(pd.NaT)  # Use NaT instead of 0
    df['TechCloseDateTime_drm'] = df['TechCloseDateTime_drm'].fillna(pd.NaT)  # Use NaT instead of 0

    # Data Preparetion

    # Calculate the total months and months
    total_months = (df['IncidentDate_drm'].max().year - df['IncidentDate_drm'].min().year) * 12 + \
                   (df['IncidentDate_drm'].max().month - df['IncidentDate_drm'].min().month)

    total_months += 1
    months_ = min(total_months,12)

    # Calculate the difference in hours
    df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'] = (df['TechCloseDateTime_drm'] - df[
        'IncidentDate_drm']).dt.total_seconds() / 3600  # Convert to hours
    df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'] = np.floor(
        df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'])
    # Split into Hours and Minutes
    df[['Total_Paused_Time_Hrs', 'Total_Paused_Time_Mins']] = df['PausedTime_drm'].str.split(':', expand=True)

    # Convert the split values to numeric (integer)
    df['Total_Paused_Time_Hrs'] = pd.to_numeric(df['Total_Paused_Time_Hrs'], errors='coerce').fillna(0)
    df['Total_Paused_Time_Mins'] = pd.to_numeric(df['Total_Paused_Time_Mins'], errors='coerce').fillna(0)

    df['ApprovedAmount_drm'] = pd.to_numeric(df['ApprovedAmount_drm'], errors='coerce').fillna(0)
    df['EmpCode_drm'] = df['EmpCode_drm'].astype(str)

    ##################################### New DataFrame ##########################################
    HrData = pd.DataFrame()
    closed_df = df[(df['Status_drm'] == "Closed") | (df['Status_drm'] == "TechnicianClosed")]
    paused_df = df[df['Status_drm'] == "Paused"]

    # ECode-Engineer Column---------------------------------------------------------------------------
    HrData['ECode'] = pd.DataFrame(df['EmpCode_drm'].unique())
    HrData['Engineer'] = HrData['ECode'].map(df.set_index('EmpCode_drm')['Assign_To_drm'].to_dict())
    # Sort HrData by Engineer column
    HrData = HrData.sort_values(by="Engineer", ascending=True)

    # Pop location mapping---------------------------------------------------------------------------
    HrData['Pop_Location'] = HrData['ECode'].map(df.groupby('EmpCode_drm')['poplocation_drm'].agg(lambda x: x.value_counts().idxmax() if not x.empty else np.nan))

    # Reporting manager mapping---------------------------------------------------------------------------
    HrData['ReportingManager'] = HrData['ECode'].map(df.set_index('EmpCode_drm')['ReportingManager_drm'].to_dict())

    # Total Calls per engineer---------------------------------------------------------------------------
    HrData['Total_Calls'] = HrData['ECode'].map(closed_df['EmpCode_drm'].value_counts()).fillna(0).astype(int)

    # Average_Call_Per_Month---------------------------------------------------------------------------
    HrData['Average_Call_Per_Month'] = np.round(HrData['Total_Calls'] / months_).fillna(0).astype(int)

    # Total MC Calls---------------------------------------------------------------------------
    mc_counts = closed_df[closed_df['IncidentType_drm'] == 'MC'].groupby('EmpCode_drm').size()
    HrData['Total_MC_Call'] = HrData['ECode'].map(mc_counts).fillna(0).astype(int)

    # SLA MC Violated---------------------------------------------------------------------------
    sla_mc_counts = closed_df[(closed_df['IncidentType_drm'] == 'MC') & (closed_df['resolutionflag_drm'] == 'V')].groupby('EmpCode_drm').size()
    HrData['SLA_MC_Violated'] = HrData['ECode'].map(sla_mc_counts).fillna(0).astype(int)

    # Percentage SLA Violated---------------------------------------------------------------------------
    HrData['%_SLA_Violated'] = np.where(HrData['SLA_MC_Violated'] != 0, np.round((HrData['SLA_MC_Violated'] / HrData['Total_MC_Call']) * 100), 0).astype(int)

    # Repeated Calls---------------------------------------------------------------------------
    repeated_assets = df.groupby(['EmpCode_drm', 'AssetID_drm']).size().reset_index(name='count')
    repeated_calls = repeated_assets[repeated_assets['count'] > 1].groupby('EmpCode_drm')['count'].sum()
    HrData['Repeated_Calls'] = HrData['ECode'].map(repeated_calls).fillna(0).astype(int)

    # Percentage Repeated Calls---------------------------------------------------------------------------
    HrData['Average_Repeated_Call_Per_Month'] = np.where(HrData['Repeated_Calls'] != 0,
        (HrData['Repeated_Calls'] / months_).round().astype(int),
        0
    )

    # Franchise Calls---------------------------------------------------------------------------
    # Create a dictionary mapping engineers to their RMs
    rm_dict = pd.Series(df2["RM"].values, index=df2["EngName"]).to_dict()

    # Initialize an empty list to hold the results
    d = []

    # Iterate over each employee in df
    for eng in df2["EngName"]:
        # Initialize the hierarchy with the engineer's name
        a = {eng: []}
        engineer = eng

        # Traverse the hierarchy until we reach an employee without an RM
        while engineer in rm_dict and pd.notna(rm_dict[engineer]):
            rm = rm_dict[engineer]
            a[eng].append(rm)
            engineer = rm

            # Append the result to the list
        d.append(a)

    for eng in HrData['Engineer']:
        # find poplocation
        ploc_name = HrData.loc[HrData["Engineer"] == eng, "Pop_Location"].values
        # get poplocation
        ploc = ploc_name[0] if len(ploc_name) > 0 and pd.notna(ploc_name[0]) else None
        fdata = df[df['poplocation_drm'] == ploc]
        calls = 0

        entry = next((item for item in d if eng in item), None)

        if entry:  # If the engineer exists in d
            for rrm in entry[eng]:  # Iterate through RM hierarchy
                calls += fdata[fdata["Assign_To_drm"] == rrm].shape[0]

        # Assign the number of calls to the corresponding row in HrData
        HrData.loc[HrData['Engineer'] == eng, 'No of Franchise Call'] = 0 if pd.isna(calls) else int(calls)

    # Convert 'No of Franchise Call' column in HrData to integer (after calculation)
    HrData['No of Franchise Call'] = HrData['No of Franchise Call'].fillna(0).astype(int)

    # Call Close in N hrs ---------------------------------------------------------------------------
    callclose4 = closed_df[closed_df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'] <= 4].groupby(
        'EmpCode_drm').size()
    HrData['Calls Closed in 4 hrs'] = HrData['ECode'].map(callclose4).fillna(0).astype(int)

    callclose8 = closed_df[
        (closed_df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'] <= 8)
        & (closed_df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'] > 4)
        ].groupby('EmpCode_drm').size()
    HrData['Calls Closed in 8 hrs'] = HrData['ECode'].map(callclose8).fillna(0).astype(int)

    callclose24 = closed_df[
        (closed_df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'] <= 24)
        & (closed_df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'] > 8)
        ].groupby('EmpCode_drm').size()
    HrData['Calls Closed in 24 hrs'] = HrData['ECode'].map(callclose24).fillna(0).astype(int)

    callclose48 = closed_df[
        (closed_df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'] <= 48.9)
        & (closed_df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'] > 24)
        ].groupby('EmpCode_drm').size()
    HrData['Calls Closed in 48 hrs'] = HrData['ECode'].map(callclose48).fillna(0).astype(int)

    callclosemore48 = closed_df[closed_df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'] > 48].groupby(
        'EmpCode_drm').size()
    HrData['Calls Closed > 48 hrs'] = HrData['ECode'].map(callclosemore48).fillna(0).astype(int)

    # Total MC Calls Paused---------------------------------------------------------------------------
    mc_pause_counts = df[(df['IncidentType_drm'] == 'MC') & (df['Status_drm'] == "Paused")].groupby(
        'EmpCode_drm').size()

    # Map these counts to HrData based on EmpCode_drm
    HrData['Total_MC_Calls_Paused'] = HrData['ECode'].map(mc_pause_counts).fillna(0).astype(int)

    # Percentage Paused Calls---------------------------------------------------------------------------
    HrData['%_Paused_call'] = np.where(HrData['Total_MC_Calls_Paused'] != 0,
                                       np.round((HrData['Total_MC_Calls_Paused'] / HrData['Total_MC_Call']) * 100),
                                       0).astype(int)

    # Total Paused Time (HH:MM)---------------------------------------------------------------------------
    paused_mc_df = paused_df[paused_df['IncidentType_drm'] == 'MC']

    # Convert hours & minutes to total seconds
    paused_mc_df['Total_Seconds'] = (
            paused_mc_df['Total_Paused_Time_Hrs'] * 3600 + paused_mc_df['Total_Paused_Time_Mins'] * 60
    )

    # Group by 'ECode' and sum total seconds
    grouped_df = paused_mc_df.groupby('EmpCode_drm', as_index=False)['Total_Seconds'].sum()
    # # Ensure 'Total_Seconds' is numeric

    # Convert total seconds to hours, minutes, and seconds
    grouped_df['Total_Hours'] = (grouped_df['Total_Seconds'] // 3600).astype(int)
    grouped_df['Total_Minutes'] = ((grouped_df['Total_Seconds'] % 3600) // 60).astype(int)

    # Format as "HH:MM:SS"
    # Ensure ECode is present in grouped_df before mapping
    paused_time_mapping = grouped_df.set_index('EmpCode_drm')['Total_Hours'].astype(str).str.zfill(2) + ":" + \
                          grouped_df.set_index('EmpCode_drm')['Total_Minutes'].astype(str).str.zfill(2)

    # Map and fill missing values with "00:00"
    HrData['Total_Paused_Time(HH:MM)'] = HrData['ECode'].map(paused_time_mapping).fillna("00:00")

    # Average Paused time -------------------------------------------------------------------------------------------------
    # Extract hours from "HH:MM" format
    HrData['Paused_Hours'] = HrData['Total_Paused_Time(HH:MM)'].str.split(":").str[0].astype(int)

    # Divide by Total_MC_Calls_Paused, ensuring no division by zero
    HrData['Average Paused time'] = np.where(
        HrData['Total_MC_Calls_Paused'] != 0,
        (HrData['Paused_Hours'] / HrData['Total_MC_Calls_Paused']).fillna(0),
        0
    ).astype(int)

    del HrData['Paused_Hours']

    # Approved Amount per Engineer-------------------------
    HrData['ApprovedAmount'] = HrData['ECode'].map(closed_df.groupby('EmpCode_drm')['ApprovedAmount_drm'].sum()).fillna(0).astype(int)

    #################### no of calls > 70 km ###################
    # ✅ Step 1: Filter `closed_df1` where `traveldistance_edtl > 70`
    gt70_df = closed_df1[closed_df1["traveldistance_edtl"] > 70]

    # ✅ Step 2: Count occurrences of `EmpCode_drm`
    count_df = gt70_df.groupby("EmpCode_drm")["traveldistance_edtl"].count().reset_index()
    count_df.columns = ["ECode", "count_above_70km"]  # Renaming for easy mapping

    # ✅ Step 3: Create mapping dictionary from `count_df`
    mapping = count_df.set_index("ECode")["count_above_70km"].to_dict()

    # ✅ Step 4: Use `.map()` to assign values to `HrData["ECode"]`
    HrData["No of calls >70 Km"] = HrData["ECode"].map(mapping).fillna(0).astype(int)

    ##################### Average call per month > 70 Km   ############################

    HrData['Average call per month > 70 Km'] = np.where(
        HrData['No of calls >70 Km'] != 0,
        (HrData['No of calls >70 Km'] / months_).round().astype(int),
        0
    )

    # new order-------------------------
    new_order = ["Engineer", "ECode", "Pop_Location", "ReportingManager", "Total_Calls",
                 "Average_Call_Per_Month", "Total_MC_Call", "SLA_MC_Violated", "%_SLA_Violated",
                 "No of calls >70 Km", "Average call per month > 70 Km",
                 "Repeated_Calls", "Average_Repeated_Call_Per_Month", "No of Franchise Call",
                 "Calls Closed in 4 hrs", "Calls Closed in 8 hrs", "Calls Closed in 24 hrs",
                 "Calls Closed in 48 hrs", "Calls Closed > 48 hrs", "Total_MC_Calls_Paused",
                 "%_Paused_call", "Total_Paused_Time(HH:MM)", "Average Paused time", "ApprovedAmount"
                 ]

    # arrange HrData columns
    HrData = HrData[new_order]

    return df,df2,HrData

# # Example Usage
# import datetime
# df,df2,HrData = fetch_data(datetime.date(2024, 10, 1), datetime.date(2024, 12, 31))
# print(HrData.columns)
