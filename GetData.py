import mysql.connector
import pandas as pd
import numpy as np

# Database connection details
host = "localhost"
#port = "3306", # MySQL server
user = "unorootsrm"  # MySQL username
password = "Onida@srm101"  # MySQL password
database = "erprmwise"  #database name

def fetch_data(start_date, end_date):
    conn = None  # Initialize conn to None
    cursor = None  # Initialize cursor to None

    try:
        # Connect to MySQL
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        cursor = conn.cursor(dictionary=True)  # Fetch results as dictionaries

        # Query 1: Fetch daily reports
        query1 = """
            SELECT
                Incident_No_drm, ticketid_drm, IncidentType_drm,
                Assign_To_drm, EmpCode_drm, IncidentDate_drm,
                State_drm, poplocation_drm, ATADate_drm,
                TechCloseDateTime_drm, Status_drm, resolutionflag_drm,
                PausedTime_drm, ApprovedAmount_drm, ReportingManager_drm,
                AssetID_drm
            FROM dailyreportmst_drm
            WHERE IncidentDate_drm BETWEEN %s AND %s;
        """
        cursor.execute(query1, (start_date, end_date))
        df = pd.DataFrame(cursor.fetchall())

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
                FROM engineermst_em
                WHERE 
                    CONCAT(fname_em, ' ', lname_em) IN (
                        SELECT DISTINCT Assign_To_drm FROM dailyreportmst_drm
                    ) 
                    -- AND resignedflag_em = 'N' 
                    -- AND deleteflag_em = 'N'
            ) AS d
            LEFT JOIN engineermst_em AS e 
                ON d.reportingmanager_em = e.typeid_em
            WHERE role_rm_em != 0;
        """
        cursor.execute(query2)
        df2 = pd.DataFrame(cursor.fetchall())

    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return None, None  # Return None in case of failure

    finally:
        # Close the cursor and connection safely
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

    ##################################### data Cleaning ##########################################

    # Remove rows where 'Assign_To_drm' is None (NaN) or 'Service Desk'
    df = df.dropna(subset=['Assign_To_drm'])  # Remove NaN values
    df = df[df['Assign_To_drm'] != 'Service Desk']  # Remove 'Service Desk' rows

    # remove extra spaces from columns
    cols_to_strip = ['EmpCode_drm', 'poplocation_drm', 'Assign_To_drm']
    df[cols_to_strip] = df[cols_to_strip].apply(lambda col: col.str.strip())

    ### for "ApprovedAmount_drm" ###
    df['ApprovedAmount_drm'] = df['ApprovedAmount_drm'].fillna(0)

    ### for "IncidentDate_drm" ###
    df['IncidentDate_drm'] = df['IncidentDate_drm'].str.replace(r'[^0-9 :-]', '',regex=True)  # Remove unwanted characters
    # df['IncidentDate_drm'] = df['IncidentDate_drm'].str.strip()  # Remove extra spaces

    # Convert to datetime after cleaning
    df['IncidentDate_drm'] = pd.to_datetime(
        df['IncidentDate_drm'].str.strip(),
        errors='coerce'
    )

    ### for "TechCloseDateTime_drm" ###
    df['TechCloseDateTime_drm'] = df['TechCloseDateTime_drm'].str.replace(r'[^0-9 :-]', '',regex=True)  # Remove unwanted characters

    # Convert to datetime after cleaning
    df['TechCloseDateTime_drm'] = pd.to_datetime(
        df['TechCloseDateTime_drm'].str.strip(),  # Remove extra spaces
        errors='coerce'
    )

    # df = df.dropna(subset=['EmpCode_drm'])

    ##################################### data preparing ##########################################
    df['EmpCode_drm'] = df['EmpCode_drm'].astype(str)

    # Calculate the total months and months
    total_months = (df['IncidentDate_drm'].max().year - df['IncidentDate_drm'].min().year) * 12 + \
                   (df['IncidentDate_drm'].max().month - df['IncidentDate_drm'].min().month)

    total_months+=1
    months_ = total_months if total_months <= 12 else 12

    # Calculate the difference in hours
    df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'] = (df['TechCloseDateTime_drm'] - df[
        'IncidentDate_drm']).dt.total_seconds() / 3600

    # Split into Hours and Minutes
    df[['Total_Paused_Time_Hrs', 'Total_Paused_Time_Mins']] = df['PausedTime_drm'].str.split(':', expand=True)

    # Convert the split values to numeric (integer)
    df['Total_Paused_Time_Hrs'] = pd.to_numeric(df['Total_Paused_Time_Hrs'], errors='coerce').fillna(0)
    df['Total_Paused_Time_Mins'] = pd.to_numeric(df['Total_Paused_Time_Mins'], errors='coerce').fillna(0)

    df['ApprovedAmount_drm'] = pd.to_numeric(df['ApprovedAmount_drm'], errors='coerce').fillna(0)
    df['EmpCode_drm'] = df['EmpCode_drm'].astype(str)

    ##################################### new dataframe ##########################################
    # new empty dataframe
    HrData = pd.DataFrame()

    closed_df = df[(df['Status_drm'] == "Closed") | (df['Status_drm'] == "TechnicianClosed")]
    paused_df = df[df['Status_drm'] == "Paused"]
    ##################################### ECODE ###########################################
    # create Engineer col
    HrData['ECode'] = pd.DataFrame(df['EmpCode_drm'].unique())

    ##################################### ENGINEER NAME ##########################################
    # Create a dictionary mapping Assign_To_drm to EmpCode_drm
    ename_to_ecode = df.set_index('EmpCode_drm')['Assign_To_drm'].to_dict()

    # Map Engineer names based on ECode
    HrData['Engineer'] = HrData['ECode'].map(ename_to_ecode)

    # Sort HrData by Engineer column
    HrData = HrData.sort_values(by="Engineer", ascending=True)

    ##################################### POP LOCATION ##########################################
    pop_location_mapping = closed_df.groupby('EmpCode_drm')['poplocation_drm'].agg(
        lambda x: x.value_counts().idxmax() if not x.empty else np.nan
    )
    # Map the most frequent Pop Location to each Ecode in Calculation
    HrData['Pop_Location'] = HrData['ECode'].map(pop_location_mapping)
    ##################################### ReportingManager ##########################################
    # Create the ECode to EName mapping dictionary
    ecode_to_reportmanager = closed_df.set_index('EmpCode_drm')['ReportingManager_drm'].to_dict()

    # Use map to create the EName column in HrData
    HrData['ReportingManager'] = HrData['ECode'].map(ecode_to_reportmanager)
    ##################################### TOTAL CALLS ##########################################
    # total_call_mapping = df.groupby('EmpCode_drm')

    totalcallsmap = closed_df['EmpCode_drm'].value_counts()

    HrData['Total_Calls'] = HrData['ECode'].map(totalcallsmap).fillna(0).astype(int)
    ##################################### AVERAGE CALLS MONTH ##########################################

    # HrData['Average_Call_Per_Month'] = (HrData['Total_Calls'] // months_).astype(int) # round down to nearest
    HrData['Average_Call_Per_Month'] = np.round(HrData['Total_Calls'] / months_).fillna(0).astype(int)

    ##################################### TOTAL MC Call ##########################################

    # Count occurrences of "MC" in IncidentType_drm for each EmpCode_drm
    mc_counts = closed_df[closed_df['IncidentType_drm'] == 'MC'].groupby('EmpCode_drm').size()

    # Map these counts to HrData based on EmpCode_drm
    HrData['Total_MC_Call'] = HrData['ECode'].map(mc_counts).fillna(0).astype(int)
    ##################################### SLA_MC_Violated ##########################################

    # Count occurrences of "MC" in IncidentType_drm AND Violated in SLA Label for each EmpCode_drm
    sla_mc_counts = closed_df[(closed_df['IncidentType_drm'] == 'MC') & (closed_df['resolutionflag_drm'] == 'V')].groupby(
        'EmpCode_drm').size()

    # Map these counts to HrData based on EmpCode_drm
    HrData['SLA_MC_Violated'] = HrData['ECode'].map(sla_mc_counts).fillna(0).astype(int)
    ##################################### Total MC Call Paused ##########################################

    # Count occurrences of "MC" in IncidentType_drm AND Violated in SLA Label for each EmpCode_drm
    mc_pause_counts = df[(df['IncidentType_drm'] == 'MC') & (df['Status_drm'] == "Paused")].groupby(
        'EmpCode_drm').size()

    # Map these counts to HrData based on EmpCode_drm
    HrData['Total_MC_Calls_Paused'] = HrData['ECode'].map(mc_pause_counts).fillna(0).astype(int)
    ##################################### Calls_Closed_in_n_hrs ##########################################

    callclose4 = closed_df[closed_df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'] <= 4].groupby('EmpCode_drm').size()
    HrData['Calls Closed in 4 hrs'] = HrData['ECode'].map(callclose4).fillna(0).astype(int)

    callclose8 = closed_df[
        (closed_df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'] <= 8)
        & (closed_df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'] >4)
    ].groupby('EmpCode_drm').size()
    HrData['Calls Closed in 8 hrs'] = HrData['ECode'].map(callclose8).fillna(0).astype(int)

    callclose24 = closed_df[
        (closed_df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'] <= 24)
        & (closed_df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'] > 8)
    ].groupby('EmpCode_drm').size()
    HrData['Calls Closed in 24 hrs'] = HrData['ECode'].map(callclose24).fillna(0).astype(int)

    callclose48 = closed_df[
        (closed_df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'] <= 48)
        & (closed_df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'] > 24)
    ].groupby('EmpCode_drm').size()
    HrData['Calls Closed in 48 hrs'] = HrData['ECode'].map(callclose48).fillna(0).astype(int)

    callclosemore48 = closed_df[closed_df['Diff-TechCloseDateTime_drm-IncidentDate_drm(hrs)'] > 48].groupby('EmpCode_drm').size()
    HrData['Calls Closed > 48 hrs'] = HrData['ECode'].map(callclosemore48).fillna(0).astype(int)

    ##################################### Total_Paused_Time ##########################################
    paused_mc_df = paused_df[paused_df['IncidentType_drm'] == 'MC']

    # Convert hours & minutes to total seconds
    paused_mc_df.loc[:, 'Total_Seconds'] = (
            paused_mc_df['Total_Paused_Time_Hrs'] * 3600 + paused_mc_df['Total_Paused_Time_Mins'] * 60
    )

    # Group by 'ECode' and sum total seconds
    grouped_df = paused_mc_df.groupby('EmpCode_drm', as_index=False)['Total_Seconds'].sum()
    # # Ensure 'Total_Seconds' is numeric
    # grouped_df['Total_Seconds'] = pd.to_numeric(grouped_df['Total_Seconds'], errors='coerce')

    # Convert total seconds to hours, minutes, and seconds
    grouped_df['Total_Hours'] = (grouped_df['Total_Seconds'] // 3600).astype(int)
    grouped_df['Total_Minutes'] = ((grouped_df['Total_Seconds'] % 3600) // 60).astype(int)
    #grouped_df['Total_Seconds_Remaining'] = (grouped_df['Total_Seconds'] % 60).astype(int)

    # Format as "HH:MM:SS"
    # Ensure ECode is present in grouped_df before mapping
    paused_time_mapping = grouped_df.set_index('EmpCode_drm')['Total_Hours'].astype(str).str.zfill(2) + ":" + \
                          grouped_df.set_index('EmpCode_drm')['Total_Minutes'].astype(str).str.zfill(2)

    # Map and fill missing values with "00:00"
    HrData['Total_Paused_Time(HH:MM)'] = HrData['ECode'].map(paused_time_mapping).fillna("00:00")

    ##################################### %_Paused_call ##########################################
    HrData['%_Paused_call'] = np.where(
        HrData['Total_MC_Calls_Paused'] != 0,
        np.round((HrData['Total_MC_Calls_Paused'] / HrData['Total_MC_Call']) * 100),
        0
    )
    HrData['%_Paused_call'] = pd.Series(HrData['%_Paused_call']).fillna(0).astype(int)

    ############################## Average paused time #########################################
    # Extract hours from "HH:MM" format
    HrData['Paused_Hours'] = HrData['Total_Paused_Time(HH:MM)'].str.split(":").str[0].astype(int)

    # Divide by Total_MC_Calls_Paused, ensuring no division by zero
    HrData['Average Paused time'] = np.where(
        HrData['Total_MC_Calls_Paused'] != 0,
        (HrData['Paused_Hours'] / HrData['Total_MC_Calls_Paused']).fillna(0),
        0
    ).astype(int)

    del HrData['Paused_Hours']
    ##################################### %_SLA_Violated ##########################################
    HrData['%_SLA_Violated'] = np.where(
        HrData['SLA_MC_Violated'] != 0,
        np.round((HrData['SLA_MC_Violated'] / HrData['Total_MC_Call']) * 100),
        0
    )
    HrData['%_SLA_Violated'] = pd.Series(HrData['%_SLA_Violated']).fillna(0).astype(int)

    ############################### Repeated Calls ###########################################
    def calculate_repeated_assets_optimized(df):
        # Pivot table to count occurrences of (Ecode, Asset ID) pairs
        asset_counts = df.pivot_table(index=['EmpCode_drm', 'AssetID_drm'], aggfunc='size')

        # Filter for repeated assets (count > 1) using NumPy for speed
        repeated_assets = asset_counts[asset_counts.values > 1]

        # Aggregate by engineer (Ecode)
        return repeated_assets.groupby(level=0).sum().astype(int)

    repeated_assets = calculate_repeated_assets_optimized(df)
    HrData['Repeated_Calls'] =np.round(HrData['ECode'].map(repeated_assets).fillna(0)).astype(int)
    ############################### Average Repeated Calls ###########################################

    HrData['Average_Repeated_Call_Per_Month'] = np.where(
        HrData['Repeated_Calls'] != 0,
        (HrData['Repeated_Calls'] / months_).round().astype(int),
        0
    )
    #################################### No of Franchies Call ##########################
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
        #find poplocation
        ploc_name = HrData.loc[HrData["Engineer"] == eng, "Pop_Location"].values
        #get poplocation
        ploc = ploc_name[0] if len(ploc_name) > 0 and pd.notna(ploc_name[0]) else None
        fdata = df[df['poplocation_drm'] == ploc]
        calls = 0
        # for entry in d:
        #     if eng in entry:  # Find the hierarchy entry for this engineer
        #         for en in entry[eng]:
        #             # Count the calls where Assign_To_drm matches the RM (en)
        #             calls += fdata[fdata['Assign_To_drm'] == en].shape[0]
        # Find the dictionary in d that contains the key 'eng'
        entry = next((item for item in d if eng in item), None)

        if entry:  # If the engineer exists in d
            for rrm in entry[eng]:  # Iterate through RM hierarchy
                calls += fdata[fdata["Assign_To_drm"] == rrm].shape[0]

        # Assign the number of calls to the corresponding row in HrData
        HrData.loc[HrData['Engineer'] == eng, 'No of Franchise Call'] = 0 if pd.isna(calls) else int(calls)

    # Convert 'No of Franchise Call' column in HrData to integer (after calculation)
    HrData['No of Franchise Call'] = HrData['No of Franchise Call'].fillna(0).astype(int)

     # Step 2: Map counts back to the main DataFrame based on location
    # HrData['No of Franchies Call'] = df['poplocation_drm'].map(location_manager_count).fillna(0).astype(int)

    ################## Amount Paid ############################
    amount = closed_df.groupby('EmpCode_drm')['ApprovedAmount_drm'].sum()
    HrData['ApprovedAmount'] = np.round(HrData['ECode'].map(amount).fillna(0)).astype(int)

    ## arrange HrData columns
    new_order = ["Engineer","ECode","Pop_Location","ReportingManager","Total_Calls",
                 "Average_Call_Per_Month","Total_MC_Call","SLA_MC_Violated","%_SLA_Violated",
                 "Repeated_Calls","Average_Repeated_Call_Per_Month","No of Franchise Call",
                 "Calls Closed in 4 hrs","Calls Closed in 8 hrs","Calls Closed in 24 hrs",
                 "Calls Closed in 48 hrs","Calls Closed > 48 hrs","Total_MC_Calls_Paused",
                 "%_Paused_call","Total_Paused_Time(HH:MM)","Average Paused time","ApprovedAmount"
                 ]
    HrData = HrData[new_order]

    return df, df2,HrData ,d

# import datetime
# df1,df2,df,d = fetch_data( datetime.date(2024, 10, 1),  datetime.date(2024, 12, 31))
# # Convert the list of dictionaries `d` into a structured format
# hierarchy = {}
#
# # Process `d`, ensuring all engineers are added
# for entry in d:
#     for eng, rms in entry.items():
#         hierarchy[eng] = rms  # Store engineer's RM hierarchy
#
# # Ensure all engineers appear in the hierarchy (even those without RMs)
# for eng in df2["EngName"].unique():
#     if eng not in hierarchy:
#         hierarchy[eng] = []  # Assign an empty list if no RM exists
#
# # Print the hierarchy in the required format
# for eng, rms in hierarchy.items():
#     print({eng: rms})
