import streamlit as st
import pandas as pd
import GetData2 as gd
from datetime import date,timedelta,datetime
from PIL import Image
from streamlit_extras.metric_cards import style_metric_cards
from streamlit_option_menu import option_menu
from numerize.numerize import numerize #exist the amount (not range amount)
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="HR Analysis", page_icon=":bar_chart:", layout="wide")

#authentication
# def creds_entered():
#     if st.session_state["user"].strip() == "admin" and st.session_state["password"].strip() == "admin":
#         st.session_state["authenticated"] = True
#     else:
#         st.session_state["authenticated"]= False
#         if not st.session_state["password"]:
#             st.warning("Please enter password.")
#         elif not st.session_state["user"]:
#             st.warning("Please enter username.")
#         else:
#             st.error("Invalid Username/Password!")
#
#
# def authenticate_user():
#     col1, col2 = st.columns([0.6, 0.4])
#
#     with col2:
#         if "authenticated" not in st.session_state:
#             st.session_state["authenticated"] = False  # Default to False
#
#         username = st.text_input("Username", key="user")
#         password = st.text_input("Password", type="password", key="password")
#
#         if st.button("Login"):  # ✅ Triggered only when the user clicks
#             if username == "admin" and password == "password":  # Replace with your authentication logic
#                 st.session_state["authenticated"] = True
#                 st.success("✅ Authentication Successful!")
#             else:
#                 st.error("❌ Invalid Username or Password")
#                 st.session_state["authenticated"] = False
#
#     return st.session_state["authenticated"]


def authenticate_user():
    _,col1, col2,_ = st.columns([0.05,0.3, 0.2,0.05])

    if "authenticated" not in st.session_state or not st.session_state["authenticated"]:
        with col1:
            st.text("\n")
            st.text("\n"*5)
            st.markdown('<p style="font-size: 90px; font-weight: bold; font-family: Arial Black;">XAU</p>',
                        unsafe_allow_html=True)

    with col2:
        # ✅ Ensure authentication state exists
        if "authenticated" not in st.session_state:
            st.session_state["authenticated"] = False

        # ✅ If user is authenticated, show logout button
        if st.session_state["authenticated"]:
            # st.sidebar.success("✅ You are logged in!")
            # st.success("✅ You are logged in!")
            if st.sidebar.button("Log Out"):
                st.session_state["authenticated"] = False  # Clear login state
                st.rerun()  # ✅ Refresh the page to show login screen
            return True  # ✅ User is authenticated

        # st.title("Login User")

        # ✅ If user is not authenticated, show login form
        username = st.text_input("", placeholder="Enter your username", key="user") # ,label_visibility="collapsed"
        password = st.text_input("", placeholder="Enter your password", type="password", key="password") # label_visibility="collapsed",

        if st.button("Login"):
            if username == "admin" and password == "password123":
                st.session_state["authenticated"] = True
                st.success("✅ Authentication Successful!")
                st.rerun()  # ✅ Refresh the page to load protected content
            else:
                st.error("❌ Invalid Username or Password")
                st.session_state["authenticated"] = False

    return False  # ✅ User is not authenticated

if authenticate_user():
    # Custom CSS for stylingz
    st.markdown('<style>.block-container {padding-top: 1rem;}</style>', unsafe_allow_html=True)

    # Header Section
    imageKsys = Image.open('Ksys.png')
    # errormsg = Image.open("Ashneer Memes.jpg")
    col1, col2 = st.columns([0.1, 0.9])
    with col1:
        st.image(imageKsys, width=100)

    html_title = """
        <style>
        .title-test {
        font-weight: bold;
        padding: 5px;
        border-radius: 6px;
        }
        </style>
        <center><h1 class="title-test">Engineer Dashboard 📈</h1></center>"""
    with col2:
        st.markdown(html_title, unsafe_allow_html=True)

    st.markdown("""---""")

    # Date Selection
    st.markdown("#### 📅 Select Date Range")
    col1, col2 = st.columns(2, gap='medium')
    with col1:
        date1 = st.date_input("Start Date", date(2024, 10, 1))
    with col2:
        date2 = st.date_input("End Date", date(2024, 12, 31))
    # with filt:
    #     option = st.selectbox(
    #         label="select deshbord",
    #         options=["Engineer Performance Summary Dashboard",
    #                  "SLA Violation Dashboard",
    #                  "Geographic Performance Dashboard",
    #                  "Repeat & Franchise Call Analysis Dashboard",
    #                  "Call Resolution Efficiency Dashboard",
    #                  "Financial Performance Dashboard"],
    #     )
    #     st.write("You selected:", option)
    date1 = pd.to_datetime(date1)
    date2 = pd.to_datetime(date2)

    start_date = date1.strftime("%Y-%m-%d")
    end_date = date2.strftime("%Y-%m-%d")

    total_months = (date2.year - date1.year) * 12 + (date2.month - date1.month)
    # for issue of 12
    end_date = end_date + timedelta(days=1)
    # # Fetch Data
    # with st.spinner("Fetching data... Please wait ⏳"):  # ✅ Show loading indicator
    #     try:
    #         df1, df2, df, d = GetData.fetch_data(start_date, end_date)
    #
    #         # ✅ Stop execution if no data is returned
    #         if df is None or df.empty:
    #             st.error("❌ No data found for the selected date range. Stopping execution.")
    #             st.stop()  # 🛑 Stops further execution
    #
    #     except Exception as e:
    #
    #         _,msg , _ = st.columns(3)
    #         with msg:
    #             st.error(f"⚠️ Please Check Date Input and Try Again ⚠️")
    #             st.image(errormsg, width=430)
    #             st.stop()  # 🛑 Stop execution completely
    #         st.stop()  # 🛑 Stop execution completely
    # Fetch Data
    with st.spinner("Fetching data... Please wait ⏳"):  # ✅ Show loading indicator
        df1,df2, HrData = gd.fetch_data(start_date, end_date)

    # ✅ Detect if DataFrames are empty (indicating SQL failure)
    if df1.empty and df2.empty and HrData.empty:
            st.error("⚠️ Database connection failed! Please check MySQL and try again.")
            st.stop()  # ✅ Prevents the app from breaking

        # Sidebar Filters
    st.sidebar.header("🏂 Choose your filter:")

    engineers = st.sidebar.multiselect("👷 Engineer", HrData["Engineer"].unique())
    pop_locations = st.sidebar.multiselect("📍 POP Location", sorted(HrData["Pop_Location"].unique()))
    reporting_managers = st.sidebar.multiselect("👨‍💼 Reporting Manager", sorted(HrData["ReportingManager"].unique()))

    # Apply Filters
    HrData_filtered = HrData.copy()
    if engineers:
        HrData_filtered = HrData_filtered[HrData_filtered["Engineer"].isin(engineers)]
    if pop_locations:
        HrData_filtered = HrData_filtered[HrData_filtered["Pop_Location"].isin(pop_locations)]
        # if incident_types:
        #     df_filtered = df_filtered[df_filtered["IncidentType_drm"].isin(incident_types)]
    if reporting_managers:
        HrData_filtered = HrData_filtered[HrData_filtered["ReportingManager"].isin(reporting_managers)]


    def home():
        global HrData_filtered
        global df1

        total_eng = HrData_filtered['ECode'].count()
        box1, box2, box3, box4 = st.columns(4, gap='small')

        with box1:
            st.metric(label="Months", value=total_months + 1)

        with box2:
            st.metric(label="Engineers", value=total_eng)

        with box3:
            st.metric(label="ApproveAmount",
                      value=numerize(sum(HrData_filtered['ApprovedAmount'])),
                      help="Total approved ammount")
        with box4:
            st.metric(label="demo", value=0)

        # Apply Custom Styling to Metric Cards
        style_metric_cards(
            background_color="#FFFFFF",
            border_left_color="#ff4b4b",
            border_color="#D3D3D3",
            box_shadow=True  # "#E0E0E0",
        )
        # Display Data
        st.markdown("#### 📋 Data")
        with st.expander("VIEW EXCEL DATASET"):
            dd = HrData_filtered.T  # ✅ Remove index and transpose

            # ✅ Fix column headers by making the first row the header
            # dd.columns = dd.iloc[0]  # Set first row as column headers
            dd = dd[0:]  # Remove the first row after setting headers
            st.write(dd)

        # st.markdown("#### 📋 Data")
        # with st.expander("VIEW EXCEL DATASET"):
        #
        #     dd = df1
        #     st.write(dd)

        _, col1, col2, n = st.columns(4)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        with col1:
            csv1 = HrData_filtered.T.to_csv()
            st.download_button("Download filtered file", data=csv1, file_name=f"EngineerPerformanceReport_{timestamp}.csv",
                               mime="text/csv",
                               help="Click here to download the report as a CSV file")

        with col2:
            csv2 = HrData.T.to_csv(index=False)
            st.download_button("Download file", data=csv2, file_name=f"AllEngineerPerformanceReport_{timestamp}.csv", mime="text/csv",
                               help="Click here to download the report as a CSV file")

        # with n:
        #     csv2 = df1.to_csv(index=False)
        #     st.download_button("ReportingManager", data=csv2, file_name="df1.csv", mime="text/csv",
        #                        help="Click here to download the report as a CSV file")

        st.markdown("""---""")


    #################### function for visuals #####################################3
    # ✅ Function to Create Bar Charts
    def create_bar_chart(data, title, color, reverse_x=False):
        # data= data['Engineer'].drop_duplicates()
        height = max(300, len(data) * 50)  # Dynamically adjust height
        if data.empty:
            return None  # Avoid creating a chart with empty data
        # Sort to ensure correct top-to-bottom order
        data = data.dropna(subset=["Engineer"]).sort_values(by="Total_Calls", ascending=not reverse_x)

        fig = px.bar(
            data,
            x="Total_Calls",
            y="Engineer",
            # y=pd.Categorical(data["Engineer"], categories=data["Engineer"], ordered=True),  # Fix order
            # orientation="h",
            title=f"<b> {title} </b>",
            color_discrete_sequence=[color] * len(data),
            template="plotly_white",
            height=height,  # Set dynamic height
        )

        fig.update_traces(
            text=data["Total_Calls"],
            textposition="inside",
            insidetextanchor="end",
            textfont=dict(color="black", size=13),
            marker=dict(line=dict(color="black", width=0.8), opacity=0.9),
        )

        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="black", size=14, weight="bold"),
            margin=dict(l=10, r=10, t=40, b=10),
            xaxis=dict(
                showgrid=True,
                gridcolor='#cecdcd',
                title="Total Calls",
                range=[data["Total_Calls"].max(), 0] if reverse_x else [0, data["Total_Calls"].max()],
                # ✅ Reverse X if needed
            ),
            yaxis=dict(
                showgrid=False,
                title="",
                ticklabelposition="outside right",
                side="right",
                tickfont=dict(
                    size=12,  # ✅ Font size (increase for better readability)
                    color="Slate Gray",  # ✅ Change font color (any HEX or color name)
                    family="Arial, sans-serif",  # ✅ Set font family
                    weight="bold",  # ✅ Make text bold (Alternative: `family="Arial Black"`)
                ),
            ),
            paper_bgcolor='rgba(0, 0, 0, 0)',
        )
        return fig


    # ✅ Add Border using Streamlit CSS
    # st.markdown(
    #     """
    #     <style>
    #         div[data-testid="stPlotlyChart"] {
    #             border: 2px solid black;  /* ✅ Black Border */
    #             border-radius: 12px;  /* ✅ Rounded Corners */
    #             padding: 10px;  /* ✅ Space Inside */
    #             margin: auto;  /* ✅ Centering */
    #             width: 100% !important;  /* ✅ Ensure it fits container */
    #             max-width: 750px;  /* ✅ Limit max width */
    #             background-color: #f9f9f9;  /* ✅ Light Gray Background */
    #         }
    #     </style>
    #     """,
    #     unsafe_allow_html=True
    # )

    ## line chart
    def trends_in_calls_assigned_per_month(df1):
        # Convert IncidentDate to datetime format
        df1['IncidentDate_drm'] = pd.to_datetime(df1['IncidentDate_drm'], errors='coerce')

        # Drop NaN values
        df1 = df1.dropna(subset=['Assign_To_drm', 'IncidentDate_drm'])

        # Convert IncidentDate to Month-Year format (e.g., "Jan 2024")
        df1['Month'] = df1['IncidentDate_drm'].dt.strftime('%b %Y')  # 'Jan 2024', 'Feb 2024', etc.

        # Default to full dataset if no engineer is selected
        df11 = df1.copy()

        # Apply filter if engineers are selected
        if engineers:
            df11 = df1[df1["Assign_To_drm"].isin(engineers)]
        if pop_locations:
            df11 = df11[df11["poplocation_drm"].isin(pop_locations)]
        if reporting_managers:
            df11 = df11[df11["ReportingManager_drm"].isin(reporting_managers)]

        # Group by Month-Year and Engineer
        monthly_engineers = df11.groupby(['Month', 'Assign_To_drm']).size().reset_index(name='Total Calls')

        # Ensure correct ordering of months
        monthly_engineers['Month'] = pd.to_datetime(monthly_engineers['Month'], format='%b %Y')
        monthly_engineers = monthly_engineers.sort_values(by="Month")
        monthly_engineers['Month'] = monthly_engineers['Month'].dt.strftime(
            '%b %Y')  # Convert back to string after sorting

        # Check if data is available
        if monthly_engineers.empty:
            st.warning("No data available for the selected engineer(s) and date range.")
        else:
            # Create a filtered line chart
            fig = px.line(
                monthly_engineers,
                x='Month',
                y='Total Calls',
                color='Assign_To_drm',  # Separate lines for each engineer
                title="Engineer-wise Performance Over Time",
                text="Total Calls",
                markers=True,
                line_shape='linear'  # "spline"
            )

            # Customize layout
            fig.update_layout(
                xaxis_title="Month-Year",
                yaxis_title="Total Calls",
                template="plotly_white",
                legend_title="Engineer",
                xaxis=dict(tickangle=-45),  # Rotate x-axis labels for better readability
            )
            fig.update_traces(textposition="top center")
            # Display the filtered line chart
            st.plotly_chart(fig, use_container_width=True)


    def calls_closed_by_sla_compliance():
        fig = px.bar(
            HrData_filtered,
            x="Engineer",
            y=["Calls Closed > 48 hrs", "Calls Closed in 48 hrs", "Calls Closed in 24 hrs", "Calls Closed in 8 hrs",
               "Calls Closed in 4 hrs"],
            title="Wide-Form Input",
            text_auto=True)
        st.plotly_chart(fig, use_container_width=True)


    ### 1.  Total Calls vs. Average Call Per Month (Scatter Plot)
    def total_calls_vs_average_call_per_month():
        """
        This visualization helps understand the consistency of engineers in handling calls.
        :return:
        """
        fig = px.scatter(HrData, x="Total_Calls", y="Average_Call_Per_Month",
                         color="ReportingManager", hover_data=['Engineer', 'Pop_Location'],
                         title='Total Calls vs Average Calls Per Month')
        st.plotly_chart(fig, use_container_width=True)


    ### 2.  SLA Violation Rate (Bar Chart)
    def sla_violation_rate():
        """
        This shows the percentage of SLA violations per engineer,
        highlighting who needs attention regarding service level adherence.
        :return:
        """
        fig = px.bar(HrData, x="Engineer", y="%_SLA_Violated",
                     color="ReportingManager",
                     title='SLA Violation Rate by Engineer',
                     labels={'%_SLA_Violated': 'SLA Violation Rate (%)'})
        st.plotly_chart(fig, use_container_width=True)


    ### 3.  Calls Closed Within Timeframes (Stacked Bar Chart)
    def calls_closed_within_timeframes():
        """
        This visualizes the distribution of call closure times, indicating efficiency in resolving issues.
        :return:
        """
        categories = ['Calls Closed in 4 hrs', 'Calls Closed in 8 hrs', 'Calls Closed in 24 hrs',
                      'Calls Closed in 48 hrs',
                      'Calls Closed > 48 hrs']
        fig = go.Figure()

        for category in categories:
            fig.add_trace(go.Bar(x=HrData['Engineer'], y=HrData[category], name=category))

        fig.update_layout(
            title='Calls Closed Within Timeframes by Engineer',
            xaxis_title='Engineer',
            yaxis_title='Number of Calls',
            barmode='stack')

        st.plotly_chart(fig, use_container_width=True)


    ### 5.  Approved Amount Distribution (Histogram)
    def approved_amount_distribution():
        '''
        This shows the distribution of approved amounts,
        useful for budget analysis and identifying outliers.
        :return:
        '''
        fig = px.histogram(
            HrData,
            x="ApprovedAmount",
            nbins=30,
            title="Distribution of Approved Amounts for Engineers",
            labels={"ApprovedAmount": "Approved Amount (Currency)"},  # Clear label
            marginal="box",  # Add marginal box plot for distribution overview
            hover_data=['Engineer', 'ReportingManager']  # Show details on hover
        )

        fig.update_layout(
            xaxis_title="Approved Amount",
            yaxis_title="Number of Engineers",
            bargap=0.02,  # Small gap between bars for better readability
            title_x=0.5,  # Center the title
            # template="plotly_white"  # Cleaner look
        )

        st.plotly_chart(fig, use_container_width=True)


    ### 5. Treemap of Reporting Manager by Engineer and SLA Violations
    def treemap(df):
        df = df.dropna(subset=['ReportingManager', 'Engineer', '%_SLA_Violated'])
        df['%_SLA_Violated'] = df['%_SLA_Violated'].fillna(0)
        fig = px.treemap(df, path=['ReportingManager', 'Engineer'], values='%_SLA_Violated',
                         color='%_SLA_Violated', hover_data=['Total_Calls'],
                         title='Treemap of Reporting Manager by Engineer and SLA Violations')
        st.plotly_chart(fig, use_container_width=True)


    ############################################ Plot graphs ##############################3

    def graphs():
        st.markdown("#### 📊 Engineer Performance Summary Dashboard")
        # ------------------------------------------------------------------------------------------------
        # Sort Data for Top 5 and Bottom 5 Engineers
        Top5 = HrData_filtered[["Engineer", "Total_Calls"]].sort_values(by="Total_Calls", ascending=False).head(5)
        bottom5 = HrData_filtered[["Engineer", "Total_Calls"]].sort_values(by="Total_Calls").head(5)

        # ✅ Create Bar Charts
        fig_top5 = create_bar_chart(Top5, "TOP 5 ENGINEERS", "#ff4b4b", reverse_x=False)
        fig_bottom5 = create_bar_chart(bottom5, "BOTTOM 5 ENGINEERS", "#0083B8", reverse_x=False)
        # ✅ Streamlit Layout
        left, right = st.columns(2)

        # Fetch top-bottom chart
        with st.spinner("Loading bar Chart... Please wait ⏳"):
            if HrData_filtered.empty:
                st.warning("No data available for the selected city and engineer.")
            else:
                left.plotly_chart(fig_top5, use_container_width=True)
                right.plotly_chart(fig_bottom5, use_container_width=True)
        # -----------------------------------------------------------------------------------------------
        with st.spinner("Loading Trends_in_Calls_Assigned_Per_Month Chart... Please wait ⏳"):
            trends_in_calls_assigned_per_month(df1)
        # -------------------------------------------------------------------------------
        with st.spinner("Loading Calls_Closed_by_SLA_Compliance Chart... Please wait ⏳"):
            calls_closed_by_sla_compliance()
        # --------------------------------------------------------------------------------------------
        with st.spinner("Loading Total Calls vs Average Call per Month Chart... Please wait ⏳"):
            approved_amount_distribution()
            total_calls_vs_average_call_per_month()
            # sla_violation_rate()
            # calls_closed_within_timeframes()
            treemap(HrData)

        # option = st.selectbox(
        #     label=  "select deshbord",
        #     options =  ["Engineer Performance Summary Dashboard",
        #                 "SLA Violation Dashboard",
        #                 "Geographic Performance Dashboard",
        #                 "Repeat & Franchise Call Analysis Dashboard"],
        # )
        # st.write("You selected:", option)


    ##################################
    # -----------------Main Page-------------
    ##################################3
    def sidebar():
        with st.sidebar:
            selected = option_menu(
                menu_title="Main Menu",
                options=["Home", "Reports"],
                icons=["house", "bi-file-earmark-bar-graph"],
                menu_icon="cast",
                default_index=0
            )
        if selected == "Home":
            home()
        if selected == "Reports":
            graphs()


    sidebar()
    ##################################
    # ------------------------------
    ##################################3
    # st.markdown(
    #         """
    #         <style>
    #             div[data-testid="stPlotlyChart"] {
    #                 border: 2px solid #000;  /* Black Border */
    #                 border-radius: 10px;  /* Rounded Corners */
    #                 padding: 15px;  /* Space Inside */
    #                 margin: auto;  /* Centering */
    #                 width: 100% !important;  /* Ensure it fits container */
    #                 max-width: 950px;  /* Limit max width */
    #                 background-color: #f9f9f9;  /* Light Gray Background */
    #                 box-shadow: 3px 3px 10px rgba(0, 0, 0, 0.15); /* Soft Shadow */
    #             }
    #
    #             /* Customize title font and alignment */
    #             h1, h2, h3 {
    #                 text-align: center;  /* Center Titles */
    #                 font-family: Arial, sans-serif;  /* Set Font */
    #                 color: #333333;  /* Dark Gray Text */
    #             }
    #
    #             /* Make axis labels bold */
    #             .xtick, .ytick {
    #                 font-weight: bold;
    #             }
    #         </style>
    #         """,
    #         unsafe_allow_html=True
    #     )

    # st.markdown("#### 🔝 Top 10 Engineers with Most Calls")
    # top_10_calls = df_filtered.nlargest(10, 'Total_Calls')  # Get top 10 engineers
    # fig_top_calls = px.bar(
    #     top_10_calls,
    #     x="Engineer",
    #     y="Total_Calls",
    #     color="Engineer",
    #     text_auto=True,
    #     title="Top 10 Engineers by Total Calls",
    # )
    # st.plotly_chart(fig_top_calls, use_container_width=True)
    #
    # st.markdown("#### 🔝Engineers with Most Calls")
    # top_10_calls = df_filtered.nlargest(10, 'Total_Calls')  # Get top 10 engineers
    # fig_top_calls = px.bar(
    #     df_filtered,
    #     x="Engineer",
    #     y="Total_Calls",
    #     color="Engineer",
    #     text_auto=True,
    #     title="Engineers by Total Calls",
    # )
    # st.plotly_chart(fig_top_calls, use_container_width=True)
    #
    #
    #
    # # Function to create bar chart
    # def create_bar_chart(data, title, color):
    #     fig = px.bar(
    #         data,
    #         x="Total_Calls",
    #         y="Engineer",
    #         orientation="h",
    #         title=title,
    #         color_discrete_sequence=[color],
    #         template="plotly_white",
    #     )
    #     return fig
    #
    # # ✅ Dropdown for Visual Selection
    # option = st.selectbox("📊 Select a Chart", ["Top Engineers", "Bottom Engineers", "Custom Chart"])
    #
    # # ✅ Show selected visual
    # if option == "Top Engineers":
    #     top5 = df.sort_values(by="Total_Calls", ascending=False).head(5)
    #     fig = create_bar_chart(top5, "TOP 5 ENGINEERS", "#fc2c03")
    # elif option == "Bottom Engineers":
    #     bottom5 = df.sort_values(by="Total_Calls", ascending=True).head(5)
    #     fig = create_bar_chart(bottom5, "BOTTOM 5 ENGdfINEERS", "#0083B8")
    # else:
    #     fig = create_bar_chart(df, "ALL ENGINEERS", "#999999")
    #
    # st.plotly_chart(fig, use_container_width=True)

