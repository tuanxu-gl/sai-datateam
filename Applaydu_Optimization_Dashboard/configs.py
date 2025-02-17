import datetime


#GENERAL CONFIGS
now = datetime.datetime.now()
date_today = ("%d-%.2d-%.2d %.2d-%.2d" % (now.year, now.month, now.day, now.hour, now.minute))
now_3 = now- datetime.timedelta(days=3)
now_4 = now- datetime.timedelta(days=4)
date_today_3 = datetime.date.today()- datetime.timedelta(days=3)
str_date_3 = ("%d%.2d%.2d" % (now_3.year, now_3.month, now_3.day))
str_date_4 = ("%d-%.2d-%.2d" % (now_4.year, now_4.month, now_4.day))

str_today = ("%d-%.2d-%.2d" % (now.year, now.month, now.day))

date_ranges = [
  #  {'start_date':'2020-08-10', 'end_date':str_date_4},
    {'start_date':'2025-01-01', 'end_date':'2025-01-31'},
]


dashboard_query_ids_live = [
    {'dashboard': 14, 'query': 789},
    {'dashboard': 14, 'query': 3183},
    {'dashboard': 292, 'query': 3852},
    {'dashboard': 14, 'query': 2899},
    {'dashboard': 319, 'query': 4266},
    {'dashboard': 319, 'query': 4251},
    {'dashboard': 14, 'query': 263},
    {'dashboard': 14, 'query': 3180},
    {'dashboard': 14, 'query': 256},
    {'dashboard': 14, 'query': 253},
    {'dashboard': 14, 'query': 251},
    {'dashboard': 14, 'query': 252},
    {'dashboard': 14, 'query': 916},
    {'dashboard': 14, 'query': 3326},
    {'dashboard': 319, 'query': 4253},
    {'dashboard': 292, 'query': 3480},
    {
        'dashboard': 294,
        'query': 3754,
        'kpi_name': 'Overall KPIs Eduland Kinderini',
        'columns': [
            ["Number of user launch Kinderini", 'number'],
            ["APD users", 'number'],
            ["Number of Kinderini sessions", 'number'],
            ["% APD users launch Kinderini", 'number'],
            ["avg_time_spent", 'number'],
            ["AVG Number of Kinderini sessions per user", 'number'],
            ["Average time spent per user in Kinderini", 'string'],
        ]
    },
    {
        'dashboard': 14,
        'query': 243,
        'kpi_name': 'Top 30 countries',
        'columns': [
            ["Country name", 'string'],
            ["Downloads", 'number'],
        ]
    },
    {
        'dashboard': 14,
        'query': 249,
        'kpi_name': 'Total Downloads',
        'columns': [
            ["Shop", 'string'],
            ["Total Installations", 'number'],
        ]
    },
    {
        'dashboard': 14,
        'query': 450,
        'kpi_name': 'Daily Downloads',
        'columns': [
            ["Date", 'string'],
            ["Daily Downloads", 'number'],
        ]
    },
    {
        'dashboard': 14,
        'query': 54,
        'kpi_name': 'Downloads per Country',
        'columns': [
            ["Country name", 'string'],
            ["Users", 'number'],
        ]
    },    
    {
        'dashboard': 14,
        'query': 873,
        'kpi_name': 'Number of scans v3',
        'columns': [
            ["Scan type", 'string'],
            ['Scans', 'number'],
        ]
    },
    {
        'dashboard': 14,
        'query': 245,
        'kpi_name': 'Physical toys brought to life - Time spent by users who have scanned surprises',
        'columns': [
            ["Time spent", 'string'],
           
        ]
    },  
    {
        'dashboard': 14,
        'query': 878,
        'kpi_name': 'Time spent per session by users who have scanned surprises',
        'columns': [
            ["sum_sessions_count", 'number'],
            ["sum_total_time_spent", 'number'],
            ["time_result", 'number'],
            ["Time spent", 'string'],
          
        ]
    },  
    {
        'dashboard': 14,
        'query': 244,
        'kpi_name': 'Physical toys brought to life - Users who have scanned surprises',
        'columns': [
            ["Users who have scanned surprises", 'number'],
          
        ]
    },
    {
        'dashboard': 14,
        'query': 258,
        'kpi_name': 'Average Surprises Scanned per User',
        'date_filter': 'server_date',
        'columns': [
         ["total_users", 'number'],
            ["sum_toy_unlocked_count", 'number'],
            ["sum_scan_mode_finished_count", 'number'],
            ["total_scans", 'number'],
               ["Average Toys Scanned per User", 'number'],
          
        ]
    },
    {
        'dashboard': 14,
        'query': 255,
        'kpi_name': 'Average Time per Session',
        'date_filter': 'client_time',
        'columns': [
         ["Total time spent", 'number'],
            ["Total Session", 'number'],
            ["time_result", 'number'],
            ["Average Time per Users", 'string'],
              
          
        ]
    },
    {
        'dashboard': 14,
        'query': 246,
        'kpi_name': 'Physical toys brought to life - Time spent by users who haven t scanned surprises',
        'date_filter': 'server_date',
        'columns': [
         ["users", 'number'],
            ["sum_total_time_spent", 'number'],
            ["time_result", 'number'],
            ["Time spent", 'string'],
              
          
        ]
    },
    {
        'dashboard': 14,
        'query': 879,
        'kpi_name': 'Time spent per session by users who havent scanned surprises',
        'date_filter': 'server_date',
        'columns': [
         ["sum_sessions_count", 'number'],
            ["sum_total_time_spent", 'number'],
            ["time_result", 'number'],
            ["Time spent", 'string'],
              
          
        ]
    },
    {
        'dashboard': 14,
        'query': 2826,
        'kpi_name': 'Total sessions and sessions per user by game',
        'date_filter': 'client_time',
        'columns': [
         ["Minigame", 'string'],
            ["Users count", 'number'],
            ["Sessions", 'number'],
            ["Sessions per user", 'number'],
           ["total_time_spent", 'number'],
           ["Average time spent per game per user (minute)", 'number'],
           ["Average time spent per game per user (min - sec)", 'string'],
              
          
        ]
    },
    {
        'dashboard': 14,
        'query': 451,
        'kpi_name': 'Average time spent per game per user',
        'date_filter': 'client_time',
        'columns': [
         ["Minigame", 'string'],
          ["Users count", 'number'],
            ["Sessions", 'number'],
            ["Sessions per user", 'number'],
 ["total_time_spent", 'number'],
         ["Average time spent per game per user (minute)", 'number'],
         ["Average time spent per game per session (minute)", 'number'],
         ["Average time spent per game per user (min - sec)", 'string'],
         ["Average time spent per game per session (min - sec)", 'string'],
          
        ]
    },
    {
        'dashboard': 14,
        'query': 3064,
        'kpi_name': 'Users play at least 2 times by Mini Game',
        'date_filter': 'client_time',
        'columns': [
         ["minigame", 'string'],
          ["Users replay", 'number'],
            ["Total users", 'number'],
            ["percentage", 'number'],
 
          
        ]
    },
    {
        'dashboard': 14,
        'query': 3144,
        'kpi_name': 'Percentage of Applaydu users played each Dedicated Experience',
        'date_filter': 'client_time',
        'columns': [
         ["Environment", 'string'],
          ["Percentage", 'number'],
           
 
          
        ]
    },
    {
        'dashboard': 14,
        'query': 3142,
        'kpi_name': 'Time spent per user and Time spent per session by Dedicated Experience',
        'date_filter': 'client_time',
        'columns': [
         ["environment", 'string'],
        ["No of Session", 'number'],
        ["No of Users", 'number'],
        ["Sessions per user", 'number'],
        ["Time spent per user (min)", 'number'],

        ["Time spent per session (min)", 'number'],

        ["Time spent per user (min - sec)", 'string'],
        ["Time spent per session (min - sec)", 'string'],
       
        ]
    },
    {
        'dashboard': 14,
        'query': 3148,
        'kpi_name': 'Users play at least 2 times  by each Dedicated Experience',
        'date_filter': 'client_time',
        'columns': [
         ["environment", 'string'],
          ["Users replay", 'number'],
            ["Total users", 'number'],
            ["percentage", 'number'],
 
          
        ]
    },
    {
        'dashboard': 14,
        'query': 874,
        'kpi_name': 'Total Scans (QR + Vignettes) splitted between Mainstream vs. Licensing toys v3',
        'date_filter': 'server_date',
        'columns': [
         ["Category", 'string'],
          ["Total Scans", 'number'],
          
 
          
        ]
    },
    {
        'dashboard': 14,
        'query': 177,
        'kpi_name': 'Total toy unlocked by Scan Toy and QR/Leaflet',
        'date_filter': 'server_date',
        'columns': [
         ["Toy name", 'string'],
          ["Scan Toy", 'number'],
          ["Scan QR/Leaflet", 'number'],
          ["Total scan", 'number'],
          
          
 
          
        ]
    },
    {
        'dashboard': 14,
        'query': 290,
        'kpi_name': 'Total toy unlocked by Scan Toy and QR/Leaflet',
        'date_filter': 'server_date',
        'columns': [
         ["Week", 'string'],
          ["Users who have scanned surprises", 'number'],
          ["total_users", 'number'],
          ["Rate Users scanned the toys/total", 'number'],
          
        ]
    },
    {
        'dashboard': 14,
        'query': 875,
        'kpi_name': 'Number of scans v3 - by leftover toys',
        'date_filter': 'server_date',
        'columns': [
         ["Leftover type", 'string'],
          ['total_scans', 'number'],       
        ]
    },
    {
        'dashboard': 14,
        'query': 355,
        'kpi_name': 'Total Scans (QR + Vignettes) Licensing toys by surprise family',
        'date_filter': 'server_date',
        'columns': [
         ["Surprise family" , 'string'],
          ["Total Scans" , 'number'],       
        ]
    },
    {
        'dashboard': 14,
        'query': 1322,
        'kpi_name': 'Number of scans v3 - daily',
        'date_filter': 'server_date',
        'columns': [
         ["Server date" , 'string'],
          ["Total Scans" , 'number'],       
        ]
    },
	{
        'dashboard': 14,
        'query': 4413,
        'kpi_name': 'P3 MAU by inflow source',
        'date_filter': 'client_time',
        'columns': [
         ["grass_month" , 'string'],
          ["starting_persona" , 'string'],       
          ["persona" , 'string'],       
          ["user_cnt" , 'number'],       
        ]
    },
    ]

dashboard_query_ids_beta =[
 
    {
        'dashboard': 14,
        'query': 4413,
        'kpi_name': 'P3 MAU by inflow source',
        'date_filter': 'client_time',
        'columns': [
         ["grass_month" , 'string'],
          ["starting_persona" , 'string'],       
          ["persona" , 'string'],       
          ["user_cnt" , 'number'],       
        ]
    },
   
]
dashboard_query_ids_live_gb =[
    {
        'dashboard': 319,
        'query': 4226,
        'kpi_name':'Total Downloads',
        'date_filter': 'client_time',
        'columns': [
         ["Shop", 'string'],
        ["Total Installations", 'number'],       
        ]
    },
   {
        'dashboard': 319,
        'query': 4238,
        'kpi_name': 'Average Surprises Scanned per User',
        'date_filter': 'server_date',
        'columns': [
         ["total_users", 'number'],
            ["sum_toy_unlocked_count", 'number'],
            ["sum_scan_mode_finished_count", 'number'],
            ["total_scans", 'number'],
               ["Average Toys Scanned per User", 'number'],
          
        ]
    },
   {
        'dashboard': 319,
        'query': 4248,
        'kpi_name': 'Number of Sessions',
        'date_filter': 'server_time',
        'columns': [
         ["Number of Sessions", 'number'],
            ["Total_Users", 'number'],
            ["Average Session per User", 'number'],
           
          
        ]
    },
       {
        'dashboard': 319,
        'query': 4259,
        'kpi_name': 'Number of Sessions',
        'date_filter': 'server_time',
        'columns': [
         ["Number of Sessions", 'int'],
            ["Total_Users", 'int'],
            ["Average Session per User", 'float'],
           
          
        ]
    },
    {
        'dashboard': 319,
        'query': 4232,
        'kpi_name': 'Number of Users - by Shop',
        'date_filter': 'active_date',
         'columns': [
         ["Shop", 'string'],
            ["Total_Users", 'int'],
         
          
        ]
    },
     {
        'dashboard': 319,
        'query': 4237,
        'kpi_name': 'Average Time per Session',
        'date_filter': 'client_time',
        'columns': [
         ["Total time spent", 'int'],
            ["Total Session", 'int'],
            ["time_result", 'int'],
            ["Average Time per Users", 'string'],
         
          
        ]
    },
    {
        'dashboard': 319,
        'query': 4243,
        'kpi_name': 'Time spent per session by users who havent scanned surprises',
        'date_filter': 'server_date',
        'columns': [
         ["sum_sessions_count", 'int'],
            ["sum_total_time_spent", 'int'],
            ["time_result", 'float'],
            ["Time spent", 'string'],
              
          
        ]
    },
    {
        'dashboard': 319,
        'query': 4255,
        'kpi_name': 'Time spent per session by users who have scanned surprises',
        'date_filter': 'server_date',
        'columns': [
            ["sum_sessions_count", 'int'],
            ["sum_total_time_spent", 'int'],
            ["time_result", 'float'],
            ["Time spent", 'string'],
          
        ]
    }, 
     {
        'dashboard': 319,
        'query': 4240,
        'kpi_name': 'Average Time Spent per User',
        'date_filter': 'client_time',
        'columns': [
            ["Total time spent", 'int'],
            ["Total Users", 'int'],
            ["time_result", 'float'],
            ["Average Time per Users", 'string'],
          
        ]
    }, 
    {
        'dashboard': 319,
        'query': 4233,
        'kpi_name': 'Physical toys brought to life - Time spent by users who have scanned surprises',
        'date_filter': 'server_date',
        'columns': [
            ["Time spent", 'string'],
           
        ]
    },
    {
        'dashboard': 319,
        'query': 4256,
        'kpi_name': 'Physical toys brought to life - Time spent by users who havent scanned surprises',
        'date_filter': 'server_date',
        'columns': [
         ["users", 'int'],
            ["sum_total_time_spent", 'int'],
            ["time_result", 'float'],
            ["Time spent", 'string'],
              
          
        ]
    },
    {
        'dashboard': 319,
        'query': 4228,
        'kpi_name': 'One and Done Ratio',
        'date_filter': 'client_time',
        'columns': [
         ["One and Done", 'float'],
          
          
        ]
    },
    {
        'dashboard': 319,
        'query': 4270,
        'kpi_name': 'Funnel v4 - Scan users',
        'date_filter': 'client_time',
        'columns': [
         ["Users", 'string'],
         ["Users each step", 'int'],
          
          
        ]
    },
    {
        'dashboard': 319,
        'query': 4265,
        'kpi_name': 'Funnel v4 - Free users',
        'date_filter': 'client_time',
        'columns': [
         ["Users", 'string'],
         ["Users each step", 'int'],
          
          
        ]
    },
    {
        'dashboard': 319,
        'query': 4258,
        'kpi_name': 'Funnel v4 - Deeplink users',
        'date_filter': 'client_time',
        'columns': [
         ["Users", 'string'],
         ["Users each step", 'int'],
          
          
        ]
    },
    {
        'dashboard': 319,
        'query': 4229,
        'kpi_name': 'ENGAGE | FOCUS - Users & Scan per season',
        'date_filter': 'server_date',
        'columns': [
         ["Season", 'string'],
         ["Users who have scanned surprises", 'int'],
         ["sum_toy_unlocked_count", 'int'],
         ["sum_scan_mode_finished_count", 'int'],
         ["Total Scans", 'int'],
         ["Average Toys Scanned per User", 'float'],
          
          
        ]
    },
   {
        'dashboard': 319,
        'query': 4227,
        'kpi_name': 'Weekly report - Total Scan (including deeplink)',
        'date_filter': 'server_date',
        'columns': [
         ["Scan type", 'string'],
         ["total_scan", 'int'],
        ]
    },
    {
        'dashboard': 319,
        'query': 4268,
        'kpi_name': 'Number of mau, scans & ratio',
        'date_filter': 'active_date',
        'columns': [
         ["year", 'int'],
         ["month", 'int'],
         ["Time", 'string'],
         ["Users", 'int'],
         ["Scanned Users", 'int'],
         ["Scan users ratio", 'float'],
         ["Average Time per Sessions", 'string'],
         ["Average Time per scanned user", 'string'],
         ["Average Time per NOT scanned user", 'string'],
        ]
    },
    {
        'dashboard': 319,
        'query': 4247,
        'kpi_name': 'Total Number of Users (NEW)',
        'date_filter': 'client_time',
        'columns': [
         ["Persona_Type", 'string'],
         ["Total Users", 'int'],
        
        ]
    },
    {
        'dashboard': 319,
        'query': 4234,
        'kpi_name': 'Personas distribution by Country',
        'date_filter': 'client_time',
        'columns': [
         ["country_name", 'string'],
         ["Persona #1", 'int'],
         ["Persona #2", 'int'],
         ["Persona #3", 'int'],
         ["Active Users", 'int'],
         ["% Persona 1", 'float'],
         ["% Persona 2", 'float'],
         ["% Persona 3", 'float'],
        
        
        ]
    },
    {
        'dashboard': 319,
        'query': 4260,
        'kpi_name': 'ENGAGE | PERSONA REPARTITION',
        'date_filter': 'client_time',
        'columns': [
         ["year", 'int'],
         ["month", 'int'],
         ["Time", 'string'],
         ["Persona_Type", 'string'],
         ["Total Users", 'int'],
        ]
    },
    {
        'dashboard': 319,
        'query': 4249,
        'kpi_name': 'Monthly Active Users (MAU)',
        'date_filter': 'client_time',
        'columns': [
         ["Month", 'int'],
         ["Year", 'int'],
         ["Time", 'string'],
         ["Monthly Active Users", 'int'],
         
        ]
    },
    {
        'dashboard': 319,
        'query': 4274,
        'kpi_name': 'ENGAGE | EVOLUTION D1/7/28',
        'date_filter': 'client_time',
        'columns': [
         ["Month", 'string'],
         ["Retention D1", 'float'],
         ["Retention D7", 'float'],
         ["Retention D28", 'float'],
         
        ]
    },{
        'dashboard': 319,
        'query': 4254,
        'kpi_name': 'ENGAGE | EVOLUTION D1/7/28 Keys Countries',
        'date_filter': 'client_time',
        'columns': [
         ["Month", 'string'],
         ["Retention D1", 'string'],
         ["Retention D3", 'string'],
         ["Retention D7", 'string'],
         ["Retention D14", 'string'],
         ["Retention D28", 'string'],
         ["Retention D30", 'string'],
         ["D7 per D1", 'string'],
       
        ]
    },
    {
        'dashboard': 319,
        'query': 4242,
        'kpi_name': 'ENGAGE | EVOLUTION D1/7/28  Only P2/P3-Keys Countries',
        'date_filter': 'client_time',
        'columns': [
         ["Month", 'string'],
         ["Retention D1", 'string'],
         ["Retention D3", 'string'],
         ["Retention D7", 'string'],
         ["Retention D14", 'string'],
         ["Retention D28", 'string'],
         ["Retention D30", 'string'],
         ["D7 per D1", 'string'],
       
        ]
    },
    {
        'dashboard': 319,
        'query': 4276,
        'kpi_name': '[Scan Users] Monthly [D1, D7, D28] Retention',
        'date_filter': 'client_time',
        'columns': [
         ["month", 'string'],
         ["D0", 'int'],
         ["D1", 'float'],
            ["D3", 'float'],
            ["D7", 'float'],
            ["D28", 'float'],
           
       
        ]
    },
    {
        'dashboard': 319,
        'query': 4252,
        'kpi_name': 'Monthly [D1, D7, D28] Retention',
        'date_filter': 'client_time',
        'columns': [
         ["month", 'string'],
         ["D0", 'int'],
         ["D1", 'float'],
            ["D3", 'float'],
            ["D7", 'float'],
            ["D28", 'float'],
           
       
        ]
    },{
        'dashboard': 319,
        'query': 4241,
        'kpi_name': 'Total Scans split by Scan Deeplink, Leaflet and Toy- by country',
        'date_filter': 'server_date',
        'columns': [
         ["country_name", 'string'],
         ["Scan Toy", 'int'],
         ["Scan Leaflet", 'int'],
            ["Scan Deep Link", 'int'],
            ["Total scans", 'int'],
           
       
        ]
    },
    {
            'dashboard': 319,
        'query': 4230,
        'kpi_name': 'Sessions and Sessions per User by Feature',
        'date_filter': 'client_time',
        'columns': [
         ["feature", 'string'],
         ["session", 'int'],
         ["Sessions per user", 'float'],
            ["Time spent per user min", 'float'],
            ["Session Duration", 'float'],
            ["Time spent per user min - sec", 'string'],
           ["Session Duration min", 'string'],
           
       
        ]
    },    
    {
        'dashboard': 319,
        'query': 4267,
        'kpi_name': 'Average time spent per game per user',
        'date_filter': 'client_time',
        'columns': [
         ["Minigame", 'string'],
         ["Users count", 'int'],
         ["Sessions", 'int'],
         ["Sessions per user", 'float'],
         ["total_time_spent", 'int'],
         ["Average time spent per game per user minute", 'float'],
         ["Average time spent per game per session minute", 'float'],
        ]
    },
    {
        'dashboard': 319,
        'query': 4264,
        'kpi_name': 'Time spent per user and Time spent per session by Dedicated Experience',
        'date_filter': 'client_time',
        'columns': [
         ["environment", 'string'],
        ["No of Session", 'int'],
        ["No of Users", 'int'],
        ["Sessions per user", 'float'],
        ["Time spent per user in min", 'float'],

        ["Time spent per session in min", 'float'],

      # ["Time spent per user (min - sec)", 'string'],
      #  ["Time spent per session (min - sec)", 'string'],
       
        ]
    },{
        'dashboard': 319,
        'query': 4271,
        'kpi_name': '[PARENTAL] Email registration funnel',
        'date_filter': 'client_time',
        'columns': [
         ["Users", 'string'],
         ["Number of Users", 'int'],
        
        ]
    },{
        'dashboard': 319,
        'query': 4263,
        'kpi_name': '[PARENTAL] Email registration funnel - monthly',
        'date_filter': 'client_time',
        'columns': [
         ["month", 'string'],
         ["Successfully Registered Email", 'int'],
         ["Verified email after registration", 'int'],
         ["ratio", 'float'],
        
        ]
    },{
        'dashboard': 319,
        'query': 4236,
        'kpi_name': '[PARENTAL] Email registration funnel - by country',
        'date_filter': 'client_time',
        'columns': [
         ["year_month", 'string'],
         ["Country", 'string'],
         ["Successfully Registered Email", 'int'],
         ["Verified email after registration", 'int'],
         ["ratio", 'float'],
        
        ]
    },
    {
            'dashboard': 319,
        'query': 4235,
        'kpi_name': 'Time spent per User and per Session by Feature',
        'date_filter': 'client_time',
        'columns': [
         ["feature", 'string'],
         ["session", 'int'],
         ["Sessions per user", 'float'],
            ["Time spent per user min", 'float'],
            ["Session Duration", 'float'],
            ["Time spent per user min - sec", 'string'],
           ["Session Duration min", 'string'],
           
       
        ]
    },{
            'dashboard': 319,
        'query': 4261,
        'kpi_name': 'Weekly Active Users',
        'date_filter': 'client_time',
        'columns': [
         ["Start of Week", 'string'],
         ["Active Users", 'int'], 
        ]
    },{
            'dashboard': 319,
        'query': 4275,
        'kpi_name': 'Total number of scan',
        'date_filter': 'client_time',
        'columns': [
         ["Total Scans", 'int'], 
        ]
    },{
            'dashboard': 319,
        'query': 4246,
        'kpi_name': 'Total Scan Users',
        'date_filter': 'client_time',
        'columns': [
         ["Total Scan Users", 'int'], 
        ]
    },{
            'dashboard': 319,
        'query': 4257,
        'kpi_name': 'Total Scan Users', #need to change
        'date_filter': 'client_time',
        'columns': [
         ["Month", 'int'], 
        ["Year", 'int'], 
        ["Time", 'string'], 
        ["Users", 'int'], 
        ]
    },{
            'dashboard': 319,
        'query': 4239,
        'kpi_name': '[Lets Story] Scan Users that created the story',
        'date_filter': 'client_time',
        'columns': [
        
        ["Users", 'int'], 
        ]
    },{
            'dashboard': 319,
        'query': 4231,
        'kpi_name': '[Lets Story] Scan Users that created the story',
        'date_filter': 'client_time',
        'columns': [
        
        ["User Type", 'string'], 
       ["users", 'int'], 
        ]
    },{
            'dashboard': 319,
        'query': 4273,
        'kpi_name': 'Number of stories created by language of Scan User',
        'date_filter': 'client_time',
        'columns': [
        
        ["Language", 'string'], 
       ["Stories", 'int'], 
        ]
    },{
            'dashboard': 319,
        'query': 4277,
        'kpi_name': 'Total Scan in Kinderini',
        'date_filter': 'client_time',
        'columns': [
        
        ["Biscuits Scanned", 'int'], 
      
        ]
    },{
            'dashboard': 319,
        'query': 4272,
        'kpi_name': 'Total users scan biscuit',
        'date_filter': 'client_time',
        'columns': [
        
        ["Total Scans Users", 'int'], 
      
        ]
    },{
            'dashboard': 319,
        'query': 4262,
        'kpi_name': 'Avg. minutes spent inside full experience (story) for scan users',
        'date_filter': 'client_time',
        'columns': [
        
        ["avg_time_story_exp", 'float'], 
        ["Average time full story experience", 'string'],
        ]
    }, {
            'dashboard': 319,
        'query': 4269,
        'kpi_name': 'Funnel Story mode for scan only',
        'date_filter': 'client_time',
        'columns': [
        
        ["#", 'string'], 
        ["Users", 'int'],
        ]
    },{
            'dashboard': 319,
        'query': 4245,
        'kpi_name': 'Funnel Story mode for scan only',
        'date_filter': 'client_time',
        'columns': [
        
        ["#", 'string'], 
        ["Users", 'int'],
        ]
    }, {
            'dashboard': 319,
        'query': 4250,
        'kpi_name': 'Engagement KPIs:  Returning Users per New Users',
        'date_filter': 'client_time',
        'columns': [
        
        ["kpi", 'string'], 
        ["value", 'string'],
        ]
    },
    ]
dashboard_query_ids_beta_gb =[
   {
            'dashboard': 319,
        'query': 4250,
        'kpi_name': 'Engagement KPIs:  Returning Users per New Users',
        'date_filter': 'client_time',
        'columns': [
        
        ["kpi", 'string'], 
        ["value", 'string'],
        ]
    },
    
 
   
   
]

variables_keep_original = ['Shop','avg_time_spent','Week','sum_sessions_count','sum_total_time_spent','time_result','total_time_spent'
                           ,'sum_toy_unlocked_count','sum_scan_mode_finished_count','total_scans','Total_Users','total_users','users','minigame','environment','percentage',
                           'grass_month','starting_persona','persona','user_cnt','year','month','country_name','feature','session','ratio']

dashboards_supported_sf = [14,292,319,294]
dashboards_supported_gb = [319]
dashboards_supported = dashboards_supported_gb

overide_old_data = False
dashboard_query_ids = dashboard_query_ids_beta_gb
SQLs_Path = './scripts/Metabase_Optimize_SQLs_gb/'










user        = "SAI_METABASE"
password    = "BWQ3n8wgMXgYr4s"
account     = "gameloft.eu-west-1"
db          = "ELEPHANT_DB"
importWarehouse = "GAME_TEAMS"
schema      = "APPLAYDU"
store_table      = "STORE_STATS"

