# Import modules
import numpy as np
import pandas as pd
import math


# Load file into pandas
# Input starts at row 2
mkt_lib = pd.read_csv("/Users/liyan.wang/Desktop/Hackathon/Sample Showpad Report Exports/library-marketing-content showpad-export-20190709.csv", header=0)
user_act = pd.read_csv("/Users/liyan.wang/Desktop/Hackathon/Sample Showpad Report Exports/user-activity export.csv", header=0)
top_mkt = pd.read_csv("/Users/liyan.wang/Desktop/Hackathon/Sample Showpad Report Exports/top-content-marketing.csv", header=0)


# UTILITIES

# Remove rows and reorganize three tables
def data_cleaning(df1, df2, df3):
    # library marketing content export
    df1['uploaded date'] = pd.to_datetime(df1['uploaded date'])
    mkt_lib_file = pd.DataFrame(df1.groupby(by='asset name').sum())
    mkt_lib_file_copy = mkt_lib_file[['(page)views', 'likes']]
    date_created = df1[['asset name','uploaded date']].drop_duplicates().groupby(by='asset name').max()
    lifetime_engagement = pd.concat([mkt_lib_file_copy, date_created], axis=1)

    # user activity export
    user_act_copy = df2.dropna(subset = ['File Name'])
    user_act_copy.Date = pd.to_datetime(user_act_copy['Date'])
    user_act_file = pd.DataFrame(user_act_copy.groupby(by='File Name').agg('sum'))
    date_last_activity = user_act_copy[['File Name','Date']].drop_duplicates().groupby(by='File Name').max()
    recent_engagement = pd.concat([user_act_file, date_last_activity], axis=1)

    # top content marketing export
    top_mkt_file = pd.DataFrame(df3.groupby(by='Display name').sum())
    top_mkt_copy = top_mkt_file[['Shares', 'Social shares']]

    return recent_engagement, lifetime_engagement, top_mkt_copy


def merge_tables(df1, df2, df3): 
    recent_engagement, lifetime_engagement, top_mkt_copy = data_cleaning(df1, df2, df3)
    table = pd.concat([recent_engagement, lifetime_engagement, top_mkt_copy], axis=1).reset_index()
    table['file_type'] = parse_file_type(table)
    table = table[['index', 'file_type', 'uploaded date', 'Date', 'Number of File Views', 'Number of File Downloads', '(page)views','likes', 'Shares', 'Social shares']]
    table.columns = ['asset_name', 'file_type', 'date_created', 'date_last_activity', 'views_recent', 'downloads_recent', 'views_lifetime', 'likes_lifetime', 'shares', 'social_shares']

    return table


# Parse file type out of asset name then
# extract the last string of content name for asset
def parse_file_type(df):
    asset = pd.Series(np.array(df['index'])).str.split(pat=".")
    file_type = [asset[i][-1] for i in range(len(asset))]

    return file_type


# Create boolean variable indicating data scource
def create_boolean(data, variable):
    boo = data[variable]

    for i in range (len(boo)):
        if pd.isnull(boo[i]):
            boo[i] = '0'
        else:
            boo[i] = '1'

    return boo


def combine_boolean(df):
    three_measures = df[['views_recent','views_lifetime', 'shares']]
    from_usr_act = create_boolean(three_measures, 'views_recent')
    from_mkt_lib = create_boolean(three_measures, 'views_lifetime')
    from_top_mkt = create_boolean(three_measures, 'shares')
    combined_measures = pd.concat([from_usr_act, from_mkt_lib, from_top_mkt], axis=1).astype(int)

    return combined_measures


# Create string variable indicating data scource
def create_source(df):
    measurement = np.array(df)
    source_index = []

    for i in range (len(measurement)):
        list = []
        for j in range (3):
            if measurement[i][j] == 1:
                list.append(j)
        source_index.append(list)

    source = source_index

    for i in range (len(source)):
        for j in range(len(source[i])):
            if source[i][j] == 0:
                source[i][j] = 'user activity'
            elif source[i][j] == 1:
                source[i][j] = 'library marketing'
            elif source[i][j] == 2:
                source[i][j] = 'top marketing'

    output = pd.Series(source).str.join(', ')

    return output


# Compute 3-quantile of each measurement then assign score
def compute_quantile(df, variable):
    # Remove NaN temporarily for computing quantile
    complete = df[['views_recent', 'views_lifetime', 'shares']].dropna()
    tmp = np.array(complete[variable].sort_values())
    lv1_len = math.ceil(1/3 * len(tmp))
    lv2_len = math.ceil(2/3 * len(tmp))
    level1 = tmp[lv1_len]
    level2 = tmp[lv2_len]

    return level1, level2


# Impute missing values with 0
def impute_missing(df, variable):
    three_impute = df[['views_recent','views_lifetime', 'shares']]
    value = three_impute[variable].fillna(0)

    return value


# Assign score to each measurement
def score_convert(data, level_1, level_2):
    val = data

    for i in range (len(val)):
        if val[i] <= level_1:
            val[i] = '1'
        elif level_1 < val[i] <= level_2:
            val[i] = '2'
        else:
            val[i] = '3'

    return val


# total = 1/3(recent_view) + 1/3(lifetime_view) + 1/3(shares)
def compute_total_score(df):
    s = np.array(df)
    n = 0

    for i in range (len(s)):
        for j in range (3):
            n += s[i][j]
        s[i] = n * 1/3
        n = 0

    total = [s[i][0] for i in range(len(s))]

    return total


def generate_score(df):
    view_recent_lv1, view_recent_lv2 = compute_quantile(df, 'views_recent')
    view_lifetime_lv1, view_lifetime_lv2 = compute_quantile(df, 'views_lifetime')
    shares_lv1, shares_lv2 = compute_quantile(df, 'shares')

    views_recent_impute = impute_missing(df, 'views_recent')
    views_lifetime_impute = impute_missing(df, 'views_lifetime')
    shares_impute = impute_missing(df, 'shares')

    recent_view_score = score_convert(views_recent_impute, view_recent_lv1, view_recent_lv2)
    lifetime_view_score = score_convert(views_lifetime_impute, view_lifetime_lv1, view_lifetime_lv2)
    share_score = score_convert(shares_impute, shares_lv1, shares_lv2)

    score = pd.concat([recent_view_score, lifetime_view_score, share_score], axis=1).astype(float)
    total = compute_total_score(score)

    return total


def final_merge(df1, df2):
    # Merge combined with boolean source, string source and total score
    combined_boolean = pd.concat([df1, df2], axis=1)
    new_combined = combined_boolean
    new_combined['data_source'] = create_source(df2)
    new_combined['total_score'] = generate_score(df1)
    new_combined = new_combined.set_index('asset_name')

    return new_combined


combined = merge_tables(mkt_lib, user_act, top_mkt)
boolean_ds = combine_boolean(combined).rename(columns={"views_recent": "from_usr_act", "views_lifetime": "from_mkt_lib", "shares": "from_top_mkt"})
merged_engagement = final_merge(combined, boolean_ds)

export = merged_engagement.to_json(r'Merged_Engagement_Table.json', orient='index')