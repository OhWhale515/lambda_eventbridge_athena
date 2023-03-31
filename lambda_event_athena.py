import boto3
import awswrangler as wr
import uuid
import pandas as pd
import datetimey
DATABASE = 'mlops'
bucket = 's3://dailypay-mlops-model-output'
days = ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']
cols = ['integration_id','monday','tuesday','wednesday','thursday','friday','saturday','sunday','last_send']
def append_row(df, row):
    return pd.concat([
                df,
                pd.DataFrame([row], columns=row.index)]
           ).reset_index(drop=True)
def lambda_handler(event, context):
    # TODO : Only add needed partitions to decrease runtime
    query_final_state = wr.athena.repair_table(table='gross_earnings', database=DATABASE)
    print(query_final_state)
    query_events = "SELECT * FROM mlops.gross_earnings where cast(datetime as timestamp) >= current_date - interval '72' hour"
    df_events = wr.athena.read_sql_query(sql=query_events, database=DATABASE, ctas_approach=True)
    query_frequency = 'SELECT integration_id, max(monday) as monday, max(tuesday) as tuesday, max(wednesday) as wednesday, max(thursday) as thursday, max(friday) as friday, max(saturday) as saturday, max(sunday) as sunday, max(last_send) as last_send, max(datekey) as datekey, max(hour) as hour FROM mlops.gross_earnings_frequency group by integration_id'
    df_frequency = wr.athena.read_sql_query(sql=query_frequency, database=DATABASE, ctas_approach=True)
    new_df = pd.DataFrame(columns=cols)
    df_events=df_events.astype(str)
    df_frequency=df_frequency.astype(str)
    ids = list(df_frequency['integration_id'])
    new_ids = []
    for index, row in df_events.iterrows():
        new_df=new_df.astype(str)
        if(row['integration_id'] not in ids and row['integration_id'] not in new_ids):
            row_dict = {col:'' for col in cols}
            row_dict['integration_id'] = row['integration_id']
            row_dict['last_send'] = row['datetime']
            dt = datetime.datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M:%S')
            row_dict[days[dt.weekday()]] = dt.strftime('%H:%M:%S')
            row_dict['datekey'] = str(dt.date())
            row_dict['hour'] = str(dt.hour)
            new_row = pd.Series(row_dict)
            new_df = append_row(new_df, new_row)
            new_ids.append(row['integration_id'])
        elif(row['integration_id'] in new_ids):
            current_row = new_df[new_df["integration_id"] == row["integration_id"]].iloc[0]
            index = current_row.name
            if(datetime.datetime.strptime(current_row['last_send'], '%Y-%m-%d %H:%M:%S') < datetime.datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M:%S')):
                dt = datetime.datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M:%S')
                new_df.at[index, 'last_send'] = row['datetime']
                new_df.at[index, days[dt.weekday()]] = dt.strftime('%H:%M:%S')
                new_df.at[index, 'datekey'] = str(dt.date())
                new_df.at[index, 'hour'] = str(dt.hour)
        else:
            current_row = df_frequency[df_frequency["integration_id"] == row["integration_id"]].iloc[0]
            if(datetime.datetime.strptime(current_row['last_send'], '%Y-%m-%d %H:%M:%S') < datetime.datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M:%S')):
                dt = datetime.datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M:%S')
                current_row['last_send'] = row['datetime']
                current_row[days[dt.weekday()]] = dt.strftime('%H:%M:%S')
                current_row['datekey'] = str(dt.date())
                current_row['hour'] = str(dt.hour)
                new_df = append_row(new_df, current_row)
                new_ids.append(current_row['integration_id'])
    if(len(new_df) > 0):
        #print(new_df.iloc[0])
        pts = []
        groupby = new_df.groupby(['datekey', 'hour'])
        for name, group in groupby:
            print(name)
            pts.append(name)
            path = '/'.join([bucket, 'gross_earnings_frequency', 'datekey=' + str(name[0]), 'hour=' + str(name[1])])
            wr.s3.to_csv(
                df=group,
                path=path + '/' + str(uuid.uuid4()) + '.csv',
                index=False
            )
        wr.catalog.add_csv_partitions(
            database='mlops',
            table='gross_earnings_frequency',
            partitions_values={
                '/'.join([bucket,'gross_earnings_frequency','datekey=' + str(pt[0]), 'hour=' + str(pt[1]),'']): [str(pt[0]), str(pt[1])] for pt in pts
            }
        )
    else:
        print("No new data")