# Import libraries
import polars as pl
from glob import glob


# create folder to set a file path
folder_path = r'C:\Users\rites\OneDrive\raw'

# enter all files
file_names = [
    "vendor.csv",
    "taxi_zone.csv",
    "trip_type.tsv",
    "payment_type.json",
    "rate_code.json"
    ]

# Function that reads files and detects format
def smart_read_files(folder_path, file_list):
    datasets = {}
    for file in file_list:
        full_path = f"{folder_path}\\{file}"
        dataset_name = file.rsplit('.', 1)[0]
        # Handle file type by extension
        if file.endswith(".csv"):
            datasets[dataset_name] = pl.read_csv(full_path)
        elif file.endswith(".tsv"):
            datasets[dataset_name] = pl.read_csv(full_path, separator="\t", quote_char=None)
        elif file.endswith(".json"):
            datasets[dataset_name] = pl.read_json(full_path)
        elif file.endswith(".json"):
            datasets[dataset_name] = pl.read_json(full_path)

        else:
            print(f"Unsupported file type: {file}")
    return datasets

# Read all datasets
all_data = smart_read_files(folder_path, file_names)

# Access all  datasets
ven = all_data.get("vendor")
taxi = all_data.get("taxi_zone")
tripty = all_data.get("trip_type")
pay=all_data.get("payment_type")
ratec=all_data.get("rate_code")

# using a if elif conditional statement to check the all datasets some columns and rows?
if ven is not None:
    print(ven.head())
elif taxi is not None:
    print(taxi.glimpse(return_as_string=True))
elif tripty is not None:
    print(tripty.schema)
elif pay is not None:
    print(pay.head())
elif ratec is not None:
    print(ratec.head())

#  trip data green
tripdt = glob(r'C:\Users\rites\OneDrive\raw\trip_data_green_parquet\**\*.parquet', recursive=True)
all_dfs = []
for file in tripdt:
    print(f"Reading: {file}")
    df = pl.read_parquet(file, low_memory=False)
    all_dfs.append(df)
df1 = pl.concat(all_dfs)
print("Total files read:", len(tripdt))
print("Total rows:", len(df1))
print(df1.columns)
print(df1.head())
print(df1.schema)

# Define a dictionary of renaming a required columns for each dataset
rename_rules = {
    "vendor": {
        "vendor_id": "VendorID"
    },
    "taxi_zone": {
        "LocationID": "DOLocationID"
    },
    "rate_code": {
        "rate_code_id": "RatecodeID"
    },
    "trip_type": {
        '"trip_type"': "trip_type",
        'trip_type_desc"': "trip_type_desc"
    }}
#  using a Function to strip column names and apply renaming columns
def rename_columns(df, dataset_name) :
    df = df.rename({col: col.strip() for col in df.columns})  # Strip whitespace from all column names
    if dataset_name in rename_rules:
        df = df.rename(rename_rules[dataset_name])
    return df

# Apply renaming to each dataset after loading
ven = rename_columns(ven,"vendor")

taxi = rename_columns(taxi,"taxi_zone")
ratec = rename_columns(ratec,"rate_code")
tripty = rename_columns(tripty,"trip_type")

#show all datasets after remaning a columns
print(ven)
print(taxi)
print(ratec)
print(tripty)

# create a  new dataframe 
nr = pl.DataFrame({
    "DOLocationID": [0],
    "Borough": ["Noborough"],
    "Zone": ["Unknown"],
    "service_zone": ["Unknown"]
})

# concat  taxi with nr
taxi = pl.concat([taxi, nr])
print(taxi.tail())

# # Create a new row as a DataFrame
nrow = pl.DataFrame({
    "trip_type": [0],
    "trip_type_desc": ["unknown"]
})

# Append it to the original ven DataFrame
tripty = pl.concat([tripty,nrow])
print(tripty)


#clean df1 datasets
#remove duplicates values
counts = df1.group_by(df1.columns).count()

# Keep rows that appear only once
df1= counts.filter(pl.col("count") == 1).select(df1.columns)
print(df1)

#check the duplicates values
print(df1.filter(df1.is_duplicated())) 
print(df1.shape)

# Extract pickup year from datetime
df1 = df1.with_columns(
    pl.col("lpep_pickup_datetime").dt.year().alias("pickup_year")
)

# Drop rows where pickup_year is 2008, 2009, 2010, 2019, or 2041
df1 = df1.filter(~pl.col("pickup_year").is_in([2008, 2009, 2010, 2019, 2041]))

# Drop rows where fare_amount is less than 0
df1 = df1.filter(pl.col("fare_amount") >= 0)
taxi=taxi.filter(pl.col("Borough")!="Unknown")

# Create a new row as a DataFrame
new_row = pl.DataFrame({
    "VendorID": [0],
    "vendor_name": ["unknown"]
})

# Append it to the original ven DataFrame
ven = pl.concat([ven, new_row])
print(ven)

# check null values in all datasets?
def check_nulls(datasets: dict):
    for name, df in datasets.items():
        if df is not None:
            print(f"\n Null count in '{name}':")
            print(df.null_count())
        else:
            print(f"\n[Warning] Dataset '{name}' is None.")

datasets_to_check = {
    "vendor": ven,
    "taxi_zone": taxi,
    "trip_type": tripty,
    "rate_code": ratec,
    "payment_type": pay,
    "trip_data": df1
}
print(check_nulls(datasets_to_check))

# #fill all missing values in df1 dataset
def fill_final_df_nulls(df,col_name,fill_value) :
    return df.with_columns([pl.col(col_name).fill_null(fill_value)])
df1=fill_final_df_nulls(df1,"VendorID",0)
df1=fill_final_df_nulls(df1,"store_and_fwd_flag","blank")
df1=fill_final_df_nulls(df1,"RatecodeID",0)
df1=fill_final_df_nulls(df1,"passenger_count",0)
df1=fill_final_df_nulls(df1,"payment_type",5)
df1=fill_final_df_nulls(df1,"trip_type",0)
df1=fill_final_df_nulls(df1,"congestion_surcharge",0)
print(df1)

#drop a "ehail_fee" column in df1.
df1=df1.drop("ehail_fee")

# #join all datasets.
def join_all_data(dname1,dname2,condition_column):
    return dname1.join(dname2,on=condition_column,how="left")
df=join_all_data(df1,ven,"VendorID")
df=join_all_data(df,pay,"payment_type")
df=join_all_data(df,ratec,"RatecodeID")
df=join_all_data(df,tripty,"trip_type")
df=join_all_data(df,taxi,"DOLocationID")
final_df=df
print(final_df)
print(final_df.schema)  


#fill missing values in final_df
def fill_final_df_nulls(df,col_name,fill_value) :
    return df.with_columns([pl.col(col_name).fill_null(fill_value)])
final_df=fill_final_df_nulls(final_df,"Borough","Noborough")
final_df=fill_final_df_nulls(final_df,"Zone","Unkonwn")
final_df=fill_final_df_nulls(final_df,"service_zone","Unkonwn")
print(final_df) 

# check nulls in final_df
print(final_df.null_count())
print(final_df.tail())

# #check the duplicates values
du=final_df.filter(final_df.is_duplicated())
print(du)


# #1. Total Trips by Payment Type?
totaL_trip = final_df.group_by("payment_type_desc",maintain_order=True).count().rename({"count": "total_trips"})
print(totaL_trip)

# 2. Total Passenger by Month and Year
final_df = final_df.with_columns([
    pl.col("lpep_pickup_datetime").dt.year().alias("year"),
    pl.col("lpep_pickup_datetime").dt.month().alias("month")])

total_pass = (final_df.group_by(["year", "month"],maintain_order=True).len().rename({"len": "Total_Passenger"})).sort(by="month",descending=False)
print(total_pass)

# 3. Total Trip Distance by Trip Type Description?
total_trip_dis=(final_df.group_by("trip_type_desc",maintain_order=True).agg(pl.col('trip_distance').sum()))
print(total_trip_dis)


#4.total and  avg congestion_surcharge by vendor name?
con_sur=final_df.group_by('vendor_name',maintain_order=True).agg(pl.col('congestion_surcharge').sum().alias("total_congestion_surcharge"),
                                                                 pl.col("congestion_surcharge").mean().alias("avg_congestion_surcharge"))
print(con_sur)


#5.total amount ,total fare amount ,totaltip amountc and also avg amount,fare,tip by service_zone?
ser_zone=final_df.group_by('service_zone',maintain_order=True).agg(pl.col('total_amount').sum(),
                                                                   pl.col('total_amount').mean().alias("avg_amount"),
                                                                   pl.col('fare_amount').sum().alias("total_fare_amount"),
                                                                   pl.col('fare_amount').mean().alias("avg_fare_amount"),
                                                                   pl.col('tolls_amount').sum().alias("total_tolls_amount"),
                                                                   pl.col('tolls_amount').mean().alias("avg_tolls_amount"))
print(ser_zone)


#6.total amount, total trip type by trip type desc ,payment type desc and year?
short_desc=final_df.group_by(["year",'trip_type_desc','payment_type_desc'],maintain_order=True).agg(pl.col("trip_type").count(),
                                                                                                    pl.col('total_amount').sum())
print(short_desc)






 



