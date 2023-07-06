import requests
import pandas as pd
from datetime import datetime, timedelta, date
import time 
import json 
import random 
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()

ticketmaster_api_key = os.getenv('ticketmaster_api_key')



def drop_na_cols(df):
    """Drops columns that are 75% NaN.
    Args:
        dataframe: Dataframe you want to drop columns from.
    Returns:
        dataframe: Dataframe with dropped columns.
    """
    threshold = len(df) * 0.75
    percent_null = df.isnull().sum() / len(df) * 100

    cols_to_drop = percent_null[percent_null > 0.95].index
    num_cols_dropped = len(df.columns) - len(cols_to_drop)
    print(f'Dropping {num_cols_dropped} cols, columns to drop: {cols_to_drop.tolist()}')
    # Drop columns exceeding the threshold
    df = df.drop(cols_to_drop,axis=1 )
    return df



def flatten_json(nested_json, exclude=['']):
    """Flatten json object with nested keys into a single level.
        Args:
            nested_json: A nested json object.
            exclude: Keys to exclude from output.
        Returns:
            The flattened json object if successful, None otherwise.
    """
    out = {}

    def flatten(x, name='', exclude=exclude):
        if type(x) is dict:
            for a in x:
                if a not in exclude:
                    flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out


def flatten_and_drop(original_df, cols_to_flatten):
    """Flattens and then drops columns from a DataFrame.
    Args:
        dataframe: Dataframe you want to drop columns from.
        cols_to_flatten: List of columns to flatten and drop NaNs from.
    Returns:
        dataframe: Dataframe with dropped columns.
    """
    merged_df = original_df.copy()  # Initialize merged_df with the original DataFrame

    for col in cols_to_flatten:
        # Apply flatten_json function to every row in the DataFrame
        flattened_data = original_df[col].apply(flatten_json)

        # Convert the flattened data to a DataFrame
        flattened_df = pd.DataFrame.from_records(flattened_data)

        # Calculate the threshold for NaN values
        threshold = len(flattened_df) * 0.75

        # Drop columns exceeding the threshold
        flattened_df = flattened_df.dropna(axis=1, thresh=threshold)

        # Concatenate the flattened DataFrame with the merged DataFrame
        merged_df = pd.concat([merged_df, flattened_df], axis=1)

    #combine list of columns to drop 
    prefix = 'attractions_0_images'

    #Combine to drop all at once 
    columns_to_delete = cols_to_flatten + merged_df.columns[merged_df.columns.str.startswith(prefix)].tolist()
    # Drop the original columns from the DataFrame
    merged_df.drop(columns=columns_to_delete, inplace=True)

    pattern = '_0'
    #get col names in list 
    column_names = merged_df.columns.tolist()
    #use list comprehension to replace pattern in col names
    new_column_names = [name.replace(pattern, '') for name in column_names]
    #update the col names in the dataframe
    merged_df.columns = new_column_names
    return merged_df


# Set up API credentials and parameters
api_key = ticketmaster_api_key
base_url = 'https://app.ticketmaster.com/discovery/v2/events.json'
city = 'Austin'
state = 'TX'
date_range = f"{date.today()},{date.today() + timedelta(days=90)}"
params = {
    'apikey': api_key,
    'city': city,
    'stateCode': state,
    'start_date': datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
    'end_date': (datetime.now() + timedelta(days=10)).strftime("%Y-%m-%dT%H:%M:%SZ"),
    'stateCode':'TX',
    'preferredCountry':'us',
    'size': 50 , # Adjust the number of results per page as per your needs
    # 'page': 0
}

# Send API requests and fetch all pages of data
all_events = []
page = 0

while True:
    params['page'] = page
    response = requests.get(base_url, params=params)
    data = response.json()
    
    if '_embedded' in data and 'events' in data['_embedded']:
        act_page = data['page']['number']
        print(f'Current Actual Page: {act_page}')
        events = data['_embedded']['events']
        all_events.extend(events)
        
        if 'page' in data and 'totalPages' in data['page'] and data['page']['number'] < data['page']['totalPages'] - 1:
            page += 1
            print(f'Incremented Page # to: {page}')
        else:
            break
    else:
        print(f'Page #: {page}, Incorrect response format, terminating')
        print(data)
        break


# Create a dataframe
df = pd.DataFrame(all_events)
#Drop Unnecessary Columns
# df = df.drop(columns=['images','seatmap','accessibility','ageRestrictions','test','_links'])

# Apply the flatten_and_drop function to your DataFrame
df = flatten_and_drop(df, ['_embedded','dates','sales','classifications','priceRanges'])

# Drop columns that are mostly NaNs
df = drop_na_cols(df)


##add the price ranges back in  
price_data = df['priceRanges'].apply(flatten_json)
price_df = pd.DataFrame.from_records(price_data)


df = pd.concat([df, price_df], axis=1)

pattern = '0_'
column_names = df.columns.tolist()
new_column_names = [name.replace(pattern, '') for name in column_names]
df.columns = new_column_names

### decide to only keep these 
keep_cols = ['name',
'type',
'id',
'url',
'venues_name',
'venues_id',
'venues_postalCode',
'venues_timezone',
'venues_city_name',
'venues_state_name',
'venues_state_stateCode',
'venues_country_name',
'venues_country_countryCode',
'venues_address_line1',
'venues_location_longitude',
'venues_location_latitude',
'attractions_name',
'attractions_type',
'attractions_id',
'attractions_url',
'attractions_classifications_segment_id',
'attractions_classifications_segment_name',
'attractions_classifications_genre_id',
'attractions_classifications_genre_name',
'attractions_classifications_subGenre_id',
'attractions_classifications_subGenre_name',
'start_localDate',
'status_code',
'start_localTime',
'currency',
'min',
'max',]

df = df[keep_cols]


# Define a dictionary to map old column names to new column names
column_mapping = {
    'name': 'event_name',
    'type': 'event_type',
    'id': 'event_id',
    'url': 'event_url',
    'venues_name': 'venue_name',
    'venues_id': 'venue_id',
    'venues_postalCode': 'venue_zipcode',
    'venues_timezone': 'venues_timezone',
    'venues_city_name': 'venue_city',
    'venues_state_name': 'venue_state_full',
    'venues_state_stateCode': 'venue_state_short',
    'venues_country_name': 'venue_country_name',
    'venues_country_countryCode': 'venue_country_short',
    'venues_address_line1': 'venue_address',
    'venues_location_longitude': 'venue_longitude',
    'venues_location_latitude': 'venue_latitude',
    'attractions_name': 'attraction_name',
    'attractions_type': 'attraction_type',
    'attractions_id': 'attraction_id',
    'attractions_url': 'attraction_url',
    'attractions_classifications_segment_id': 'attraction_segment_id',
    'attractions_classifications_segment_name': 'attraction_segment_name',
    'attractions_classifications_genre_id': 'attraction_genre_id',
    'attractions_classifications_genre_name': 'attraction_genre_name',
    'attractions_classifications_subGenre_id': 'attraction_subgenre_id',
    'attractions_classifications_subGenre_name': 'attraction_subgenre_name',
    'start_localDate': 'event_start_date',
    'status_code': 'ticket_status',
    'start_localTime': 'event_start_time',
    'currency': 'currency',
    'min': 'min_price',
    'max': 'max_price'
}

# Rename the columns using the dictionary
df = df.rename(columns=column_mapping)

##reset the index so we can convert to json 
df.reset_index(drop=True, inplace=True)


## SET UP THE KAFKA PRODUCER 
import json 
import random 
from datetime import datetime
import time
from kafka import KafkaProducer

topic_name = 'twitter'
# Messages will be serialized as JSON 
def serializer(message):
    return json.dumps(message).encode('utf-8')

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

## Clean out the producer 
producer.flush() 

# Convert each row to JSON and send as message to Kafka
for _, row in df.iterrows():
    # Convert the row to a dictionary with column names as keys
    data = row.to_dict()

    # Include the column names in the dictionary
    message = {col: data[col] for col in df.columns}

    # Print the JSON message
    print(message)
    
    # Send the JSON message to the Kafka topic
    producer.send(topic_name, message)
    time.sleep(5)

# Close the Kafka producer
producer.close()



# if __name__ == '__main__':
#     # Infinite loop - runs until you kill the program
#     count = 0
#     while count < 20:
#         count += 1
#         # Generate a message
#         dummy_message = generate_messages()
        
#         # Send it to our 'messages' topic
#         print(f'Producing message @ {datetime.now()} | Message = {str(dummy_message)}')
#         producer.send(topic_name, dummy_message)
        
#         # Sleep for a random number of seconds
#         time_to_sleep = random.randint(1, 11)
#         time.sleep(time_to_sleep)