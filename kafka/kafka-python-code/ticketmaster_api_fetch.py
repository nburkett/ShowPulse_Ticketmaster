import requests
import pandas as pd
from datetime import datetime, timedelta, date
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# Get API Key
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

    cols_to_drop = percent_null[percent_null > 0.5].index
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



def fetch_ticketmaster_data(city:str, stateCode: str, countryCode:str ) -> pd.DataFrame :
    """Fetches data from the Ticketmaster API.
    Args:
        city: City you want to search for events in. "Austin"
        stateCode: State you want to search for events in. "TX"
        countryCode: Country you want to search for events in. "US"

    Returns:
        dataframe: Dataframe with data from the Ticketmaster API.
    """
    # Set up API credentials and parameters
    base_url = 'https://app.ticketmaster.com/discovery/v2/events.json'
    params = {
        'apikey': ticketmaster_api_key,
        'city': city,
        'stateCode': stateCode,
        'countryCode': countryCode,
        'size': 50 , # Adjust the number of results per page as per your needs
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
                # print(f'Incremented Page # to: {page}')
            else:
                break
        else:
            print(f'Page #: {page}, Incorrect response format, terminating')
            print(data)
            break

    
    # Create a dataframe
    df = pd.DataFrame(all_events)
    print(f'Created dataframe showing all events in {city},{stateCode} with {len(df.columns)} columns and {len(df.index)} rows')
    price_copy = df.copy()
    # Drop selected cols & columns that are mostly NaNs
    # df = df.drop(columns=['images','seatmap','accessibility','ageRestrictions','test','_links'])
    # df = drop_na_cols(df)

    #Drop Unnecessary Columns
    print(f'Now dataframe has {len(df.columns)} columns and {len(df.index)} rows')

    # Apply the flatten_and_drop function to your DataFrame
    df = flatten_and_drop(df, ['_embedded','dates','sales','classifications','priceRanges'])

    ##add the price ranges back in  
    price_data = price_copy['priceRanges'].apply(flatten_json)
    price_df = pd.DataFrame.from_records(price_data)


    df = pd.concat([df, price_df], axis=1)

    pattern = '0_'
    column_names = df.columns.tolist()
    new_column_names = [name.replace(pattern, '') for name in column_names]
    df.columns = new_column_names

    print(f'Now dataframe has {len(df.columns)} columns and {len(df.index)} rows')
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
    print(f'Dataframe for {city},{stateCode} loaded. Final size: {len(df.columns)} columns, {len(df.index)} rows')
    return df


# df = fetch_ticketmaster_data('Dallas','TX','US')
# df.to_csv('/Users/nicburkett/Downloads/dallas_data.csv')
# df.head()



