import os
from os.path import exists
import pandas as pd
from datetime import datetime as dt
import time
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import polyline
import folium
import json

with open('config.json', 'r') as file:
    config = json.load(file)

client_id = config['credentials']['client_id']
client_secret = config['credentials']['client_secret']
refresh_token = config['credentials']['refresh_token']
map_center = config['credentials']['map_center']
map_zoom = config['credentials']['map_zoom']

### API token request 
auth_url = "https://www.strava.com/oauth/token"

credentials = {
    'client_id': client_id,
    'client_secret': client_secret,
    'refresh_token': refresh_token,
    'grant_type': "refresh_token",
    'f': 'json'
}

# use refresh token to get new access token and prep header
res = requests.post(auth_url, data=credentials, verify=False)
access_token = res.json()['access_token']
header = {'Authorization': 'Bearer ' + access_token}

#see if a file already exists & set continued boolean
continued = os.path.exists(credentials["client_id"] + "-activities.csv")
if continued == True:
    print("Found existing data in directory. Looking for new activities")
    # if a saved file exists, load it in
    athlete = pd.read_csv(credentials["client_id"] + "-activities.csv")

#if a file already exists, find date of most recent event logged
lastact_epoch = 0
if continued == True:
    lastact = max(athlete['start_date'])
    datetime = dt.strptime(lastact[:-1], '%Y-%m-%dT%H:%M:%S')
    lastact_epoch = datetime.timestamp()

#get athlete activities - this has a list of all activities, but it doesn't have all the details    
athletedata = []
athlete_url = "https://www.strava.com/api/v3/athlete/activities"
page = 1
while True:
    param = {'per_page': 200, 'page': page, 'after': lastact_epoch}
    req_data = requests.get(athlete_url, headers=header, params=param).json()
    if len(req_data) == 0:
        break
    if "message" in req_data:
        if "Rate Limit Exceeded" in req_data["message"]:
            print('Rate limit exceeded. Try again in 15 minutes')
            break   
    athletedata.extend(req_data)
    page += 1

#add new activities to df (if it already exists), or create a df from results
if continued == True:
    athlete = athlete.append(pd.json_normalize(athletedata))
else:
    print('Did not find any existing data in directory.')
    athlete = pd.json_normalize(athletedata)

# merge_and_save takes the activity details, adds them to the athlete activities, and saves as csv
def merge_and_save(activities, athlete, continued, credentials):

        activities_df = pd.json_normalize(activities)
        cols = ['id', 'map.polyline']
        activities_df = activities_df[cols]

        merged = pd.merge(left=athlete, right=activities_df, how="left", on="id")

        # have to combine columns if this isn't the first time running
        if continued == True:
            merged["map.polyline"] = merged["map.polyline_x"].fillna(merged["map.polyline_y"])
        merged = merged.drop(columns=["map.polyline_x", "map.polyline_y"], axis=1)
        
        # print to csv
        merged.to_csv(credentials["client_id"] + "-activities.csv", index=False)
  

# get_activities takes the athlete activities result and looks up details for each activitiy individually
def get_activities(athlete, continued, credentials, new_data):
    for index, row in athlete.iterrows():
        # check to see if activity details have already been added for each row
        if continued == True and len(str(row["map.polyline"])) > 3:
            pass
        
        else:
            # look up activity
            ActivityByID_url = (
                "https://www.strava.com/api/v3/activities/"
                + str(row["id"])
                + "?includeAllEfforts=true")
            header = {"Authorization": "Bearer " + access_token}
            activitydetail = requests.get(ActivityByID_url, headers=header).json()
            
            #let you know a rate limit has been hit
            if "message" in activitydetail:
                if "Rate Limit Exceeded" in activitydetail["message"]:
                    print("Rate Limit Exceeded.")
                    
                    # if the limit was hit right away, notify user and skip the merge and save step
                     # it could potentially be useful to save athlete activities, but probably not worth it
                    if len(activities) == 0:
                        print('No activity details returned. Will try again in 15 minutes')       

                    else:
                        # run merge_and_save to save data to csv and let user know
                        merge_and_save(activities, athlete, continued, credentials)
                        print("Progress saved to file. Waiting 15 minutes to try again")

                    # wait almost 16 minutes just to make sure you don't get another rate limit
                    time.sleep(950)
                    continue

            else:
                # because polyline is the main thing I'm going for & what I'm using to see
                # if details have already been looked up, I add a 'no_data' to the column
                # if there's no polyline
                if len(str(activitydetail["map"]["polyline"])) < 5:
                    activitydetail["map"]["polyline"] = "no_data"
                # add details to a running JSON list of details
                activities.append(activitydetail)
                new_data +=1
    # return the big list of JSON details
    return(new_data)
 
activitydetail = []
activities = []
new_data = 0
new_results = get_activities(athlete, continued, credentials, new_data)
print(new_results)
if new_results > 0:
    print('Adding ' + str(new_results) + ' new activities.')
    merge_and_save(activities, athlete, continued, credentials)
else:
    print('No new data found.')


################

# fix data
activities = pd.read_csv('124433-activities.csv')
#convert distance from object to numeric
activities['distance_meter'] = pd.to_numeric(activities['distance'], errors = 'coerce')
#distance from meters to miles
activities['distance'] = activities['distance_meter'] * 0.000621371

activities['elapsed_minutes'] = activities['elapsed_time'] /60 
activities['mph'] = activities['distance'] / (activities['elapsed_minutes'] / 60)
activities['avg pace'] = activities['elapsed_minutes'] / activities['distance']

pd.set_option('mode.chained_assignment', None) 
activities['start_date_local'] = pd.to_datetime(activities['start_date_local'])
activities['year'] = activities['start_date_local'].dt.year
activities['year'] = (activities['year']).astype(object) #change year from numeric to object
activities['month'] = activities['start_date_local'].dt.month_name()
activities['dayofyear'] = activities['start_date_local'].dt.dayofyear
activities['dayofyear'] = pd.to_numeric(activities['dayofyear'])

def update_maps(stravadata):
    walk_map = folium.Map(
        location= map_center,
        zoom_start= map_zoom,
        tiles='OpenStreetMap',
        width=1800,
        height=1500
    )
    
    
    
    walks = stravadata[stravadata.type=='Walk']
    years = list(set(walks['year']))
    
    for y in years:
        yearwalks = folium.FeatureGroup(name=str(y)).add_to(walk_map)
        yeardata = walks[walks.year==y]

        for i in yeardata['map.polyline']:
            if i == 'no_data':
                pass
            elif type(i) == float:
                pass
            else:
                line = folium.PolyLine(locations=polyline.decode(i), smoothFactor=2, weight=6, opacity=.7)
                yearwalks.add_child(line)
            
    folium.LayerControl().add_to(walk_map)
    walk_map.save('walks.html')



    ride_map = folium.Map(
        location=[47.690191, -122.225480],
        zoom_start=11,
        tiles='OpenStreetMap',
        width=1800,
        height=1500
    )
    rides = stravadata[stravadata.type=='Ride']
    years = list(set(rides['year']))
    
    for y in years:
        yearrides = folium.FeatureGroup(name=str(y)).add_to(ride_map)
        yeardata = rides[rides.year==y]

        for i in yeardata['map.polyline']:
            if i == 'no_data':
                pass
            elif type(i) == float:
                pass
            else:
                line = folium.PolyLine(locations=polyline.decode(i), smoothFactor=2, weight=6, opacity=.7)
                yearrides.add_child(line)
            
    folium.LayerControl().add_to(ride_map)
    ride_map.save('rides.html')
update_maps(activities)