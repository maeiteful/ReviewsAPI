import os
from google.cloud import bigquery
import requests
import json
import re
import ast
import numpy as np
import pandas as pd
import datetime
import gspread
import time
from functools import reduce
from oauth2client.service_account import ServiceAccountCredentials
from pytz import timezone
import threading
import multiprocessing
import logging
import requests

logging.basicConfig(level=logging.INFO)
  
client_id = '609820795124-a1dgp2kk247m97p349fjdplq9kfu8db0.apps.googleusercontent.com'
client_secret = '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'     
refresh_token='1//0fWp_mQSS2eRUCgYIARAAGA8SNwF-L9Irw_A1lw0_ssCNZjQpktOG6cjx3wy0Zu_UviJkjfJZRP24L26TUTZUCtJAd5vhxMOgWds'

def refreshToken(client_id, client_secret, refresh_token):
        params = {
                "grant_type": "refresh_token",
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token
        }

        authorization_url = "https://www.googleapis.com/oauth2/v4/token"

        r = requests.post(authorization_url, data=params)

        if r.ok:
                return r.json()['access_token']
        else:
                print(r.status_code)
                print(r.text)
                return None




def locationProccess(location):
            payload2 = {
            "pageSize":50,
            }
            
            account_id="111784892975052807992"
            name=location['name']
            print(location['title'])
            #name='locations/5385310975646857448'
            api_endpoint = f'https://mybusiness.googleapis.com/v4/accounts/{account_id}/{name}/reviews'
            access_token = refreshToken(client_id, client_secret, refresh_token)
            access_token_auth = 'Bearer ' + access_token
            lsa_response = requests.get(api_endpoint,headers={'Authorization': access_token_auth, 'Content-Type': 'application/json'},params=payload2)
            if lsa_response.status_code ==200:
                if lsa_response.json() == {}:
                    return
                
                reviews=[]
                reviews.extend(lsa_response.json()['reviews'])
                
                reviewnextpagetoken=""
                try:
                        
                    reviewnextpagetoken=lsa_response.json()['nextPageToken']
                    payload2 = {
                        "pageSize":50,
                        "pageToken":reviewnextpagetoken
                        }
                    counter=0
                    while True:
                        
                        api_endpoint = f'https://mybusiness.googleapis.com/v4/accounts/{account_id}/{name}/reviews'
                        access_token = refreshToken(client_id, client_secret, refresh_token)
                        access_token_auth = 'Bearer ' + access_token
                        Nextpage = requests.get(api_endpoint,headers={'Authorization': access_token_auth, 'Content-Type': 'application/json'},params=payload2)
                        if Nextpage.status_code ==200:
                            counter+=1
                            print(counter)
                            logging.info(counter)
                            logging.info("reviews: "+str(len(reviews)))
                            reviews.extend(Nextpage.json()['reviews'])
                            #print(Nextpage.json()['reviews'])
                            if Nextpage.json()['nextPageToken']:
                                payload2 = {
                                "pageSize":50,
                                "pageToken":Nextpage.json()['nextPageToken']
                                }
                            else:
                                break
                        else:
                            print(Nextpage.status_code)
                            print(Nextpage.text)
                            logging.info(Nextpage.status_code)
                            break
                except Exception as e :
                    print(e)
                    logging.info(e)
                #print(lsa_response.text.encode('utf-8').decode('unicode-escape', 'ignore'))
                loc=location["name"]
                loc2=loc.replace('/', '').replace('|', '')
                #if not temptoken:
                #    tkn= ""
                #else:
                #    tkn=temptoken
                title=location["title"].replace('/', '').replace('|', '')
                title=title.replace(':', '').replace(',', '')
                x = datetime.datetime.now(tz  = timezone('America/New_York')) 
                current_date = x.date()
                folder_path= f"Reviews/{current_date}"


                
                file_path = f'{folder_path}/{title}-{loc2} -- {0}.json'
                
                #ExistingNames=[]
                #try:
                #    with open(file_path, 'r') as json_file:
                #        ReviewList = json.load(json_file)
                #    ReviewList=ReviewList['reviews']
                #    for StoredReview in ReviewList:
                #        ExistingNames.append(StoredReview["reviewId"])
                #except FileNotFoundError:
                #    
                #    pass  # The file doesn't exist, so we'll create it later
                
                
                
                
                
                #r=lsa_response.json()
                #recievedReviews=r["reviews"]
                #
                #for id in recievedReviews:
                #    if id['reviewId'] not in ExistingNames:
                #        print("new review from: "+ id['reviewer']['displayName'])
                #        
                #IDs=[]
                #for id in recievedReviews:
                #    IDs.append(id['reviewId'])
                #
                #for id in ExistingNames:
                #    if id not in IDs:
                #        for StoredReview in ReviewList:
                #            if StoredReview["reviewId"]== id:
                #                print(StoredReview['reviewer']['displayName']+"'s review has been deleted")
                        
                # Save the JSON content to the file
                final = lsa_response.json()
                print(len(reviews))
                String2IntMap={
                    "FIVE":5,
                    "FOUR":4,
                    "THREE":3,
                    "TWO":2,
                    "ONE":1
                }
                for review in reviews:
                    try:
                        val=review["starRating"]
                        review["starRating"]=String2IntMap[val]
                    except:
                        continue
                logging.info("FINAL: "+str(len(reviews)))
                final["client_id"]=loc2.replace('locations', '')
                final["client_name"]=title
                final['reviews'] = reviews
                final['customer_id'] = 2
                
                url = "http://lsa.clixsy.com:54323/api/reviews"
                
                json_data = json.dumps(final)

                # Set the content type to JSON in the request headers
                headers = {'Content-Type': 'application/json'}
                with open(file_path, 'w', encoding='utf-8') as file:
                    json.dump(final, file, ensure_ascii=False, indent=2)
                # Send the POST request to the endpoint with the JSON data
                response = requests.post(url, data=json_data, headers=headers)
                time.sleep(0.1)
                
                
                
            else:
            # Print the error status code and response content
                print(f"Error: {lsa_response.status_code}")
                print(lsa_response.text)



def Reviews_main(token):


    #access_token = refreshToken(client_id, client_secret, refresh_token)
    #access_token_auth = 'Bearer ' + access_token
    #
    #lsa_response = requests.get('https://mybusinessaccountmanagement.googleapis.com/v1/accounts',headers={'Authorization': access_token_auth}) 
    #j=lsa_response.json()
    #print(lsa_response.json())
    #print(lsa_response.text)
    #accounts=j['accounts']
    #for i in accounts:
    #    print(i['name'])
    #    access_token = refreshToken(client_id, client_secret, refresh_token)
    #    access_token_auth = 'Bearer ' + access_token
#
    #    payload = {
    #    "readMask": "name,title",
    #    "pageSize":10,
    #    }   
    #    account_id="111784892975052807992"
#
    #    base_url = "https://mybusiness.googleapis.com/v4/accounts/116197453511878364826/locations:batchGetReviews"
    #    api_endpoint = f'https://mybusinessbusinessinformation.googleapis.com/v1/{i["name"]}/locations'
#
    #    lsa_response = requests.get(api_endpoint,headers={'Authorization': access_token_auth, 'Content-Type': 'application/json'},params=payload) 
    #    if lsa_response.status_code ==200:
    #        print(lsa_response.json()['nextPageToken'])
    #        jsonR=lsa_response.json()
    #        try:
    #            locations= jsonR['locations']
    #            print(len(locations))
    #        except :
    #            continue
    #        
    #        for location in locations:
    #        
#
    #            name=location['name']
    #            #name='locations/5385310975646857448'
    #            api_endpoint = f'https://mybusiness.googleapis.com/v4/accounts/{account_id}/{name}/reviews'
    #            access_token = refreshToken(client_id, client_secret, refresh_token)
    #            access_token_auth = 'Bearer ' + access_token
    #            lsa_response = requests.get(api_endpoint,headers={'Authorization': access_token_auth, 'Content-Type': 'application/json'})
    #            if lsa_response.status_code ==200:
    #                if lsa_response.json() == {}:
    #                    continue
    #                #print(lsa_response.text.encode('utf-8').decode('unicode-escape', 'ignore'))
    #                loc=location["name"]
    #                loc2=loc.replace('/', '').replace('|', '')
    #                title=location["title"].replace('|', '').replace('/', '')
    #                file_path = f'jsons/{title}-{loc2}.json'
    #                ExistingNames=[]
    #                try:
    #                    with open(file_path, 'r') as json_file:
    #                        ReviewList = json.load(json_file)
    #                    ReviewList=ReviewList['reviews']
    #                    for StoredReview in ReviewList:
    #                        ExistingNames.append(StoredReview["reviewId"])
    #                except FileNotFoundError:
#
    #                    pass  # The file doesn't exist, so we'll create it later
    #                
    #                
    #                
    #                
    #                
    #                r=lsa_response.json()
    #                recievedReviews=r["reviews"]
#
    #                #for id in recievedReviews:
    #                #    if id['reviewId'] not in ExistingNames:
    #                #        print("new review from: "+ id['reviewer']['displayName'])
##
    #                #IDs=[]
    #                #for id in recievedReviews:
    #                #    IDs.append(id['reviewId'])
##
    #                #for id in ExistingNames:
    #                #    if id not in IDs:
    #                #        for StoredReview in ReviewList:
    #                #            if StoredReview["reviewId"]== id:
    #                #                print(StoredReview['reviewer']['displayName']+"'s review has been deleted")
#
    #                # Save the JSON content to the file
    #                with open(file_path, 'w', encoding='utf-8') as file:
    #                    json.dump(lsa_response.json(), file, ensure_ascii=False, indent=2)
    #            else:
    #            # Print the error status code and response content
    #                print(f"Error: {lsa_response.status_code}")
    #                print(lsa_response.text)
    #    else:
    #    # Print the error status code and response content
    #        print(f"Error: {lsa_response.status_code}")
    #        print(lsa_response.text)
    #    
    #return
    access_token = refreshToken(client_id, client_secret, refresh_token)
    access_token_auth = 'Bearer ' + access_token
    
    payload = {
        "readMask": "name,title,websiteUri",
        "pageSize":100,
        }
    if token:
        payload = {
        "readMask": "name,title,websiteUri",
        "pageSize":100,
        "pageToken":token,
        }
    account_id="111784892975052807992"
    
    base_url = "https://mybusiness.googleapis.com/v4/accounts/116197453511878364826/locations:batchGetReviews"
    api_endpoint = f'https://mybusinessbusinessinformation.googleapis.com/v1/accounts/{account_id}/locations'
    temptoken=None
    lsa_response = requests.get(api_endpoint,headers={'Authorization': access_token_auth, 'Content-Type': 'application/json'},params=payload) 
    if lsa_response.status_code ==200:
        try:
            temptoken=lsa_response.json()['nextPageToken']
            print(lsa_response.json()['nextPageToken'])
        except:
            temptoken=None
        jsonR=lsa_response.json()
        
        locations= jsonR['locations']
        print(len(locations))
        #print(locations)
        
        
                
        max_processes = 12  # Change this to your desired maximum
        x = datetime.datetime.now(tz  = timezone('America/New_York')) 
        current_date = x.date()
        folder_path= f"Reviews/{current_date}"
        if not os.path.exists(folder_path):
            # If it doesn't exist, create it
            os.makedirs(folder_path)
  
        with multiprocessing.Pool(processes=max_processes) as pool:
        # Use the pool.map function to apply locationProccess to each location
        # The pool takes care of running processes concurrently up to the specified maximum
            pool.map(locationProccess, locations)
        #processes = [] = []
        #for location in locations:
        #    process = multiprocessing.Process(target=locationProccess, args=(location,))
        #    processes.append(process)
        #    process.start()
#
        #count=0
        #for process  in processes:
        #    print("Joining Threads: "+ str(count) + "/"+str(len(processes)))
        #    process.join()
        #    count=count+1
	
            
    else:
    # Print the error status code and response content
        print(f"Error: {lsa_response.status_code}")
        print(lsa_response.text)
    if temptoken:
        print("NEW PAGE")
        Reviews_main(temptoken)
    return




def accounts():
    access_token = refreshToken(client_id, client_secret, refresh_token)
    access_token_auth = 'Bearer ' + access_token
    

    account_id="111784892975052807992"
    
    api_endpoint = f'https://mybusinessaccountmanagement.googleapis.com/v1/locations/{3443100194671465586}/admins'
    temptoken=None
    lsa_response = requests.get(api_endpoint,headers={'Authorization': access_token_auth, 'Content-Type': 'application/json'}) 
    if lsa_response.status_code ==200:
        print(lsa_response.json())




def Execute_Main():
	Acc_info = Reviews_main(None)
    #accounts()


if __name__ == "__main__":
	Execute_Main()
 
 
#account id: 116197453511878364826


#refresh=1//0fqpBZKCh3hwwCgYIARAAGA8SNwF-L9Iry5q8m849FdlJNMX_QD6CGP4Sh3SrPgcVothysx2mEaqcO9FTEBrUkhxT76_PZF7Xm8I <- myro personal
# old: 1//03KX5mduHoC0SCgYIARAAGAMSNwF-L9Ir2fvK5WmrUyPIbaLmowqhKCZUQ3QrB_A1EZoVO4vLW-lxqlDOD1eFjH2NoY6AubMGoNs <-Myroslav@clixsy
#1//0fTq9TOuxg1O5CgYIARAAGA8SNwF-L9Ir5H9Po4x85exjw-KexBXv_Z77HJGQU0JHInD1GDBG94TpEOSnIO5SU2zLYL5dN72b2ak <-clixsy