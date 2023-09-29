from requests_oauthlib import OAuth2Session
import requests
import psycopg2

CLIENT_ID = 'Iv1.90f2cd0aee7a792f' #This is the client id of our oauth application  
CLIENT_SECRET = '7c5d044f72f8ea88a509744bbec982cba9feeb22' #This is the client secret of our oauth application
REDIRECT_URI = 'https://www.example.com/callback' #Redirect url of the oauth application, to which it will be redirected after authorisation
AUTHORIZATION_URL = 'https://github.com/login/oauth/authorize' #Url to give permission for authorisation to access data 
TOKEN_URL = 'https://github.com/login/oauth/access_token' #The token url containing access token

# Create a new OAuth2Session using the client ID and secret
oauth = OAuth2Session(CLIENT_ID, redirect_uri=REDIRECT_URI)

# Generate the authorization URL
authorization_url, state = oauth.authorization_url(AUTHORIZATION_URL)

# Print the authorization URL and prompt the user to visit it
print(f'Please visit this URL to authorize the application: {authorization_url}')

# After the user has authorized the application, the user will be redirected to the callback URL with a code parameter
authorization_response = input('Paste the full callback URL here: ')

# Exchange the authorization code for an access token
token = oauth.fetch_token(TOKEN_URL, authorization_response=authorization_response, client_secret=CLIENT_SECRET)

# Use the access token to make authenticated requests to the GitHub API
access_token = token['access_token']
headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/vnd.github.v3+json'
    }

#Authenticated request has been made below using the GET method and is retrived in the json file format
response = requests.get('https://api.github.com/user/repos', headers=headers)

if response.status_code==404:
    print("Error in fetching data - incorrect url is one of the possible reasons")
    exit(0)

d = response.json() #Convert to json format for better data handling
if len(d)==0:
    print("There is no repository (which is private or public) for the given user ")
    exit(0)
else:
    print("Repository details are")
    for repo in d:
        print(f"{repo['owner']['id']}, {repo['owner']['login']}, {repo['id']}, {repo['name']}, {repo['private']}, {repo['stargazers_count']}")

#This part below handles postgreSQL and data storage in it
try:
    conn=psycopg2.connect(database="gghub", user='postgres', password='password', host='127.0.0.1', port= '5432') #Establish connection with database
except psycopg2.OperationalError as e:
    print('Unable to connect to databse!')#Error handling, if not connection not successful
    exit(0)

print("Connection established with database")

conn.autocommit = True #Each statement is committed implicitly
cursor = conn.cursor() #Create a cursor

print("Cursor created")

sql ='''CREATE TABLE IF NOT EXISTS ggghub
    (
        Owner_ID int,
        Owner_name varchar(500),	
        Owner_email varchar(500),
        Repo_id int,
        Repo_name varchar(200),
        Status varchar(200),
        Stars_count int
    )'''
cursor.execute(sql) #Create a table named ggghub(if it does not exist, else do not) with ownerid, owner name, owner email, repo id, repo name, status, starscount

for repo in d:
    a1=str(repo["owner"]["id"]) #Ownerid of the repository
    a2=repo["owner"]["login"] #Owner name of the repository
    a3= str(repo["id"]) #Id of Repository
    a4=repo["name"] #Name of repository
    a5=str(repo["private"]) #Check for private or public status of repository
    if a5=="False":
        a5="Public" #If repository status is false, it means the repository is public
    else:
        a5="Private" #If status is true, it means repository is private
    a6=str(repo["stargazers_count"]) #The stargrazers count of the repository
    #Below statement is used to check for dulpicates
    b1='SELECT * from ggghub WHERE owner_id='+a1+' AND repo_id='+a3+''  
    result=cursor.execute(b1) #Execute the above query with owner id and repo id as columns
    b=cursor.rowcount #Return the rowcounts back
    if b>=1: #If the number of rowcounts is greater than 1, it means record with same owner id and repo id exists hence update them where ownerid and repoid already exists
        cursor.execute("UPDATE ggghub SET Owner_ID = "+a1+", Owner_name = '"+a2+"', Repo_id= "+a3+", Repo_name='"+a4+"', Status='"+a5+"', Stars_count="+a6+" WHERE owner_id="+a1+" AND repo_id="+a3+"")
        print("Table updated with new values")
    else: #If the rowcounts is 0, insert into the table as they are new records
        cursor.execute("INSERT INTO ggghub(Owner_ID, Owner_name, Owner_email, Repo_id, Repo_name, Status, Stars_count) VALUES ("+a1+", '"+a2+"', 'NULL', "+a3+", '"+a4+"','"+a5+"', "+a6+")")
            #As the email does not exists for owners in json file, it is inserted as null into the database
        print("Value inserted in table")


s = "COPY (SELECT * FROM ggghub) TO STDOUT WITH CSV DELIMITER ',' HEADER"
with open("./details.csv", "w") as file:
    cursor.copy_expert(s, file)
#Save the details of the table to a csv file
print("Details saved to a csv file")