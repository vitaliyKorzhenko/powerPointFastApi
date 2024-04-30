# powerPointFastApi
git clone project

# install packages
pip install -r requirements.txt


# start project

add to path uvicorn 


uvicorn main:app --reload

or full path 

/home/vitaliy/.local/bin/uvicorn main:app --reload


# check api 
check 127.0.0.1:8000 

# deploy

nohup uvicorn main:app --host 0.0.0.0 --port 8000 &

# check port
sudo lsof -i :8000
uvicorn 742180 root    6u  IPv4 8824527      0t0  TCP *:8000 (LISTEN)
kill [your id ]

