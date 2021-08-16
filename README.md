## RetailDW
Load json files and derive insights of retail banking customer transaction
# How to start
Install Docker  

Either clone the repo of download as zip 
<pre>
In a terminal window, navigate to the folder where you downloaded this repo and run the below command to build the docker image  
	
	docker build -t retaildw -f solution/Dockerfile .  #Make sure the dot is included  

Now run the below command to run the program in a container  
	
	docker run --name retaildw -it retaildw  
</pre>
