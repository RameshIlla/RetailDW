## RetailDW
Load json files and derive insights of retail banking customer transactions
# How to start
Install Docker  

Either clone the repo of download as zip 
<pre>
In a terminal window, navigate to the folder where this repo is downloaded and run the below command to build the docker image  
	
	docker build -t retaildw -f solution/Dockerfile .  # Make sure the dot is included  

Now run the below command to run the program in a container  
	
	docker run --name retaildw -it retaildw  
</pre>

# How it works  
Step 1:   
The program first loops though all the json files in the given directoiries (3 in this case) and loads the combined data from all json files into one data frame per directory.  
Then it prints the flattened dataframes showing the historical data of Accounts, Cards and Savings accounts data frames.

Step 2:   
The data in the data frames created in step 1 consists of raw data hence needs to be cleaned up. Below actions are performed in this step.  
Rename columns to meaningful names and to avoid coufusion during joins.  
Populate the value for the 'set' column to the main column for better readability and to facilitate keys for joins.  
Drop the set columns as the values are already populated in the main columns.  
Fill up blank record cells with values from provious records until further update is made to that field.  

Step 3:  
Join the dataframes processed in step 2 to derive insights of transactions made on both cards and savings accounts in single view.
Technically these transactions records are concatinated after cleanup to avoid cartesian product and provide simple view.
Added two new computed columns sa_trans_amt and card_balance  to the final data frame to diplay the exact transaction amount and the balance left after each transaction. 
