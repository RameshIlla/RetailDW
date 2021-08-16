import os, json
import pandas as pd
import numpy as np
import regex as re
# this finds our json files
cust_accounts_path = './data/accounts/'
cards_path = './data/cards/'
savings_accounts_path = './data/savings_accounts/'


def get_df_from_directory(directory):
    #Get the list of json files in the  accounts directory
    json_files = [pos_json for pos_json in os.listdir(directory) if pos_json.endswith('.json')]
    json_files.sort(key=lambda f: int(re.sub('\D', '', f)))
        
    df_file= [] # an empty list to store the data frames

    # loop through the accounts json flies and assign each file to data frame
    for index, js in enumerate(json_files):
        with open(os.path.join(directory, js)) as json_file:
            
            json_data = pd.json_normalize(json.loads(json_file.read()))
            df_file.append(json_data)        
    df_directory = pd.concat(df_file) 
    return df_directory

def process_cust_account (df_cust_accounts):
    df_cust_accounts.columns=['cust_id','cust_op','cust_ts','cust_account_id','name','address','phone_number','email','set_phone_number','sa_id','set_address','set_email','card_id']

    df_cust_accounts.phone_number = np.where(df_cust_accounts.cust_op.eq('u'), df_cust_accounts.set_phone_number, df_cust_accounts.phone_number)
    df_cust_accounts.phone_number=df_cust_accounts.groupby('cust_id').phone_number.apply(lambda x: x.ffill())

    df_cust_accounts.email = np.where(df_cust_accounts.cust_op.eq('u'), df_cust_accounts.set_email, df_cust_accounts.email)
    df_cust_accounts.email=df_cust_accounts.groupby('cust_id').email.apply(lambda x: x.ffill())

    df_cust_accounts.address = np.where(df_cust_accounts.cust_op.eq('u'), df_cust_accounts.set_address, df_cust_accounts.address)
    df_cust_accounts.address=df_cust_accounts.groupby('cust_id').address.apply(lambda x: x.ffill())

    df_cust_accounts.sa_id=df_cust_accounts.groupby('cust_id').sa_id.apply(lambda x: x.ffill())
    df_cust_accounts.card_id=df_cust_accounts.groupby('cust_id').card_id.apply(lambda x: x.ffill())
    df_cust_accounts.cust_account_id=df_cust_accounts.groupby('cust_id').cust_account_id.apply(lambda x: x.ffill())
    df_cust_accounts.name=df_cust_accounts.groupby('cust_id').name.apply(lambda x: x.ffill())

    df_cust_accounts['cust_ts'] = pd.to_datetime(df_cust_accounts['cust_ts'])
    df_cust_accounts['sa_id']=df_cust_accounts['sa_id'].astype(str)

    df_cust_accounts=df_cust_accounts.drop(['set_phone_number','set_address','set_email'], axis=1)
    df_cust_accounts=df_cust_accounts[['cust_account_id','name','sa_id','card_id']].drop_duplicates()

    df_cust_accounts=df_cust_accounts[df_cust_accounts.card_id !='' ]
    df_cust_accounts= df_cust_accounts.dropna()
    return df_cust_accounts

###########################################################################
def process_cards(df_cards):
    df_cards.columns = ['card_global_id','card_op','datetime','card_id','card_number','card_credit_used','card_monthly_limit','card_status','card_set_status','card_set_credit_used']

    
    df_cards.card_status = np.where(df_cards.card_op.eq('u'), df_cards.card_set_status, df_cards.card_status)
    df_cards.card_status=df_cards.groupby('card_global_id').card_status.apply(lambda x: x.ffill())

    df_cards.card_credit_used = np.where(df_cards.card_op.eq('u'), df_cards.card_set_credit_used, df_cards.card_credit_used)
    df_cards.card_credit_used=df_cards.groupby('card_global_id').card_credit_used.apply(lambda x: x.ffill())


    df_cards.card_id=df_cards.groupby('card_global_id').card_id.apply(lambda x: x.ffill())
    df_cards.card_number=df_cards.groupby('card_global_id').card_number.apply(lambda x: x.ffill())
    df_cards.card_monthly_limit=df_cards.groupby('card_global_id').card_monthly_limit.apply(lambda x: x.ffill())

    df_cards.insert(5,'card_balance', df_cards['card_monthly_limit']-df_cards['card_credit_used'])

    df_cards=df_cards.drop(['card_set_status','card_set_credit_used'], axis=1)
    df_cards=df_cards[df_cards.card_status=='ACTIVE']
    
    return df_cards


###########################################################################
def process_savings_accounts(df_savings_accounts):

    df_savings_accounts.columns=['sa_global_id','sa_op','datetime','sa_id','sa_balance','sa_interest_rate_percent','sa_status','sa_set_balance','sa_set_interest_rate_percent']

    df_savings_accounts.sa_balance = np.where(df_savings_accounts.sa_op.eq('u'), df_savings_accounts.sa_set_balance, df_savings_accounts.sa_balance)
    df_savings_accounts.sa_balance=df_savings_accounts.groupby('sa_global_id').sa_balance.apply(lambda x: x.ffill())

    df_savings_accounts.sa_interest_rate_percent = np.where(df_savings_accounts.sa_op.eq('u'), df_savings_accounts.sa_set_interest_rate_percent, df_savings_accounts.sa_interest_rate_percent)
    df_savings_accounts.sa_interest_rate_percent=df_savings_accounts.groupby('sa_global_id').sa_interest_rate_percent.apply(lambda x: x.ffill())


    df_savings_accounts.sa_id=df_savings_accounts.groupby('sa_global_id').sa_id.apply(lambda x: x.ffill())
    df_savings_accounts.sa_status=df_savings_accounts.groupby('sa_global_id').sa_status.apply(lambda x: x.ffill())

    #df_savings_accounts['sa_ts'] = pd.to_datetime(df_savings_accounts['sa_ts'])
    df_savings_accounts['sa_id']=df_savings_accounts['sa_id'].astype(str)

    df_savings_accounts.insert(5,'sa_trans_amt', df_savings_accounts.sa_balance-df_savings_accounts.sa_balance.shift(1))

    df_savings_accounts=df_savings_accounts.drop(['sa_set_balance','sa_set_interest_rate_percent'], axis=1)

    # Remove zero sa_trans_amount records as they dont add value to the final concatinated dataframe
    df_savings_accounts=df_savings_accounts[df_savings_accounts.sa_trans_amt !=0.0 ]

    return df_savings_accounts
    #print(df_cust_accounts)
    #print(df_cards)
    #print (df_savings_accounts)

######################################################################


def join_cust_cards_sa(df_cust_accounts,df_cards, df_savings_accounts):

    #df_cust_savings_account = pd.merge(df_cust_accounts, df_savings_accounts,  how='inner',left_on='sa_id',right_on='sa_id')
    df_cust_cards = pd.merge(df_cust_accounts[['cust_account_id','name','card_id']],df_cards [['card_id','datetime','card_number','card_credit_used' ,'card_balance' , 'card_monthly_limit' ]], how='inner', left_on ='card_id',right_on='card_id')
    #df_cust_cards.insert(2,'sa_id', '')
    #df_cust_cards.insert(8,'sa_trans_amount', '')
    #df_cust_cards.insert(9,'sa_balance', '')
    #df_cust_cards=df_cust_cards.rename(columns={'card_ts':'ts'})

    #print(df_cust_cards)

    #print(df_savings_accounts)
    #df_savings_accounts=df_savings_accounts[[]].drop_duplicates()
    df_cust_savings_account = pd.merge(df_cust_accounts[['cust_account_id','name','sa_id']].drop_duplicates(), df_savings_accounts[['sa_id','datetime','sa_trans_amt','sa_balance']],  how='inner',left_on='sa_id',right_on='sa_id')


    #df_cust_savings_account = pd.merge(df_cust_accounts, df_savings_accounts[['sa_op','sa_ts','sa_id','sa_balance','sa_interest_rate_percent','sa_status']],  how='inner',left_on='sa_id',right_on='sa_id')
    #df_cust_savings_account = pd.merge(df_cust_accounts, df_savings_accounts[['sa_op','sa_ts','sa_id','sa_balance','sa_interest_rate_percent','sa_status']],  how='inner',on='sa_id')#,right_on='sa_account_id')
    #df_cust_savings_account = df_cust_accounts.join(df_savings_accounts, how = 'left',on='sa_id')
    #df_cust_savings_account = df_cust_accounts.set_index('sa_id').join(df_savings_accounts.set_index('sa_id'))

    #print(df_savings_accounts)
    #print(df_cust_savings_account)
    df_cust_savings_acount_cards =pd.concat([df_cust_cards,df_cust_savings_account])
    df_cust_savings_acount_cards=df_cust_savings_acount_cards.sort_values(by='datetime',ascending='true')
    df_cust_savings_acount_cards['datetime'] = pd.to_datetime(df_cust_savings_acount_cards['datetime'])
    df_cust_savings_acount_cards.sa_balance=df_cust_savings_acount_cards.groupby('cust_account_id').sa_balance.apply(lambda x: x.ffill())
                    #f_savings_accounts.sa_status=df_savings_accounts.groupby('sa_global_id').sa_status.apply(lambda x: x.ffill())


    return df_cust_savings_acount_cards
#print(df_cust_savings_acount_cards)

#Main function            
if __name__ == "__main__":
    
    # Load data from json files into data frames
    df_cust_accounts=get_df_from_directory(cust_accounts_path)
    df_cards=get_df_from_directory(cards_path)
    df_savings_accounts=get_df_from_directory(savings_accounts_path)
   
    #print raw dataframes
    print('*************************************Raw flattened data from Accounts json**************************************************')
    print (df_cust_accounts)
    print('*************************************Raw flattened data from Cards json******************************************************')
    print (df_cards)
    print('*************************************Raw flattened data from Savings Accounts json******************************************')
    print (df_savings_accounts)
    
    #process and clean up data frames
    df_cust_accounts = process_cust_account(df_cust_accounts)
    df_cards = process_cards (df_cards)
    df_savings_accounts = process_savings_accounts(df_savings_accounts)
    
    #Finally join the processed dataframes to see historical transaction details made on cards and savings accounts
    
    df_cust_savings_acount_cards = join_cust_cards_sa(df_cust_accounts,df_cards, df_savings_accounts)
    print('**************************Joined View of transactions made on Cards and Savings accounts of the customers******************')
    print(df_cust_savings_acount_cards)
    
       
    