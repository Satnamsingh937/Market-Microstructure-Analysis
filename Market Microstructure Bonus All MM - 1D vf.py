# -*- coding: utf-8 -*-
"""

Market Microstructure Project BONUS (only applied on 1 day due to lack of memory in our computers)
MSc Finance & Big Data

Salma CHERAIBAT
Mohamed DAKKOURI
Salma ESSAOUI
Satnam SINGH
Jihane WADI

"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


pd.set_option("display.max_columns",30) # A QUOI CA SERT ? 

df_quotes = pd.read_excel(r"C:\Users\ssatn\Desktop\CHV_quotes.xlsx")
df_trades = pd.read_excel(r"C:\Users\ssatn\Desktop\CHV_trades.xlsx")

df_quotes["DATE"] = df_quotes["DATE"].astype(str)
df_quotes["TIME"] = df_quotes["TIME"].astype(str)
df_quotes["DATE_TIME"] = pd.to_datetime(df_quotes["DATE"].str[:] + " " + df_quotes["TIME"].str[:])


df_trades["DATE"] = df_trades["DATE"].astype(str)
df_trades["TIME"] = df_trades["TIME"].astype(str)
df_trades["DATE_TIME"] = pd.to_datetime(df_trades["DATE"].str[:] + " " + df_trades["TIME"].str[:])

# Delete all the lines with NaN in MMID Column
df_quotes = df_quotes.loc[~(df_quotes["MMID"].isna()) & ~(df_quotes['BID']==0)& ~ (df_quotes['OFR']==0)]

# Filter the trade file to keep only 1 day (due to lack of memory in our computer we cannot run it on the full dataset)
df_trades_1D = df_trades.loc[df_trades["DATE_TIME"] < "1998-08-03 16:00:00"]
df_trades_1D = df_trades.loc[df_trades["DATE_TIME"] > "1998-08-03 09:33:08"]

# Get the last quotes for each MM 
list_MM = list(df_quotes["MMID"].unique())
list_data = []
for j in df_trades_1D["DATE_TIME"] : 
    A = pd.DataFrame(df_quotes[["MMID","DATE_TIME","BID","OFR","BIDSIZ","OFRSIZ"]].loc[df_quotes["DATE_TIME"] < j])
    for i in list_MM : 
        try:
            Extract = np.asarray(A.loc[A["MMID"] == i])[-1]
            list_data.append([j, Extract])
        except:
            list_data.append([j, 'Introuvable'])
            print(i)
            
Result = pd.DataFrame(list_data)
Result.rename(columns = {0:"DATE",1:"INFO"}, inplace = True)

Result["DATE"] = Result["DATE"].astype("str")
Result["INFO"] = Result["INFO"].astype("str")

Result = pd.DataFrame(Result.groupby("DATE")["INFO"].unique())

df_trades_1D["DATE_TIME"] = df_trades_1D["DATE_TIME"].astype("str")

df_final = pd.merge(Result, df_trades_1D[["DATE_TIME","PRICE","SIZE","DATE"]], left_on=Result.index, right_on="DATE_TIME", how='left')

def nettoyage(x) : 
    x = str(x)
    return x.replace("Timestamp","").replace("(","").replace("'","").replace(")","").replace("[","").replace("]","").replace('"',"").replace("\n","")

df_final["INFO"] = df_final["INFO"].apply(nettoyage)

df_final[[i for i in range(0,21)]]=df_final["INFO"].str.rsplit(" ",expand=True)

df_final.drop(columns=["INFO", 0, 7, 14],inplace=True)

df_final.rename(columns={1:"DATE_SHAW", 2:"TIME_SHAW", 3:"BID_SHAW", 4:"OFR_SHAW", 5:"BID_SIZE_SHAW", 6:"OFR_SIZE_SHAW",
                             8:"DATE_TRIM", 9:"TIME_TRIM", 10:"BID_TRIM", 11:"OFR_TRIM", 12:"BID_SIZE_TRIM", 13:"OFR_SIZE_TRIM",
                             15:"DATE_MADF", 16:"TIME_MADF", 17:"BID_MADF", 18:"OFR_MADF", 19:"BID_SIZE_MADF", 20:"OFR_SIZE_MADF"},
                            inplace=True)

# Get the best Bid and Ask :

df_final['BID_SHAW'] = pd.to_numeric(df_final['BID_SHAW'])
df_final['OFR_SHAW'] = pd.to_numeric(df_final['OFR_SHAW'])
df_final['BID_TRIM'] = pd.to_numeric(df_final['BID_TRIM'])
df_final['OFR_TRIM'] = pd.to_numeric(df_final['OFR_TRIM'])
df_final['BID_MADF'] = pd.to_numeric(df_final['BID_MADF'])
df_final['OFR_MADF'] = pd.to_numeric(df_final['OFR_MADF'])    

df_final["BEST BID"] = df_final.iloc[:,[6,12,18]].max(axis=1)
df_final["BEST OFR"] = df_final.iloc[:,[7,13,19]].min(axis=1)

#Get the MM for the best Bid and Ask 
df_final["BEST BID POSITION"] = df_final.iloc[:,[6,12,18]].astype(float).idxmax(axis=1)
df_final["BEST OFR POSITION"] = df_final.iloc[:,[7,13,19]].astype(float).idxmin(axis=1)

for i in range(len(df_final["BEST BID POSITION"])):
    df_final["BEST BID POSITION"].iloc[i] = str(df_final["BEST BID POSITION"].iloc[i])[4:]
    df_final["BEST OFR POSITION"].iloc[i] = str(df_final["BEST OFR POSITION"].iloc[i])[4:]
    
# Get MID SPREAD, SPREAD , 20%, 30% OF SPREAD 

df_final["SPREAD"] = 0
df_final["MID SPREAD"] = 0
df_final["SPREAD_20_PERCENT"] = 0
df_final["SPREAD_30_PERCENT"] = 0

df_final["SPREAD 2"] = 0 # 2 if we have different MM for best bid and best ofr
df_final["MID SPREAD 2"] = 0
df_final["SPREAD_20_PERCENT 2"] = 0 
df_final["SPREAD_30_PERCENT 2"] = 0

for i in range(len(df_final.index)):
    if df_final["BEST BID POSITION"].iloc[i] == df_final["BEST OFR POSITION"].iloc[i]:
           
        df_final["SPREAD"].iloc[i] = df_final["BEST OFR"].iloc[i] - df_final["BEST BID"].iloc[i]
        df_final["MID SPREAD"].iloc[i] = (df_final["BEST OFR"].iloc[i] - df_final["BEST BID"].iloc[i])/2
        
        df_final["SPREAD_20_PERCENT"].iloc[i] = 0.2 * df_final["SPREAD"].iloc[i]
        df_final["SPREAD_30_PERCENT"].iloc[i] = 0.3 * df_final["SPREAD"].iloc[i]
        
    else :
        
        BID_MM = df_final["BEST BID POSITION"].iloc[i]
        OFR_MM = df_final["BEST OFR POSITION"].iloc[i]
        
        df_final["SPREAD"].iloc[i] = df_final["OFR_" + BID_MM].iloc[i] - df_final["BID_" + BID_MM].iloc[i]
        df_final["MID SPREAD"].iloc[i] = (df_final["OFR_" + BID_MM].iloc[i] - df_final["BID_" + BID_MM].iloc[i])/2
        
        df_final["SPREAD_20_PERCENT"].iloc[i] = 0.2 * df_final["SPREAD"].iloc[i]
        df_final["SPREAD_30_PERCENT"].iloc[i] = 0.3 * df_final["SPREAD"].iloc[i]
        
        df_final["SPREAD 2"].iloc[i] = df_final["OFR_" + OFR_MM].iloc[i] - df_final["BID_" + OFR_MM].iloc[i]
        df_final["MID SPREAD 2"].iloc[i] = (df_final["OFR_" + OFR_MM].iloc[i] - df_final["BID_" + OFR_MM].iloc[i])/2
        
        df_final["SPREAD_20_PERCENT 2"].iloc[i] = 0.2 * df_final["SPREAD 2"].iloc[i]
        df_final["SPREAD_30_PERCENT 2"].iloc[i] = 0.3 * df_final["SPREAD 2"].iloc[i]

# Algorithm CLNV
tick_rule_liste=[]
quote_rule_liste=[]

for i, j, k, m, n, o in zip(df_final["PRICE"], df_final["BEST BID"], df_final["BEST OFR"], df_final["SPREAD_30_PERCENT"], df_final["BEST OFR POSITION"], df_final["BEST BID POSITION"]) : 
    if n == o:
        if i < j or i > k : 
            tick_rule = 1
            quote_rule = 0
        elif j + m < i < k-m : 
            tick_rule = 1
            quote_rule = 0
        else : 
            tick_rule = 0
            quote_rule = 1
        tick_rule_liste.append(tick_rule)
        quote_rule_liste.append(quote_rule)
    else:
        tick_rule_liste.append(0)
        quote_rule_liste.append(0)
        
df_final["TICK RULE"] = tick_rule_liste
df_final["QUOTE RULE"] = quote_rule_liste
        
df_final["TICK RULE 2"] = 0
df_final["QUOTE RULE 2"] = 0

for i in range(27,29): #len(df_final.index)):
    
    if df_final["BEST BID POSITION"].iloc[i] != df_final["BEST OFR POSITION"].iloc[i]:
        print(i)
                
        BID_MM = df_final["BEST BID POSITION"].iloc[i]
        OFR_MM = df_final["BEST OFR POSITION"].iloc[i]
        #print(i, BID_MM, OFR_MM)
        
        if df_final["PRICE"].iloc[i] < df_final["BID_"+BID_MM].iloc[i] or df_final["PRICE"].iloc[i] > df_final["OFR_" + BID_MM].iloc[i] : 
            print('ZZZ')
            df_final["TICK RULE"].iloc[i] = 1
            df_final["QUOTE RULE"].iloc[i] = 0
            
        elif df_final["BID_" + BID_MM].iloc[i] + df_final["SPREAD_30_PERCENT"].iloc[i] < df_final["PRICE"].iloc[i] < df_final["OFR_" + BID_MM].iloc[i] - df_final["SPREAD_30_PERCENT"].iloc[i]: 
            print('AAA')
            df_final["TICK RULE"].iloc[i] = 1
            df_final["QUOTE RULE"].iloc[i] = 0
        else : 
            print('EEE')
            df_final["TICK RULE"].iloc[i] = 0
            df_final["QUOTE RULE"].iloc[i] = 1
    

for i in range(len(df_final.index)):
    
    if df_final["BEST BID POSITION"].iloc[i] != df_final["BEST OFR POSITION"].iloc[i]:
                
        BID_MM = df_final["BEST BID POSITION"].iloc[i]
        OFR_MM = df_final["BEST OFR POSITION"].iloc[i]
        if df_final["PRICE"].iloc[i] < df_final["BID_"+OFR_MM].iloc[i] or df_final["PRICE"].iloc[i] > df_final["OFR_" + OFR_MM].iloc[i] : 
            df_final["TICK RULE 2"].iloc[i] = 1
            df_final["QUOTE RULE 2"].iloc[i] = 0
            
        elif df_final["BID_" + OFR_MM].iloc[i] + df_final["SPREAD_30_PERCENT 2"].iloc[i] < df_final["PRICE"].iloc[i] < df_final["OFR_" + OFR_MM].iloc[i]  - df_final["SPREAD_30_PERCENT 2"].iloc[i]: 
            df_final["TICK RULE 2"].iloc[i] = 1
            df_final["QUOTE RULE 2"].iloc[i] = 0
        else : 
            df_final["TICK RULE 2"].iloc[i] = 0
            df_final["QUOTE RULE 2"].iloc[i] = 1


df_final["BUYER INITIATED TRADE"] = 0
df_final["SELLER INITIATED TRADE"] = 0
df_final["BUYER INITIATED TRADE 2"] = 0
df_final["SELLER INITIATED TRADE 2"] = 0

# TICK RULE TRADE DIRECTION
for i in range(1,df_final.shape[0],1) : 
    #if  df_final["BEST BID POSITION"].iloc[i] == df_final["BEST OFR POSITION"].iloc[i]:
        
    if df_final['TICK RULE'].iloc[i] == 1 : 
    
        if df_final["PRICE"].iloc[i] > df_final["PRICE"].iloc[i-1] : 
            df_final["BUYER INITIATED TRADE"].iloc[i] = 1
            df_final["SELLER INITIATED TRADE"].iloc[i] = 0             
            
        elif df_final["PRICE"].iloc[i] < df_final["PRICE"].iloc[i-1] : 
            df_final["BUYER INITIATED TRADE"].iloc[i] = 0
            df_final["SELLER INITIATED TRADE"].iloc[i] = 1    
                
        elif df_final["PRICE"].iloc[i] == df_final["PRICE"].iloc[i-1] : 
            
            for j in range(i-1, 0, -1) : 
                
                if df_final["PRICE"].iloc[i] > df_final["PRICE"].iloc[j] :
                    df_final["BUYER INITIATED TRADE"].iloc[i] = 1
                    df_final["SELLER INITIATED TRADE"].iloc[i] = 0     
                    break
                    
                elif df_final["PRICE"].iloc[i] < df_final["PRICE"].iloc[j]: 
                    df_final["BUYER INITIATED TRADE"].iloc[i] = 0
                    df_final["SELLER INITIATED TRADE"].iloc[i] = 1     
                    break
                    
df_final["MID"] = 0
df_final["MID 2"] = 0

for i in range(0,df_final.shape[0]) : 
        if  df_final["BEST BID POSITION"].iloc[i] == df_final["BEST OFR POSITION"].iloc[i]:
            df_final["MID"].iloc[i] =(df_final["BEST OFR"].iloc[i] + df_final["BEST BID"].iloc[i])/2
        else:
            BID_MM = df_final["BEST BID POSITION"].iloc[i]
            OFR_MM = df_final["BEST OFR POSITION"].iloc[i]
            df_final["MID"].iloc[i] =(df_final["OFR_" + BID_MM].iloc[i] + df_final["BID_"+ BID_MM].iloc[i])/2
            df_final["MID 2"].iloc[i] =(df_final["OFR_" + OFR_MM].iloc[i] + df_final["BID_"+ OFR_MM].iloc[i])/2

# QUOTE RULE TRADE DIRECTION
for i in range(0,df_final.shape[0]) : 
    
    if df_final["QUOTE RULE"].iloc[i] == 1 : 
        
        if df_final["PRICE"].iloc[i] > df_final["MID"].iloc[i] : 
            df_final["BUYER INITIATED TRADE"].iloc[i] = 1
            df_final["SELLER INITIATED TRADE"].iloc[i] = 0
            
        elif  df_final["PRICE"].iloc[i] < df_final["MID"].iloc[i] : 
            df_final["BUYER INITIATED TRADE"].iloc[i] = 0
            df_final["SELLER INITIATED TRADE"].iloc[i] = 1
        else : 
            df_final["BUYER INITIATED TRADE"].iloc[i] = np.nan
            df_final["SELLER INITIATED TRADE"].iloc[i] = np.nan
                
        if  df_final["BEST BID POSITION"].iloc[i] != df_final["BEST OFR POSITION"].iloc[i]:
    
            if  df_final["PRICE"].iloc[i] > df_final["MID 2"].iloc[i] : 
                df_final["BUYER INITIATED TRADE 2"].iloc[i] = 1
                df_final["SELLER INITIATED TRADE 2"].iloc[i] = 0
                
            elif  df_final["PRICE"].iloc[i] < df_final["MID 2"].iloc[i] : 
                df_final["BUYER INITIATED TRADE 2"].iloc[i] = 0
                df_final["SELLER INITIATED TRADE 2"].iloc[i] = 1
            else : 
                df_final["BUYER INITIATED TRADE 2"].iloc[i] = np.nan
                df_final["SELLER INITIATED TRADE 2"].iloc[i] = np.nan
            
df_final[(df_final["SELLER INITIATED TRADE"] == np.nan ) | (df_final["BUYER INITIATED TRADE"] == np.nan)]            
df_final[(df_final["SELLER INITIATED TRADE 2"] == np.nan ) | (df_final["BUYER INITIATED TRADE 2"] == np.nan)]  

df_final['BEST MM'] = ''
for i in range(1,df_final.shape[0],1) : 

    if  df_final["BEST BID POSITION"].iloc[i] != df_final["BEST OFR POSITION"].iloc[i]:
        
        if df_final["SELLER INITIATED TRADE"].iloc[i] == df_final["SELLER INITIATED TRADE 2"].iloc[i] or df_final["BUYER INITIATED TRADE"].iloc[i] == df_final["BUYER INITIATED TRADE 2"].iloc[i]  :
            
            if df_final["SELLER INITIATED TRADE"].iloc[i] == 1:
                BID_MM = df_final["BEST BID POSITION"].iloc[i]
                OFR_MM = df_final["BEST OFR POSITION"].iloc[i]
                #best_bid = max(df_final["BID_"+ BID_MM].iloc[i], df_final["BID_"+ OFR_MM].iloc[i])
                best_bid = df_final.iloc[:,[6,12,18]].astype(float).idxmax(axis=1)[0][4:]
                df_final['BEST MM'].iloc[i] = best_bid
            else:
                BID_MM = df_final["BEST BID POSITION"].iloc[i]
                OFR_MM = df_final["BEST OFR POSITION"].iloc[i]
                #best_ofr = min(df_final["OFR_"+ BID_MM].iloc[i], df_final["OFR_"+ OFR_MM].iloc[i])
                best_ofr = df_final.iloc[:,[7,13,19]].astype(float).idxmax(axis=1)[0][4:]
                df_final['BEST MM'].iloc[i] = best_ofr
        
        else:
            df_final['BEST MM'].iloc[i] = 666
            
    else:
        df_final['BEST MM'].iloc[i] = df_final["BEST BID POSITION"].iloc[i]
    
df_final = df_final.loc[df_final['BEST MM']!= 666]
            
# STEP 4 : PROPORTION OF INSIDE/ OUTSIDE / AT QUOTES           
df_final['Quote Position'] = ''

for i in range(0, df_final.shape[0]):
    if  df_final["BEST BID POSITION"].iloc[i] == df_final["BEST OFR POSITION"].iloc[i]:
        if df_final["BEST BID"].iloc[i] < df_final["PRICE"].iloc[i] < df_final["BEST OFR"].iloc[i]:
            df_final['Quote Position'].iloc[i] = "Inside Quote"
        elif df_final["BEST BID"].iloc[i] == df_final["PRICE"].iloc[i] or df_final["PRICE"].iloc[i] == df_final["BEST OFR"].iloc[i] :
            df_final['Quote Position'].iloc[i] = "At Quote"
        elif df_final["BEST BID"].iloc[i] > df_final["PRICE"].iloc[i] or df_final["PRICE"].iloc[i] > df_final["BEST OFR"].iloc[i] :
            df_final['Quote Position'].iloc[i] = "Outside Quote"            
    
    else:
        Best_MM = df_final['BEST MM'].iloc[i]
        
        if df_final["BID_"+Best_MM].iloc[i] < df_final["PRICE"].iloc[i] < df_final["OFR_"+Best_MM].iloc[i]:
            df_final['Quote Position'].iloc[i] = "Inside Quote"
        elif df_final["BID_"+Best_MM].iloc[i] == df_final["PRICE"].iloc[i] or df_final["PRICE"].iloc[i] == df_final["OFR_"+Best_MM].iloc[i] :
            df_final['Quote Position'].iloc[i] = "At Quote"
        elif df_final["BID_"+Best_MM].iloc[i] > df_final["PRICE"].iloc[i] or df_final["PRICE"].iloc[i] > df_final["OFR_"+Best_MM].iloc[i] :
            df_final['Quote Position'].iloc[i] = "Outside Quote"          

        
df_bar = pd.DataFrame(df_final['Quote Position'].value_counts() / len(df_final['Quote Position']) * 100)

plt.bar(df_bar.index, df_bar['Quote Position'], color =['#045851','#24AFC8','#E1B81F'], width = 0.5)
plt.xticks(rotation = 45)


df_final['FINAL MID'] = df_final['MID']

for i in range(len(df_final.index)):
    if df_final["BEST BID POSITION"].iloc[i] != df_final["BEST OFR POSITION"].iloc[i]:
        if df_final["BEST OFR POSITION"].iloc[i] == df_final["BEST MM"].iloc[i]:
            df_final['FINAL MID'].iloc[i] = df_final['MID 2'].iloc[i]

# STEP 5 :      

df_final['EFFECTIVE SPREAD'] = 0

for i in range(1, len(df_final.index)):
    Pj = df_final['PRICE'].iloc[i]
    bench = df_final['FINAL MID'].iloc[i - 1]
    if df_final['BUYER INITIATED TRADE'].iloc[i] == 1:
        Dj = 1
    else:
        Dj = -1
    df_final['EFFECTIVE SPREAD'].iloc[i] = 2 * Dj * (np.log(Pj) - np.log(bench))
    

# STEP 7 : EFFECTIVE SPREAD AND ORDER FLOW IMBALANCE TS

df_final['WEIGHT'] = df_final['SIZE'] / df_final['SIZE'].sum()
df_final['EFFECTIVE SPREAD AVG'] = df_final['EFFECTIVE SPREAD'] * df_final['WEIGHT']


BIG = df_final.groupby('DATE')['BUYER INITIATED TRADE'].sum()
BIG = pd.DataFrame(BIG)

SIG = np.abs(df_final.groupby('DATE')['SELLER INITIATED TRADE'].sum())
SIG = pd.DataFrame(SIG).reset_index()

ES = df_final.groupby('DATE')[['EFFECTIVE SPREAD AVG']].sum()
ES = pd.DataFrame(ES).reset_index()

df_EF_OFI = BIG.merge(SIG, how = 'left', on = 'DATE')
df_EF_OFI = df_EF_OFI.merge(ES, how = 'left', on = 'DATE')
df_EF_OFI['OFI'] = (np.abs(df_EF_OFI['BUYER INITIATED TRADE'] - df_EF_OFI['SELLER INITIATED TRADE'])) / ((df_EF_OFI['BUYER INITIATED TRADE'] + df_EF_OFI['SELLER INITIATED TRADE']) / 2)
df_EF_OFI

df_Most_active_MM_trades = pd.DataFrame(df_final['BEST MM'].value_counts()).reset_index()
most_active_MM = df_Most_active_MM_trades['index'][0]

print(' MOST ACTIVE MM IN TERMS OF TRADES IS :',most_active_MM)






