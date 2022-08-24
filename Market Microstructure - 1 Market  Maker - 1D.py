# -*- coding: utf-8 -*-
"""

Market Microstructure Project 1 Market Maker - 1 Day 
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

df_quotes = pd.read_excel("CHV_quotes_full.xlsx")
df_trades = pd.read_excel("CHV_trades_full.xlsx")

# Vérifier le shape de chaque Market Maker

for i in df_quotes["MMID"].unique() : 
    print(i,df_quotes.loc[df_quotes["MMID"]==i].shape)
    
# Prendre TRIM en tant qur Market Maker
df_quotes = df_quotes.loc[df_quotes["MMID"]=="TRIM"]

# Nettoyage + Filtrage

df_quotes = df_quotes.loc[~(df_quotes["BID"]==0) & ~(df_quotes["OFR"]==0)].reset_index()

df_quotes = df_quotes.loc[df_quotes["DATE"]<=df_quotes.iloc[0]["DATE"]] # NE PAS LANCER SI VOUS SOUHAITEZ AVOIR BCP DE DONNEES 

df_quotes["TIME"]=df_quotes["TIME"].astype("str")
df_quotes["HOUR"]=df_quotes["TIME"].str[0:2]
df_quotes["MINUTE"]=df_quotes["TIME"].str[3:5]
df_quotes["SECOND"]=df_quotes["TIME"].str[6:8]

for i in list(df_quotes.columns)[-3:]: 
    df_quotes[i]=df_quotes[i].astype("int64")
    
df_trades = df_trades.copy(deep = False)

df_trades = df_trades.loc[df_trades["DATE"]<=df_trades.iloc[0]["DATE"]]  # NE PAS LANCER SI VOUS SOUHAITEZ AVOIR BCP DE DONNEES 

df_trades["TIME"]=df_trades["TIME"].astype("str")
df_trades["HOUR"]=df_trades["TIME"].str[0:2]
df_trades["MINUTE"]=df_trades["TIME"].str[3:5]
df_trades["SECOND"]=df_trades["TIME"].str[6:8]

for i in list(df_trades.columns)[-3:]: 
    df_trades[i]=df_trades[i].astype("int64")
    
df_trades = df_trades.loc[(df_trades["MINUTE"]>=df_quotes.iloc[0]["MINUTE"])&(df_trades["SECOND"]>=df_quotes.iloc[0]["SECOND"])].reset_index(drop = True)

# Merge les Deux Data Frame 1 JOUR

for i in list(df_quotes.columns)[-3:]: 
    df_quotes[i]=df_quotes[i].astype("str")
for i in list(df_trades.columns)[-3:]: 
    df_trades[i]=df_trades[i].astype("str")
    
    
df_trades["DATE"]=df_trades["DATE"].astype("str")
df_quotes["DATE"]=df_quotes["DATE"].astype("str")

df_trades["DATE_TIME"]=df_trades["DATE"]+" "+df_trades["HOUR"]+":"+df_trades["MINUTE"]+":"+df_trades["SECOND"]

df_quotes["DATE_TIME"]=df_quotes["DATE"]+" "+df_quotes["HOUR"]+":"+df_quotes["MINUTE"]+":"+df_quotes["SECOND"]

df_trades["DATE_TIME"]=pd.to_datetime(df_trades["DATE_TIME"])
df_quotes["DATE_TIME"]=pd.to_datetime(df_quotes["DATE_TIME"])

df_final = pd.merge_asof(df_trades,df_quotes, on="DATE_TIME", tolerance = pd.Timedelta(days = 1),allow_exact_matches = False).loc[~(pd.merge_asof(df_trades,df_quotes, on="DATE_TIME", tolerance = pd.Timedelta(days = 1),allow_exact_matches = False )["TIME_y"].isna())]

df_final.reset_index(drop = True, inplace = True)
df_final.drop(columns="index",inplace = True)

# Retirer les X et les Y du DataFrame
New_Col=[]
for i in df_final.columns : 
    if "_x" in i : 
        i = i[:-1]+"df_trades"
    elif "_y" in i : 
        i = i[:-1]+"df_quotes"
    else : 
        i = i
    New_Col.append(i)
df_final.columns = New_Col

# Filtrer sur les Données Secondes - 1 
df_final.loc[~(df_final["DATE_df_trades"].str[:]+df_final["TIME_df_trades"].str[:]==df_final["DATE_df_quotes"].str[:]+df_final["TIME_df_quotes"].str[:])].reset_index(drop = True)

# Get MID SPREAD, SPREAD , 20%, 30% OF SPREAD 

df_final["SPREAD"]=df_final["OFR"]-df_final["BID"]
df_final["MID SPREAD"]=(df_final["OFR"]-df_final["BID"])/2

df_final["SPREAD_20_PERCENT"]=0.2*df_final["SPREAD"]
df_final["SPREAD_30_PERCENT"]=0.3*df_final["SPREAD"]

# Algorithm CLNV
tick_rule_liste=[]
quote_rule_liste=[]
for i, j, k, m in zip(df_final["PRICE"], df_final["BID"], df_final["OFR"],df_final["SPREAD_30_PERCENT"]) : 
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
    
df_final["TICK RULE"] = tick_rule_liste
df_final["QUOTE RULE"] = quote_rule_liste


df_final["BUYER INITIATED TRADE"] = 0
df_final["SELLER INITIATED TRADE"] = 0

# TICK RULE TRADE DIRECTION

for i in range(1,df_final.shape[0],1) : 
    if df_final.iloc[i,-4]==1 : 
        
        if df_final.iloc[i,3] > df_final.iloc[i-1,3] : 
            df_final.iloc[i,-2]=1
            df_final.iloc[i,-1]=0             
            
        elif df_final.iloc[i,3] < df_final.iloc[i-1,3] : 
            df_final.iloc[i,-1]=-1 
            df_final.iloc[i,-2]=0
                
        elif df_final.iloc[i,3] == df_final.iloc[i-1,3] : 
            
            for j in range(i-1, 0, -1) : 
                
                if df_final.iloc[i,3] > df_final.iloc[j,3] :
                    df_final.iloc[i,-2]=1
                    df_final.iloc[i,-1]=0
                    break
                    
                elif df_final.iloc[i,3] < df_final.iloc[j,3] : 
                    df_final.iloc[i,-1]=-1
                    df_final.iloc[i,-2]=0
                    break

df_final["MID"]=(df_final["OFR"]+df_final["BID"])/2

# QUOTE RULE TRADE DIRECTION
for i in range(0,df_final.shape[0]) : 
    if df_final.iloc[i,-4] == 1 : 
        if df_final.iloc[i,3] > df_final.iloc[i,-1] : 
            df_final.iloc[i,-3] =1
            df_final.iloc[i,-2] = 0
            
        elif df_final.iloc[i,3] < df_final.iloc[i,-1] : 
            df_final.iloc[i,-2] = -1
            df_final.iloc[i,-3] = 0
        else : 
            df_final.iloc[i,-2] = np.nan
            df_final.iloc[i,-3] = np.nan
            
df_final[(df_final["SELLER INITIATED TRADE"] == np.nan )| (df_final["BUYER INITIATED TRADE"] == np.nan)]            


# STEP 4 : PROPORTION OF INSIDE/ OUTSIDE / AT QUOTES           
df_final['Quote Position'] = ''
for i in range(0, df_final.shape[0]):
    if df_final.iloc[i,16] < df_final.iloc[i,3] < df_final.iloc[i,17]:
        df_final.iloc[i,-1] = "Inside Quote"
    elif df_final.iloc[i,16] == df_final.iloc[i,3] or df_final.iloc[i,3] == df_final.iloc[i,17] :
        df_final.iloc[i,-1] = "At Quote"
    elif df_final.iloc[i,16] < df_final.iloc[i,3] or df_final.iloc[i,3] > df_final.iloc[i,17] :
        df_final.iloc[i,-1] = "Outside Quote"            
            
df_bar = pd.DataFrame(df_final['Quote Position'].value_counts() / len(df_final['Quote Position']) * 100)

plt.bar(df_bar.index, df_bar['Quote Position'], color =['#045851','#24AFC8','#E1B81F'], width = 0.5)
            
# STEP 5 :
            
df_final["BIDSIZ"] = df_final["BIDSIZ"] * 100
df_final["OFRSIZ"] = df_final["OFRSIZ"] * 100

df_final['EFFECTIVE SPREAD'] = 0
for i in range(1, len(df_final.index)):
    Pj = df_final['PRICE'].iloc[i]
    bench = df_final['MID'].iloc[i - 1]
    if df_final['BUYER INITIATED TRADE'].iloc[i] == 1:
        Dj = 1
    else:
        Dj = -1
    df_final['EFFECTIVE SPREAD'].iloc[i] = 2 * Dj * (np.log(Pj) - np.log(bench))
    
# STEP 7 : EFFECTIVE SPREAD AND ORDER FLOW IMBALANCE TS

df_final['WEIGHT'] = df_final['SIZE'] / df_final['SIZE'].sum()
df_final['EFFECTIVE SPREAD AVG'] = df_final['EFFECTIVE SPREAD'] * df_final['WEIGHT']

BIG = df_final.groupby('DATE_df_trades')['BUYER INITIATED TRADE'].sum()
BIG = pd.DataFrame(BIG)

SIG = np.abs(df_final.groupby('DATE_df_trades')['SELLER INITIATED TRADE'].sum())
SIG = pd.DataFrame(SIG).reset_index()

ES = df_final.groupby('DATE_df_trades')['EFFECTIVE SPREAD AVG'].sum()
ES = pd.DataFrame(ES).reset_index()

df_EF_OFI = BIG.merge(SIG, how = 'left', on = 'DATE_df_trades')
df_EF_OFI = df_EF_OFI.merge(ES, how = 'left', on = 'DATE_df_trades')
df_EF_OFI['OFI'] = (np.abs(df_EF_OFI['BUYER INITIATED TRADE'] - df_EF_OFI['SELLER INITIATED TRADE'])) / ((df_EF_OFI['BUYER INITIATED TRADE'] + df_EF_OFI['SELLER INITIATED TRADE']) / 2)
df_EF_OFI
          
            
            
            
            