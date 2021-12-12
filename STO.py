import warnings
import time
import pandas as pd
import numpy as np

warnings.filterwarnings('ignore')
start_time=time.time()

#path for saving final file
path='C:\\Users\\Mudassir\\Desktop\\STO Automation\\todays\\Final.xlsx'

#################################################Function Definitions########################################################
def stage_one_stp(working):
    keys=(working['Pack']+"|"+working['Brand']+"|"+working['Plant']).drop_duplicates().to_list()
    columns_names=['Key Plant', 'Key', 'WH', 'Plant','Pack','Brand', 'Stage one STI' ,'Stage one STO']
    columns_names2=['Stage','Key Plant', 'Key', 'WH', 'Plant','Pack','Brand', 'Stage one STI' ,'Stage one STO']
    temp2=pd.DataFrame(columns=columns_names2)
    for key in keys:
        temp=working[(working['Plant']==key.split('|')[2]) & (working['Pack']==key.split('|')[0]) & (working['Brand']==key.split('|')[1])]
        system_cover=(temp['Net WH Stock-PHC']+temp['Dist Stock PHC']).sum()/temp['PD Demand PHC'].sum()
        temp['System Cover']=system_cover
        temp['Loc Cover']=temp['WH Cover Days']+temp['Dist Cover Days']
        temp['FC Share']=temp['PD Demand PHC']/temp['PD Demand PHC'].sum()
        temp['Stock Required']=temp['Net WH Stock-PHC'].sum()*temp['FC Share']
        temp['STO Required']=temp['Stock Required']-temp['Net WH Stock-PHC']
        temp.replace([np.inf,-np.inf,np.nan],0,inplace=True)
        temp.loc[(temp['STO Required']<0) | (temp['WH']==key.split('|')[2]) | (temp['System Cover']<temp['Loc Cover']),'STO Required']=0
        
        ############################################################################################
        '''If STO required is more than stock available then leave FC share stock at source location
        and divide rest into receiving locations as per STO share'''
        ############################################################################################
        
        if temp['STO Required'].sum()>temp.loc[(temp['WH']==temp['Plant']),'Net WH Stock-PHC'].sum():
            temp['STO Share']=temp['STO Required']/temp['STO Required'].sum()
            temp['Stage one STI']=temp['STO Share']*(1-temp.loc[(temp['WH']==temp['Plant']),'FC Share'].sum())*temp.loc[(temp['WH']==temp['Plant']),'Net WH Stock-PHC'].sum()
        else:
            temp['STO Share']=0
            temp['Stage one STI']=temp['STO Required']
        temp.loc[temp['WH']==temp['Plant'],'Stage one STO']=temp['Stage one STI'].sum()
        temp2=temp2.append(temp.loc[:,columns_names],ignore_index = True)
    working=pd.merge(working,temp2.loc[:,['Key','Stage one STI','Stage one STO']],left_on='Key',right_on='Key',how='left')
    working.replace([np.inf,-np.inf,np.nan],0,inplace=True)
    temp2.replace([np.inf,-np.inf,np.nan],0,inplace=True)
    columns=['Stage', 'WH', 'Plant','Pack','Brand', 'Stage one STI']
    columns2=['Stage','Receiving','Sending','Pack','Brand','STP-PHC']
    temp2['Stage']='One'
    temp2=temp2.loc[(temp2['Stage one STI']>0),columns]
    temp2.columns=columns2
    
    return working,temp2


def stage_two_stp(working):
    keys=(working['Pack']+"|"+working['Brand']+"|"+working['Plant2']).drop_duplicates().to_list()
    columns_names=['Key Plant', 'Key', 'WH', 'Plant2','Pack','Brand', 'Stage Two STI' ,'Stage Two STO']
    columns_names2=['Stage','Key Plant', 'Key', 'WH', 'Plant2','Pack','Brand', 'Stage Two STI' ,'Stage Two STO']
    temp2=pd.DataFrame(columns=columns_names2)
    for key in keys:
        temp=working[(working['Plant2']==key.split('|')[2]) & (working['Pack']==key.split('|')[0]) & (working['Brand']==key.split('|')[1])]
        temp['Loc Cover']=(temp['Net WH Stock-Stage One']+temp['Dist Stock PHC'])/temp['PD Demand PHC']
        temp['FC Share']=temp['PD Demand PHC']/temp['PD Demand PHC'].sum()
        if temp['Type'].drop_duplicates().to_list()[0]=='A' or temp['Type'].drop_duplicates().to_list()[0]=='B':
            temp['STO Required']=(temp['Critical Days']+1-temp['Loc Cover'])*temp['PD Demand PHC']
            temp.replace([np.inf,-np.inf,np.nan],0,inplace=True)
            temp.loc[(temp['STO Required']<0) | (temp['WH']==key.split('|')[2]) ,'STO Required']=0
        
            ############################################################################################
            '''If STO required is more than stock available then leave FC share stock at source location
            and divide rest into receiving locations as per STO share'''
            ############################################################################################
            if temp['STO Required'].sum()>temp.loc[(temp['WH']==temp['Plant2']),'Net WH Stock-Stage One'].sum():
                temp['STO Share']=temp['STO Required']/temp['STO Required'].sum()
                temp['Stage Two STI']=temp['STO Share']*(1-temp.loc[(temp['WH']==temp['Plant2']),'FC Share'].sum())*temp.loc[(temp['WH']==temp['Plant2']),'Net WH Stock-Stage One'].sum()
            else:
                temp['STO Share']=0
                temp['Stage Two STI']=temp['STO Required']
            temp.loc[temp['WH']==temp['Plant2'],'Stage Two STO']=temp['Stage Two STI'].sum()
            temp2=temp2.append(temp.loc[:,columns_names],ignore_index = True)
            
    working=pd.merge(working,temp2.loc[:,['Key','Stage Two STI','Stage Two STO']],left_on='Key',right_on='Key',how='left')
    working.replace([np.inf,-np.inf,np.nan],0,inplace=True)
    temp2.replace([np.inf,-np.inf,np.nan],0,inplace=True)
    columns=['Stage', 'WH', 'Plant2','Pack','Brand', 'Stage Two STI']
    columns2=['Stage','Receiving','Sending','Pack','Brand','STP-PHC']
    temp2['Stage']='Two'
    temp2=temp2.loc[(temp2['Stage Two STI']>0),columns]
    temp2.columns=columns2
    return working,temp2

def stage_three_stp(working):
    freight_matrix=pd.read_excel('Freight matrix.xlsx')
    freight_matrix.sort_values(by='Freight',inplace=True)
    keys=(working['Pack']+"|"+working['Brand']).drop_duplicates().to_list()
    columns_names=['Stage','Receiving', 'Sending','Pack','Brand', 'STP-PHC']
    temp2=pd.DataFrame(columns=columns_names)
    working['Stage Three STI']=0
    working['Stage Three STO']=0
    working['Net WH Stock-Stage Three']=working['Net WH Stock-Stage Two']
    for key in keys:
        temp=working[(working['Pack']==key.split('|')[0]) & (working['Brand']==key.split('|')[1])]
        temp['Loc Cover']=(temp['Net WH Stock-Stage Two']+temp['Dist Stock PHC'])/temp['PD Demand PHC']
        temp['FC Share']=temp['PD Demand PHC']/temp['PD Demand PHC'].sum()
        if temp['Type'].drop_duplicates().to_list()[0]=='C':
            temp['STO Required']=(temp['Critical Days']+1-temp['Loc Cover'])*temp['PD Demand PHC']
            temp.replace([np.inf,-np.inf,np.nan],0,inplace=True)
            temp.loc[(temp['STO Required']<0) ,'STO Required']=0
            temp['Excess stock']=(temp['Loc Cover']-temp['Max Days'])*temp['PD Demand PHC']
            temp.loc[(temp['Excess stock']<0) ,'Excess stock']=0
            temp['Stage Three STO']=0
            temp['Stage Three STI']=0
            temp['Net WH Stock-Stage Three']=temp['Net WH Stock-Stage Two']
            temp.loc[temp['Excess stock']>temp['Net WH Stock-Stage Three'],'Excess stock']=temp.loc[temp['Excess stock']>temp['Net WH Stock-Stage Three'],'Net WH Stock-Stage Three']
            if (temp['Excess stock'].sum()==0) or (temp['STO Required'].sum()==0):
                continue
            else:
                stock_required=temp.loc[temp['STO Required']>0,:]
                can_dispatch=temp.loc[temp['Excess stock']>0,:]
                for receiving in stock_required['WH']:
                    can_dispatch=temp.loc[temp['Excess stock']>0,:]
                    if (temp.loc[temp['WH']==receiving,'STO Required']<=0).values[0]:
                        continue
                    else:
                        freight=freight_matrix.loc[freight_matrix['Receiving']==receiving,:]
                        can_dispatch=pd.merge(can_dispatch,freight[['Sending','Freight']],left_on='WH',right_on='Sending',how='left')
                        can_dispatch.sort_values(by='Freight',inplace=True)
                        for sending in can_dispatch['WH']:
                            if (temp.loc[temp['WH']==receiving,'STO Required']<=0).values[0]:
                                break
                            elif (temp.loc[temp['WH']==sending,'Excess stock']<=0).values[0]:
                                continue
                            else:
                                transaction=min(temp.loc[temp['WH']==receiving,'STO Required'].values[0],temp.loc[temp['WH']==sending,'Excess stock'].values[0])
                                temp.loc[temp['WH']==receiving,'Stage Three STI']+=transaction
                                temp.loc[temp['WH']==receiving,'Net WH Stock-Stage Three']+=transaction
                                temp.loc[temp['WH']==receiving,'STO Required']-=transaction
                                temp.loc[temp['WH']==sending,'Stage Three STO']+=transaction
                                temp.loc[temp['WH']==sending,'Net WH Stock-Stage Three']-=transaction
                                temp.loc[temp['WH']==sending,'Excess stock']-=transaction

                                working.loc[(working['WH']==receiving)&(working['Pack']==key.split('|')[0]) & (working['Brand']==key.split('|')[1]),'Stage Three STI']+=transaction
                                working.loc[(working['WH']==receiving)&(working['Pack']==key.split('|')[0]) & (working['Brand']==key.split('|')[1]),'Net WH Stock-Stage Three']+=transaction
                                working.loc[(working['WH']==sending)&(working['Pack']==key.split('|')[0]) & (working['Brand']==key.split('|')[1]),'Stage Three STO']+=transaction
                                working.loc[(working['WH']==sending)&(working['Pack']==key.split('|')[0]) & (working['Brand']==key.split('|')[1]),'Net WH Stock-Stage Three']-=transaction

                                values=['Three',receiving,sending,key.split('|')[0],key.split('|')[1],transaction]
                                app=pd.DataFrame([values],columns=columns_names)
                                temp2=temp2.append(app,ignore_index = True)
    return working,temp2
####################################################End of Functions####################################################

#############Data Import#########################
pd.options.display.float_format = "{:,.2f}".format
conversion = pd.read_excel('Conversion UC to PHC.xlsx')
forecast = pd.read_excel('Sellout RE.xlsx')
fg_key = pd.read_excel('FG Keys.xlsx')
wh_key =  pd.read_excel('Location Keys.xlsx')
plant_key = pd.read_excel('Plant Key.xlsx')
stock = pd.read_excel('stock.xlsx')
transit = pd.read_excel('Transit.xlsx')
sto = pd.read_excel('STO.xlsx')
veh_key = pd.read_excel('Vehicle Key.xlsx')
permutation = pd.read_excel('Permutation.xlsx')
dist_stock = pd.read_excel('CCI Warehouse & Distributor Stock Status - PK.xlsx',sheet_name='DAR PK-Dist')
working = permutation.copy(deep=True)
dist_sourcing=pd.read_excel('Dist_sourcing.xlsx')


######import forecast in working###########
forecast.loc[:,'Key']=forecast.loc[:,'SKU']+forecast.loc[:,'Warehouse']

temp=pd.merge(working.loc[:,'Key'],forecast.groupby('Key')['July SO RE'].sum(),on='Key',how='left')
working['SO RE']=temp['July SO RE'].fillna(0)

#############end of import###################

#####Per Day Forecast######    
working.loc[:,'PD Demand']=working.loc[:,'SO RE']/working.loc[:,'CM Days']

##########Converting per day forecast to PHC##############
temp = pd.merge(working.loc[:,'Pack'],conversion.loc[:,['Pack','UC']],on='Pack',how='left')
working['PD Demand PHC']=working['PD Demand']/temp['UC']

##Importing disposable stock to working sheet
stock['Material']=stock['Material'].apply(lambda x : int(x.replace("-","")))
stock = pd.merge(stock,fg_key,left_on='Material',right_on='Article Number',how='left')
stock['Plant']=stock['Storage Location'].apply(str) + stock['Plant'].apply(str)
stock['Plant']=stock['Plant'].apply(int)
stock = pd.merge(stock,plant_key.loc[:,['Key','Name']],left_on='Plant',right_on='Key',how='left')
stock['Plant']=stock['Plant'].apply(str)
stock['Key']=stock['Pack']+stock['Brand']+stock['Name']
stock['PHC']=stock['Unristricted Stock']-stock['Open Order with Confirmation']-stock['Delivery Goods not Issued PHC']+stock['Production Plan with Opened Order']

temp=pd.merge(working.loc[:,'Key'],stock.groupby('Key')['PHC'].sum(),on='Key',how='left')
working['Stock']=temp['PHC'].fillna(0)
###End of import stock#######

##Importing transit stock to working sheet
transit['Material']=transit['Material'].apply(lambda x:int(x.replace("-","")))
transit = pd.merge(transit,fg_key,left_on='Material',right_on='Article Number',how='left')
transit=pd.merge(transit,plant_key.loc[:,['Plant','Name']].drop_duplicates(),on='Plant',how='left')
transit['Key']=transit['Pack']+transit['Brand']+transit['Name']
transit['PHC']=transit['Quantity']

temp=pd.merge(working.loc[:,'Key'],transit.groupby('Key')['PHC'].sum(),on='Key',how='left')
working['Transit']=temp['PHC'].fillna(0)
###End of import transit#######

##Importing stock transfer orders
sto['Material']=sto['Material'].apply(lambda x : int(x.replace("-","")))
sto = pd.merge(sto,fg_key,left_on='Material',right_on='Article Number',how='left')
sto['Sending']=sto['Issuing Storage Loc.'].apply(str) + sto['Vendor/supplying plant'].apply(lambda x:x[0:4])
sto['Sending']=pd.merge(sto['Sending'].apply(int),plant_key.loc[:,['Key','Name']],left_on='Sending',right_on='Key',how='left')['Name']

sto['Receiving']=sto['Storage Location'].apply(str) + sto['Plant'].apply(str)
sto['Receiving']=pd.merge(sto['Receiving'].apply(int),plant_key.loc[:,['Key','Name']],left_on='Receiving',right_on='Key',how='left')['Name']

sto['Key-Receiving']=sto['Pack']+sto['Brand']+sto['Receiving']
sto['Key-Sending']=sto['Pack']+sto['Brand']+sto['Sending']
sto['PHC']=sto['Order Quantity']-sto['Qty Delivered']
sto.loc[sto.loc[:,'Sending']==sto.loc[:,'Receiving'],'PHC']=0

working['STO']=pd.merge(working['Key'],sto.groupby('Key-Sending')['PHC'].sum(),left_on = 'Key',right_on='Key-Sending',how='left')['PHC']
working['STI']=pd.merge(working['Key'],sto.groupby('Key-Receiving')['PHC'].sum(),left_on = 'Key',right_on='Key-Receiving',how='left')['PHC']

working.loc[:,['STO','STI']]=working.loc[:,['STO','STI']].fillna(0)
###End of STO/STI import#######

#################Dist Stock Import##############################
new_header = dist_stock.iloc[0]
dist_stock=dist_stock[1:]
dist_stock.columns = new_header
dist_stock=dist_stock.iloc[:,1:]
dist_stock=dist_stock.fillna(0)
dist_stock=dist_stock[:-1]
dist_stock['L01 Article Number Key']=dist_stock['L01 Article Number Key'].apply(int)
dist_stock=pd.merge(dist_stock,fg_key,left_on='L01 Article Number Key',right_on='Article Number',how='left')
dist_stock['L01 Delivery Location Key']=dist_stock['L01 Delivery Location Key'].apply(lambda x:x[len(x)-2:len(x):1])
dist_stock['Key']=dist_stock['Pack']+dist_stock['Brand']+dist_stock['L01 Delivery Location Key']
dist_sourcing['Key']=dist_sourcing['Pack']+dist_sourcing['Brand']+dist_sourcing['Voyage Code']
dist_stock=pd.merge(dist_stock,dist_sourcing.loc[:,['Key','Warehouse']],how='left')
dist_stock['Key2']=dist_stock['Pack']+dist_stock['Brand']+dist_stock['Warehouse']
working['Dist Stock PHC']=pd.merge(working['Key'],dist_stock.groupby('Key2')['End of Day Stock PHC'].sum(),left_on='Key',right_on='Key2',how='left')['End of Day Stock PHC']
working['Dist Stock PHC']=working['Dist Stock PHC'].fillna(0)
#####################End of Section##################################



#########Stock and Cover days for WH#################
working['Net WH Stock-PHC']=working['Stock']+working['Transit']+working['STI']-working['STO']
working['WH Cover Days']=working['Net WH Stock-PHC']/working['PD Demand PHC']
working['WH Cover Days'].replace([np.inf,-np.inf,np.nan],0,inplace=True)
##########End of Section###########################

#########Stock and Cover days for Dist#################
working['Dist Cover Days']=working['Dist Stock PHC']/working['PD Demand PHC']
working['Dist Cover Days'].replace([np.inf,-np.inf,np.nan],0,inplace=True)
##########End of Section###########################


working,stageonestp=stage_one_stp(working)
working['Net WH Stock-Stage One']=working['Net WH Stock-PHC']+working['Stage one STI']-working['Stage one STO']
working,stagetwostp=stage_two_stp(working)
working['Net WH Stock-Stage Two']=working['Net WH Stock-Stage One']+working['Stage Two STI']-working['Stage Two STO']
working,stagethreestp=stage_three_stp(working)
writer = pd.ExcelWriter(path, engine = 'xlsxwriter')

total=stageonestp.append(stagetwostp,ignore_index=True)
total=total.append(stagethreestp,ignore_index=True)

working.to_excel(writer, sheet_name = 'Working',index=False)
total.to_excel(writer,sheet_name='Total STP',index=False)

#########################Writing Output with Formatting###########################
workbook  = writer.book
format1 = workbook.add_format({'num_format': '#,##0'})
worksheet = writer.sheets['Working']
worksheet.set_column('I:AF', 12, format1)
worksheet = writer.sheets['Total STP']
worksheet.set_column('F:F', 12, format1)
writer.save()
writer.close()
##################################################################################
end_time=time.time()
total_time=end_time-start_time
print(total_time)