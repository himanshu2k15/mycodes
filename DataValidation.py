from ayx import Alteryx
import pandas as pd
pd.options.mode.chained_assignment = None

sumry= pd.DataFrame({'dummy':[1],'dummy2':[1]})

CRM=Alteryx.read("#1")
SNW=Alteryx.read("#2")
PRI=Alteryx.read("#3")  #pri_key = ['CUSTOMER_AFFILIATION_ID', 'SOURCE_SYSTEM', 'COUNTRY_ISO2_CODE']
Sample=Alteryx.read("#4")




pri_key =PRI['Primary_Keys'].tolist()
samp_size=list(Sample['Sample_Size'])[0]

if len(CRM)!=0 or len(SNW)!=0:
    print(' Records availanble in SRC or TGT')
    


    #print('CRM Columns: ',CRM.columns)
    #print('SNW Columns: ',SNW.columns)
    

    all_col = CRM.columns.tolist()


    sumry['SRC_CNT']=len(CRM)
    sumry['SRC_CNT_Duplicate']=len( CRM[CRM.duplicated()])
    sumry['TGT_CNT']=len(SNW)
    sumry['TGT_CNT_Duplicate']=len( SNW[SNW.duplicated()])



    pk_CRM = CRM.groupby(pri_key)
    pk_size_CRM = pk_CRM.size().reset_index() 
    sumry['Duplicates_ON_PK_SRC']=len(pk_size_CRM[pk_size_CRM[0] > 1])
    print('Duplicates_ON_PK_SRC : ',sumry['Duplicates_ON_PK_SRC'])

    pk_SNW = SNW.groupby(pri_key)
    pk_size_SNW = pk_SNW.size().reset_index() 
    sumry['Duplicates_ON_PK_TGT']=len(pk_size_SNW[pk_size_SNW[0] > 1])
    print('Duplicates_ON_PK_TGT : ',sumry['Duplicates_ON_PK_TGT'])

    for col in all_col:
            sumry['NULL_in_'+col+'_SRC']=CRM[col].isna().sum()
            sumry['NULL_in_'+col+'_TGT']=SNW[col].isna().sum()
            #if col =='COUNTRY_ISO2_CODE':
                #print(col ,' null value in SRC - ',str(CRM[col].isna().sum()))
            

        
    all_col.sort()

    for key in pri_key:
        CRM[key+'_CHANGE_SRC']=CRM[key]
        SNW[key+'_CHANGE_TGT']=SNW[key]

    print('primar key -',pri_key)
    #print('All Columns key -',all_col)

    samples=pd.DataFrame(columns=all_col)
    samples['Mismatch']=''
    samples['Type']=''
    col_test = list(set(all_col).difference(set(pri_key) ))
    
    CRM[col_test] = CRM[col_test].astype(str)
    SNW[col_test] = SNW[col_test].astype(str)

    #print('Column to Test - ', col_test)
    df_joined_all = CRM.merge(SNW,how='outer',on=pri_key,suffixes=('_CRM', '_SNW'))
    df_joined = CRM.merge(SNW,how='inner',on=pri_key,suffixes=('_CRM', '_SNW'))
    print('After joining Number of Rows - ',len(df_joined))
    df_joined.columns =df_joined.columns.str.replace(r"_CHANGE_SRC", "_CRM")
    df_joined.columns =df_joined.columns.str.replace(r"_CHANGE_TGT", "_SNW")
    df_joined_all.columns =df_joined_all.columns.str.replace(r"_CHANGE_SRC", "_CRM")
    df_joined_all.columns =df_joined_all.columns.str.replace(r"_CHANGE_TGT", "_SNW")
    null_src = []
    null_tgt =[]
    for i in pri_key:
        col_nm_src=i+'_CRM'
        col_nm_tgt=i+'_SNW'
        src_pk_null=df_joined_all[col_nm_src].isna().sum()
        tgt_pk_null=df_joined_all[col_nm_tgt].isna().sum()
        null_src.append(src_pk_null)
        null_tgt.append(tgt_pk_null)
        
    sumry['ROWS_AFTER_JOIN_NP_SRC']=max(null_src)
    sumry['ROWS_AFTER_JOIN_NP_TGT']=max(null_tgt)


    #df_joined.columns      
    for col in col_test:
        print('Comparing Column - ',col)
        #df_joined['MATCH_'+col]=df_joined[col+'_CRM']==df_joined[col+'_SNW']
        df_joined['MATCH_'+col]=df_joined[col+'_CRM'].fillna('-').eq(df_joined[col+'_SNW'].fillna('-'))
        #df['column_one'].fillna('-').eq(df['column_two'].fillna('-'))
        sumry['Mismatch_IN_'+col]=len(df_joined[df_joined['MATCH_'+col]==False])
        mismtch=df_joined[df_joined['MATCH_'+col]==False]
        #sumry['NOT_MATCH_'+col]=len(mismtch)
        
        if len(mismtch)!=0:
            print('Mismatch found on Column - ',col)
            if samp_size==0:
                rws=mismtch.copy(deep=True)
            else:
                
                rws=mismtch.head(samp_size) #mismtch.copy(deep=True)
            sample_rws_src =rws[[i+'_CRM' for i in all_col ]]
            sample_rws_src['Mismatch']='NOT_MATCH_'+col
            sample_rws_src['Type']='SRC'
            sample_rws_tgt =rws[[i+'_SNW' for i in all_col]]
            sample_rws_tgt['Mismatch']='NOT_MATCH_'+col
            sample_rws_tgt['Type']='TGT'     
            #print('Source Row Data - \n',sample_rws_src[['CUSTOMER_ID_CRM',col+'_CRM','Mismatch','Type']])
            #print('Target Row Data - \n',sample_rws_tgt[['CUSTOMER_ID_SNW',col+'_SNW','Mismatch','Type']])
            sample_rws_src.columns =sample_rws_src.columns.str.replace(r"_CRM", "") 
            sample_rws_tgt.columns =sample_rws_tgt.columns.str.replace(r"_SNW", "") 
            samples=samples.append(sample_rws_src,ignore_index=True)
            samples=samples.append(sample_rws_tgt,ignore_index=True)
            #print(samples[['CUSTOMER_ID',col,'Mismatch','Type']])
            #print('Mismatch found size SRC',len(sample_rws_src))
            #print(mismtch[['CUSTOMER_ID_CRM','MATCH_'+col,col+'_CRM',col+'_SNW']])
            #print('Columns of Source :',sample_rws_src.columns.tolist())
            #print('Columns of TGT :',sample_rws_tgt.columns.tolist())
            #print('Mismatch found size sample',len(samples))
    sumry.drop(columns=['dummy','dummy2'],inplace=True)        
    sumry=sumry.transpose().reset_index().rename(columns={'index':'Description',0:'Count'})        
    missing_rcd_in_src= df_joined_all[[i+'_SNW' for i in pri_key]][df_joined_all[[i+'_CRM' for i in pri_key]].isna().all(1)].reset_index(drop=True)
    missing_rcd_in_tgt= df_joined_all[[i+'_CRM' for i in pri_key]][df_joined_all[[i+'_SNW' for i in pri_key]].isna().all(1)].reset_index(drop=True)
    Alteryx.write(samples,1)
    Alteryx.write(sumry,2)
    Alteryx.write(missing_rcd_in_src,3)
    Alteryx.write(missing_rcd_in_tgt,4)
