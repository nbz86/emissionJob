import io,os
import urllib.request
import xml.etree.ElementTree as ET
import pandas as pd

DOMAIN_ID_DATA_URL='https://raw.githubusercontent.com/cansyl/ECPred/master/ECPred%20Datasets/EC_MainClass_PositiveTrain/Hydrolases_Positive_Train.Txt'
UNIPROT_XML_DATA_PULL_BASE_URL='https://rest.uniprot.org/uniprotkb'

def helperReadOnlineXmlContent(xml_link=None):
    url_response = urllib.request.urlopen(xml_link)
    xml_content = url_response.read()  ## read the content as bytes format
    xml_content_str = xml_content.decode("utf-8")  # convert bytes to string format
    return xml_content_str

def helperDetectRootFromGivenXmlString(xml_string=None):
    xml_content = io.StringIO(xml_string)
    xml_tree = ET.parse(xml_content)
    xml_root = xml_tree.getroot()
    return xml_root

def helperParseGivenElementsChildren(given_element=None):
    # Creating an empty dictionary
    position_dict = {}
    for child_element in given_element:
        for sub_children in child_element:
            #print(sub_children.tag, sub_children.attrib)
            #print(sub_children.tag.split('}')[-1], sub_children.attrib['position'])
            # Add a key-value pair to empty dictionary in python
            position_dict[sub_children.tag.split('}')[-1]] = sub_children.attrib['position']
    print(position_dict)
    return position_dict

def helperXmlDictContentToDfFormatter(dict_data=None):
    df = pd.DataFrame(dict_data.items()).T  # or list(d.items()) in python 3
    df.columns=df.iloc[0].values # use first row values as column names
    df=df.drop(index=0) # drop first row from pandas
    print(df.head())
    return df

def helperJoinTwoPandasDfColumnWise(df1=None,df2=None):
    join_df = pd.concat([df1, df2], axis=1)
    return join_df

def helperJoinTwoPandasDfRowWise(df1=None,df2=None):
    join_df = pd.concat([df1, df2])
    return join_df

def helperDomainListIdReaderFromWebContent(data_url=None):
    data = urllib.request.urlopen(data_url).read().decode('utf-8')
    data_list = data.split('\n')[1:] # remove header from data
    data_list = [x for x in data_list if x] # remove empty elems if exists
    print(data_list)
    return data_list

def helperUniprotXmlContentLinkBuilder(base_url=None,uniprot_id=None):
    xml_file_name='%s.xml'%(uniprot_id)
    xml_link=os.path.join(base_url,xml_file_name)
    #print(xml_link)
    return xml_link

### WORKER FUNCTION DEFINITIONS
def workerParseGivenElementFromXmlRoot(xml_root=None,elem_to_parse='{http://uniprot.org/uniprot}feature',search_key='domain'):
    feature_holder_df=pd.DataFrame()
    position_holder_df=pd.DataFrame()
    for given_element in xml_root.iter(elem_to_parse):
        dict_value = given_element.attrib
        if dict_value['type']==search_key:
            print(dict_value)
            print(given_element)
            # df=pd.DataFrame(dict_value.items()).T  # or list(d.items()) in python 3
            # print(df.head())
            feature_df=helperXmlDictContentToDfFormatter(dict_data=dict_value)
            feature_holder_df=pd.concat([feature_holder_df, feature_df])
            position_dict=helperParseGivenElementsChildren(given_element=given_element)
            position_df=helperXmlDictContentToDfFormatter(dict_data=position_dict)
            position_holder_df = pd.concat([position_holder_df, position_df])
    # build output df for given id
    domain_data_df=helperJoinTwoPandasDfColumnWise(df1=feature_holder_df,df2=position_holder_df)
    print(domain_data_df)
    return domain_data_df

def workerParseOrganismElementFromXmlRoot(xml_root=None,elem_to_parse='{http://uniprot.org/uniprot}name',search_keys=['scientific','common']):
    # Creating an empty dictionary
    organism_dict = {}
    for given_element in xml_root.iter(elem_to_parse):
        dict_value = given_element.attrib
        if len(dict_value):
            #print(dict_value)
            if dict_value['type'] in search_keys:
                # print(dict_value['type'])
                # print(given_element.text)
                # Add a key-value pair to empty dictionary in python
                organism_dict['%s_name'%(dict_value['type'])] = given_element.text
                print(organism_dict)
                #df=pd.DataFrame(dict_value.items()).T  # or list(d.items()) in python 3
                # print(df.head())

    organism_df=helperXmlDictContentToDfFormatter(dict_data=organism_dict)
    return organism_df

### LOOPER FUNCTION DEFINITIONS
def looperUniprotDomainIdPositionFinder(data_url=None,base_url=None):
    domain_df_holder=pd.DataFrame()
    # job 1. pull corresponding uniprot ids from web
    uniprot_ids = helperDomainListIdReaderFromWebContent(data_url=data_url)
    for uniprot_id in uniprot_ids:
        try:
            xml_link=helperUniprotXmlContentLinkBuilder(base_url=base_url, uniprot_id=uniprot_id)
            xml_content_str = helperReadOnlineXmlContent(xml_link=xml_link)
            xml_root = helperDetectRootFromGivenXmlString(xml_string=xml_content_str)
            domain_df = workerParseGivenElementFromXmlRoot(xml_root=xml_root)
            organism_df=workerParseOrganismElementFromXmlRoot(xml_root=xml_root)
            if len(domain_df):
                domain_df['UniProt_id']=uniprot_id
                domain_df=helperJoinTwoPandasDfColumnWise(df1=domain_df,df2=organism_df)
                domain_df_holder=helperJoinTwoPandasDfRowWise(df1=domain_df_holder,df2=domain_df)
                domain_df_holder.to_csv('domain_list.csv',index=False)
            print('-------------')
        except:
            print('An error has occured on ID:%s parse job...'%(uniprot_id))
            continue
    return None

### CALL THE MAIN FUNCTION
looperUniprotDomainIdPositionFinder(data_url=DOMAIN_ID_DATA_URL,base_url=UNIPROT_XML_DATA_PULL_BASE_URL)