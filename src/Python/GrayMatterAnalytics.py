from pyspark import  SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from functools import reduce
import sys

measure=sys.argv[1]

conf = SparkConf(). \
    setAppName("Gary Matter"). \
    setMaster('local')

sc =SparkContext(conf=conf)
sqlContext = SQLContext(sc)
sampleData=sqlContext.read.csv('/home/jasbir/Documents/PySpark_itversity/GrayMatter' , header=True)

# print(sampleData.count())
#Replacing missing values incase measure COPD
sampleData=sampleData.fillna('0')

Diagnosis_codes={'AMI': [410.00, 410.01, 410.10, 410.11, 410.20, 410.21, 410.30, 410.31, 410.40, 410.41,
                         410.50, 410.51, 410.60, 410.61, 410.70, 410.71, 410.80, 410.81, 410.90, 410.91],
                'COPD': [491.21, 491.22, 491.8, 491.9, 492.8, 493.20, 493.21, 493.22, 496, 518.81, 518.82,
                         518.84, 799.1],
                 'HF':[402.01, 402.11, 402.91, 404.01, 404.03, 404.11, 404.13, 404.91, 404.93,'428.xx'],
                 'HWR': 'CCS',
                 'PN': [480.0, 480.1, 480.2, 480.3, 480.8, 480.9, 481, 482.0, 482.1, 482.2, 482.30, 482.31,
                        482.32, 482.39, 482.40, 482.41, 482.42, 482.49, 482.81, 482.82, 482.83, 482.84,
                        482.89, 482.9, 483.0, 483.1, 483.8, 485, 486, 487.0, 488.11],
                 'THA-TKA': [81.51, 81.54]
}

# Used to get value Range for all the columns in dataset

# print(sampleData.describe().show())


colmns_to_Rplace =['HistoryofPTCA', 'HistoryofCABG', 'Congestiveheartfailure',
                   'Acutecoronarysyndrome', 'Anteriormyocardialinfarction', 'Otherlocationofmyocardialinfarction',
                   'Anginapectorisoldmyocardialinfarction', 'Coronaryatherosclerosis', 'Valvularorrheumaticheartdisease',
                   'Specifiedarrhythmias', 'Historyofinfection', 'Metastaticcanceroracuteleukemia',
                   'Cancer', 'Diabetesmellitus(DM)orDMcomplications', 'Protein-caloriemalnutrition',
                   'Disordersoffluidelectrolyteacid-base', 'Irondeficiencyorotheranemiasandblooddisease',
                   'Dementiaorotherspecifiedbraindisorders', 'Hemiplegiaparaplegiaparalysisfunctionaldisability',
                   'Stroke', 'Cerebrovasculardisease', 'Vascularorcirculatorydisease', 'Chronicobstructivepulmonarydisease', 'Asthma',
                   'Pneumonia', 'End-stagerenaldiseaseordialysis', 'Renalfailure', 'Otherurinarytractdisorders', 'Decubitusulcerorchronicskinulcer',
                   'LungUpperDigestiveTractandOtherSevereCancers', 'LymphaticHeadandNeckBrainandOtherMajorCancers;BreastColorectalandotherCancersandTumors;'
                    'OtherRespiratoryandHeartNeoplasms', 'OtherDigestiveandUrinaryNeoplasms',
                   'OtherEndocrine/Metabolic/NutritionalDisorders', 'PancreaticDisease', 'PepticUlcerHemorrhageOtherSpecifiedGastrointestinalDisorders',
                   'OtherGastrointestinalDisorders', 'SevereHematologicalDisorders', 'Drug/AlcoholInducedDependence/Psychosis', 'MajorPsychiatricDisorders', 'Depression', 'AnxietyDisorders', 'OtherPsychiatricDisorders', 'QuadriplegiaParaplegiaParalysisFunctionalDisability', 'Polyneuropathy', 'HypertensiveHeartandRenalDiseaseorEncephalopathy', 'CellulitisLocalSkinInfection', 'VertebralFractures', 'Liverandbiliarydisease', 'Fibrosisoflungandotherchroniclungdisorders', 'Nephritis', 'End-stageliverdisease', 'Seizuredisordersandconvulsions', 'Chronicheartfailure', 'Coronaryatherosclerosisoranginacerebrovasculardisease', 'Dialysisstatus', 'Septicemia/shock', 'Cardio-respiratoryfailureorcardio-respiratoryshock', 'Rheumatoidarthritisandinflammatoryconnectivetissuedisease', 'Respiratordependence/tracheostomystatus', 'Transplants', 'Coagulationdefectsandotherspecifiedhematologicaldisorders', 'Hipfracture/dislocation', 'Pleuraleffusion/pneumothorax', 'Urinarytractinfection', 'Otherinjuries', 'Skeletaldeformities',
                   'Posttraumaticosteoarthritis', 'Morbidobesity', 'Hypertension', 'Majorsymptomsabnormalities', 'HistoryofMechanicalVentilation', 'SleepApnea', 'OtherandUnspecifiedHeartDisease']

for col in colmns_to_Rplace:
    sampleData= sampleData.withColumn(col, when(sampleData[col]=='Yes',1).otherwise(0))



# Filter based on Diagnosis Codes For Given Measure

sampleData_rdd= sampleData.rdd;

sampleData_Filtered=sampleData_rdd.filter(lambda  x:x['diagnosis_code'] in str(Diagnosis_codes[measure]))

sampleData_Filtered = sampleData_Filtered.toDF()

# print(sampleData_Filtered.schema)
#Calculating Comorbidity value for a row

# Strangely this does not work check later
# sampleData_Filtered=sampleData_Filtered.withColumn('Comorbidity_Total', sum(sampleData_Filtered[col] for col in sampleData_Filtered.columns))


def column_add(a, b):
    return a.__add__(b)

sampleData_Filtered = sampleData_Filtered.withColumn('Comorbidity_Total',
                      reduce(column_add, (sampleData_Filtered[col] for col in colmns_to_Rplace)))


sampleData_Filtered_rdd= sampleData_Filtered.rdd;

#Lenght of stay as key value pair
# LengthOfStay={1:2,2:2,3:3,4:5,5:6,6:6,7:5,8:5,9:5,10:5,11:5,12:5,13:5}


def GetLengthOfStay(l):
    if l< 1:
        return 0;
    else :
        if l>=1 & l<=3:
            return l;
        else :
            if l>=4 & l<=6:
                return 4;
            else :
                if l>=7 & l<=13:
                    return  5;
                else :
                    if l >=14:
                        return 7;
                    else:
                        return  0;


#‘Inpatient_visits’ column to get Acute Admissions [or EmergencyAdmission] values as mentioned in the mail
def GetAcuteAdmissions(a):
    if a==1:
        return 3;
    else:
        return 0;

def GetComorbidityScore(c):
    if c>=0 & c<4:
        return c;
    else :
        if c>=4:
            return  5;
        else :
            return  0;


def GetEDVisits(e):
    if e>=0 & e<=3:
        return  e;
    else:
        if e>=4:
            return  4;
        else :
            return  0;



def CalculateLaceScore(x):
    Length_Of_Stay= GetLengthOfStay(int(x['LengthofStay']))
    Acute_Admissions=GetAcuteAdmissions(int(x['Inpatient_visits']))  #‘Inpatient_visits’ column to get Acute Admissions [or EmergencyAdmission] values as mentioned in the mail
    Comorbidity_Score=GetComorbidityScore(int(x['Comorbidity_Total']))
    ED_Visits=GetEDVisits(int(x['ED_visits']))
    return  (Length_Of_Stay+Acute_Admissions+Comorbidity_Score+ED_Visits);

lace_scores=sampleData_Filtered_rdd.map(lambda x: CalculateLaceScore(x)).collect()

denominator=len(lace_scores)

numerator = len([i for i in lace_scores if i >= 9])

score = float(numerator/denominator)

print('Score For '+measure+' measure',score)



