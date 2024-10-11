from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from airflow import DAG
import os
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.tree import DecisionTreeClassifier
from sqlalchemy import create_engine
from datetime import datetime


def load_and_filter_data():
    spark = SparkSession.builder.appName("FilterBreastCancer")\
        .getOrCreate()

    df = spark.read.option("header", "false").csv(
        '/opt/airflow/dags/DATASET.csv', 
        encoding='latin1', inferSchema=True
    )
    
    df = df.withColumnRenamed("_c0", "PREDICATION_ID")\
           .withColumnRenamed("_c1", "SENTENCE_ID")\
           .withColumnRenamed("_c2", "PMID")\
           .withColumnRenamed("_c3", "PREDICATE")\
           .withColumnRenamed("_c4", "SUBJECT_CUI")\
           .withColumnRenamed("_c5", "SUBJECT_NAME")\
           .withColumnRenamed("_c6", "SUBJECT_SEMTYPE")\
           .withColumnRenamed("_c7", "SUBJECT_NOVELTY")\
           .withColumnRenamed("_c8", "OBJECT_CUI")\
           .withColumnRenamed("_c9", "OBJECT_NAME")\
           .withColumnRenamed("_c10", "OBJECT_SEMTYPE")\
           .withColumnRenamed("_c11", "OBJECT_NOVELTY")\
           .withColumnRenamed("_c12", "unk1")\
           .withColumnRenamed("_c13", "unk2")\
           .withColumnRenamed("_c14", "unk3")
    
    C_synonyms = ['C0678222', 'C0006142', 'C0007124', 'C0346153',
              'C0281267', 'C0860581', 'C0497466', 'C0278601',
              'C4049259', 'C0281189', 'C0879255', 'C0741682',
              'C1134719', 'C3642345', 'C1096616', 'C0154084',
              'C3495917', 'C5243564', 'C0279563', 'C0235653',
              'C4733095', 'C0281663', 'C4520821', 'C3160889',
              'C3160887', 'C4733094', 'C2938924', 'C1527349',
              'C4733093', 'C4733092', 'C4721421', 'C0278486',
              'C0677776', 'C2827981', 'C1861906', 'C3495949',
              'C2980042', 'C0278487', 'C2827980', 'C0853879',
              'C4724023', 'C3642346', 'C4528560', 'C4520714',
              'C4520898', 'C3539878', 'C1328549', 'C5243562',
              'C4735965', 'C0279855', 'C2030735', 'C1960398',
              'C1328544', 'C0278488', 'C2986662', 'C2216702',
              'C2986664', 'C2986665', 'C1328547', 'C5243563',
              'C1328545', 'C0861387', 'C0861369', 'C1504470',
              'C0861351', 'C0861378', 'C1328548', 'C0278485',
              'C0861396', 'C1112794', 'C0278489', 'C1336156',
              'C1336178', 'C0278513', 'C2216697', 'C2216698',
              'C2216696', 'C2216699', 'C2216701', 'C2216694',
              'C4733091', 'C4733090', 'C2216703', 'C1854365',
              'C5442603', 'C3549742', 'C5442556', 'C0853971',
              'C0853972', 'C4274421', 'C0856130', 'C4016282',
              'C5394852', 'C0346152', 'C4324525', 'C4016427', 'C0741679', 'C0741674', 'C0741686', 'C3897071', 
              'C3146271', 'C2216700', 'C2216704', 'C2216708', 'C4733126', 'C4733127', 'C5170546', 'C5225787', 
              'C0741681', 'C0741676', 'C0741675', 'C4528588', 'C4528552', 'C4528553', 'C4528571', 'C4528572', 
              'C4528582', 'C4528558', 'C4528550', 'C4528590', 'C4528589', 'C4528557', 'C4528556', 'C4528559',
              'C4528573', 'C4528580', 'C4528583', 'C4528551', 'C4528554', 'C4528555', 'C4528561', 'C4528570',
              'C4528591', 'C4528592', 'C2216706', 'C2216707', 'C2216713', 'C2216714', 'C2216709', 'C2216715',
              'C2216705', 'C2216711', 'C1332442', 'C1332443', 'C0741680', 'C0741677', 'C2216718', 'C2216716',
              'C2216717', 'C2216719', 'C2216720', 'C4528549', 'C4528569', 'C5441655', 'C0860580', 'C1334807', 'C3812899', 'C0007104', 'C4722518', 'C1710547', 'C4018978', 'C1336076', 'C5447335', 'C4688316', 'C1332623', 'C1332626', 'C1332624', 'C4727105', 'C1332625', 'C4727106', 'C4524681', 'C4042788', 
              'C4042789', 'C3642471', 'C3898879', 'C3642347']

    Recurrent_BC_synonyms = ['C0278493', 'C0861360', 'C0935909']

    Male_BC_synonyms = ['C0238033', 'C0242787']

    all_synonyms = C_synonyms + Recurrent_BC_synonyms + Male_BC_synonyms
    df = df.filter(col("OBJECT_CUI").isin(all_synonyms)).distinct()
    df = df.withColumn("Cancer_Type", 
            F.when(col("OBJECT_CUI").isin(C_synonyms), "Female Breast Cancer")
            .when(col("OBJECT_CUI").isin(Male_BC_synonyms), "Male Breast Cancer")
            .when(col("OBJECT_CUI").isin(Recurrent_BC_synonyms), "Recurrent Breast Cancer")
            .otherwise("Other")
        )
    df = df.select("PREDICATE", "SUBJECT_NAME", "OBJECT_NAME","Cancer_Type")
    pandas_df = df.toPandas()
    pandas_df.to_csv("/opt/airflow/dags/filtered_dataset3.csv", index=False)


def clean_and_verify_data(input_file):
    df = pd.read_csv(input_file, encoding='latin1')
    
    empty_columns = df.columns[df.isnull().all()]
    df.drop(columns=empty_columns, inplace=True)

    df.dropna(inplace=True)

    expected_types = {
        'PREDICATE': 'object',
        'SUBJECT_NAME': 'object',
        'OBJECT_NAME': 'object'
    }
    
    for column, expected_type in expected_types.items():
        if not pd.api.types.is_dtype_equal(df[column].dtype, expected_type):
            print(f"Warning: Column '{column}' is not of type '{expected_type}'.")

    print("Basic statistics of the DataFrame:")
    print(df.describe(include='all'))

    df.to_csv("/opt/airflow/dags/cleaned_filtered_dataset3.csv", index=False)

def filter_data(input_file):

    df = pd.read_csv(input_file, encoding='latin1')
    
    
    causes_filter = ['CAUSES', 'PREDISPOSES', 'AFFECTS', 'DISRUPTS', 'COMPLICATES', 'AUGMENTS']
    causes_df = df[df['PREDICATE'].isin(causes_filter)].drop_duplicates()
    causes_df.to_csv("/opt/airflow/dags/causes_filtered.csv", index=False)

    treatments_filter = ['METHOD_OF', 'TREATS', 'ADMINISTERED_TO', 'USES']
    treatments_df = df[df['PREDICATE'].isin(treatments_filter)].drop_duplicates()
    treatments_df.to_csv("/opt/airflow/dags/treatments_filtered.csv", index=False)

    preventions_filter = ['PREVENTS', 'INHIBITS', 'NEG_PREVENTS']
    preventions_df = df[df['PREDICATE'].isin(preventions_filter) |
                                     (df['OBJECT_NAME'] == 'breast cancer prevention')].drop_duplicates()
    preventions_df.to_csv("/opt/airflow/dags/preventions_filtered.csv", index=False)

    diagnoses_filter = ['DIAGNOSES', 'MEASURES']
    diagnoses_df = df[df['PREDICATE'].isin(diagnoses_filter)].drop_duplicates()
    diagnoses_df.to_csv("/opt/airflow/dags/diagnoses_filtered.csv", index=False)
import pandas as pd

def classify_causes(input_file):
    df = pd.read_csv(input_file, encoding='latin1')
    
    categories = {
    'Hazardous or Poisonous Substance': ['hazardous', 'poisonous', 'toxic', 'carcinogen', 'mutagen', 'dangerous', 'toxicant', 'benzene', 'formaldehyde'],
    'Gene or Genome': ['gene', 'genome', 'nucleotide sequence', 'nucleic acid', 'nucleoside', 'genetic material', 'BRCA1', 'BRCA2', 'TP53'],
    'Amino Acid, Peptide, or Protein': ['amino acid', 'peptide', 'protein', 'polypeptide', 'HER2', 'estrogen receptor'],
    'Hormone': ['hormone', 'endocrine', 'estrogen', 'progesterone', 'testosterone', 'hormone therapy', 'aromatase inhibitors'],
    'Neoplastic Process': ['neoplastic', 'malignant', 'cancer', 'tumor', 'carcinoma', 'breast cancer', 'metastasis', 'sarcoma', 'leukemia'],
    'Pharmacologic Substance': ['pharmacologic', 'drug', 'medication', 'pharmaceutical', 'tamoxifen', 'trastuzumab', 'anastrozole', 'letrozole', 'palbociclib'],
    'Nucleotide Sequence': ['nucleotide sequence', 'DNA sequence', 'RNA sequence', 'BRCA1 gene', 'BRCA2 gene'],
    'Research Device': ['research device', 'laboratory equipment', 'biopsy tool', 'PCR machine', 'microscope'],
    'Chemical': ['chemical', 'organic chemical', 'inorganic chemical', 'compound', 'carcinogen', 'toxin', 'solvent', 'acid'],
    'Element, Ion, or Isotope': ['element', 'ion', 'isotope', 'metal', 'non-metal', 'carbon', 'oxygen', 'hydrogen'],
    'Nucleic Acid, Nucleoside, or Nucleotide': ['nucleic acid', 'nucleoside', 'nucleotide', 'DNA', 'RNA', 'adenine', 'thymine', 'cytosine', 'guanine'],
    'Indicator, Reagent, or Diagnostic Aid': ['indicator', 'reagent', 'diagnostic aid', 'immunohistochemistry', 'biomarker', 'antibody', 'dye'],
    'Biologically Active Substance': ['biologically active substance', 'biologic agent', 'cytokine', 'growth factor', 'hormone'],
    'Virus': ['virus', 'pathogen', 'HPV', 'human papillomavirus', 'retrovirus'],
    'Food': ['food', 'nutrient', 'dietary substance', 'phytochemicals', 'vitamin', 'mineral'],
    'Chemical Viewed Functionally': ['chemical viewed functionally', 'functional chemical', 'reactant', 'agent'],
    'Immunologic Factor': ['immunologic factor', 'immune response', 'cytokine', 'immunotherapy', 'antigen', 'antibody'],
    'Antibiotic': ['antibiotic', 'antimicrobial', 'penicillin', 'doxycycline'],
    'Substance': ['substance', 'material', 'element', 'compound'],
    'Lipid': ['lipid', 'fatty acid', 'cholesterol', 'triglyceride', 'phospholipid'],
    'Body Substance': ['body substance', 'biological fluid', 'serum', 'plasma', 'saliva'],
    'Environmental Effect of Humans': ['environmental effect', 'ecological impact', 'radiation', 'pollution', 'climate change'],
    'Steroid': ['steroid', 'steroidal hormone', 'estrogen', 'progesterone', 'testosterone', 'anabolic steroid'],
    'Inorganic Chemical': ['inorganic chemical', 'mineral', 'cadmium', 'lead', 'mercury'],
    'Biomedical or Dental Material': ['biomedical material', 'dental material', 'medical device', 'prosthesis', 'implant'],
    'Eicosanoid': ['eicosanoid', 'prostaglandin', 'leukotriene'],
    'Bacterium': ['bacterium', 'bacteria', 'E. coli', 'Staphylococcus'],
    'Chemical Viewed Structurally': ['chemical viewed structurally', 'molecular structure', 'chemical bond', 'functional group'],
    'Enzyme': ['enzyme', 'catalyst', 'topoisomerase', 'aromatase', 'protease'],
    'Phenomenon or Process': ['phenomenon', 'process', 'event', 'tumor progression', 'cell proliferation', 'metastasis'],
    'Invertebrate': ['invertebrate', 'insect', 'worm', 'arachnid'],
    'Carbohydrate': ['carbohydrate', 'sugar', 'starch', 'glucose', 'fructose'],
    'Disease or Syndrome': ['disease', 'syndrome', 'disorder', 'breast cancer', 'metastatic breast cancer', 'tumor syndrome'],
    'Genetic Function': ['genetic function', 'gene expression', 'mutation', 'genetic variant'],
    'Fungus': ['fungus', 'mushroom', 'yeast'],
    'Neuroreactive Substance or Biogenic Amine': ['neuroreactive substance', 'biogenic amine', 'neurotransmitter', 'serotonin', 'dopamine']
}

    def classify_subject(subject_name):
        subject_name_lower = subject_name.lower()
        for category, keywords in categories.items():
            if any(keyword.lower() in subject_name_lower for keyword in keywords):
                return category
        return 'Other'

    df['Category'] = df['SUBJECT_NAME'].apply(classify_subject)

    df.to_csv(input_file, index=False)
def classify_treats(input_file):
    df = pd.read_csv(input_file, encoding='latin1')
    
    categories = {
    'Chemotherapy': [
        'chemotherapy', 'chemo', 'adjuvant chemotherapy', 'cytotoxic agent', 
        'anthracyclines', 'chemotherapeutic agent', 'mitoxantrone', 'cyclophosphamide', 
        'ifosfamide', 'epirubicin', 'taxol', 'fluorouracil', 'etoposide', 'adriamycin', 
        'cisplatin', 'methotrexate', 'vincristine', 'carboplatin', 'doxorubicin', 
        'chlorambucil', 'bendamustine', 'thalidomide'
    ],
    'Radiotherapy': [
        'radiotherapy', 'radio', 'radiation therapy', 'brachytherapy', 'x-ray therapy', 
        'intensity-modulated radiotherapy', 'adjuvant radiotherapy', 'radiation therapy, lymphatic', 
        'external radiotherapy'
    ],
    'Surgery': [
        'surgery', 'surgical', 'mastectomy', 'lumpectomy', 'breast-conserving surgery', 
        'quadrantectomy', 'modified radical mastectomy', 'excision', 'radical mastectomy', 
        'dissection', 'reconstructive surgical procedures', 'operative surgical procedures', 
        'implantation procedure', 'transplantation', 'maxillary left canine abutment', 
        'mandibular right third molar abutment', 'skin transplantation', 'lymph node excision', 'biopsy'
    ],
    'Hormone Therapy': [
        'hormone therapy', 'tamoxifen', 'aromatase inhibitor', 'hormone replacement therapy', 
        'estrogens', 'estrogen replacement therapy', 'progestins', 'estradiol', 'estrone', 
        'megestrol acetate', 'medroxyprogesterone', 'raloxifene', 'buserelin', 
        'androgen antagonists', 'gonadotropin-releasing hormone analog', 'diethylstilbestrol'
    ],
    'Targeted Therapy': [
        'targeted therapy', 'herceptin', 'trastuzumab', 'lapatinib', 'bevacizumab', 
        'antiangiogenesis therapy'
    ],
    'Immunotherapy': [
        'immunotherapy', 'immunologic adjuvants', 'vaccines', 'anti-inflammatory agents', 
        'biological response modifiers', 'interleukin', 'monoclonal antibodies', 'checkpoint inhibitors'
    ],
    'Supportive Care': [
        'supportive care', 'palliative care', 'symptomatic treatment', 'group therapy', 
        'psychotherapy', 'medical castration', 'cold therapy', 'symptomatic treatment'
    ],
    'Alternative Therapy': [
        'alternative therapy', 'holistic', 'complementary', 'herbal medicine', 'acupuncture', 
        'homeopathy'
    ],
    'Pharmacotherapy': [
        'pharmacotherapy', 'pharmaceutical preparations', 'drug delivery systems', 'medications', 
        'oral anticoagulants', 'antineoplastic agents', 'corticosteroids'
    ],
    'Adjuvant Therapy': [
        'adjuvant therapy', 'high-dose chemotherapy', 'neoadjuvant therapy'
    ],
    'Gene Therapy': [
        'gene therapy', 'genetic therapy'
    ],
    'Biological Therapy': [
        'biological therapy', 'biological agents'
    ],
    'Endocrine Therapy': [
        'endocrine therapy', 'hormone replacement therapy'
    ],
    'Surgical Procedures': [
        'operative surgical procedures', 'surgical procedures'
    ],
    'Procedures on Breast': [
        'procedures on breast', 'breast-conserving surgery'
    ]
}

    def classify_subject(subject_name):
        subject_name_lower = subject_name.lower()
        for category, keywords in categories.items():
            if any(keyword.lower() in subject_name_lower for keyword in keywords):
                return category
        return 'Other'

    df['Category'] = df['SUBJECT_NAME'].apply(classify_subject)

    df.to_csv(input_file, index=False)

def classify_prevents(input_file):
    df = pd.read_csv(input_file, encoding='latin1')
    
    categories = {
    'Lifestyle Factors': ['diet', 'exercise', 'lifestyle', 'activity', 'smoking', 'alcohol'],
    'Medications': ['drug', 'pharmaceutical', 'medication', 'aspirin', 'statin', 'metformin', 'chemo', 'antibiotic'],
    'Medical Procedures': ['surgery', 'treatment', 'procedure', 'therapy', 'transplant', 'biopsy', 'mastectomy', 'chemotherapy', 'radiotherapy'],
    'Behavioral Interventions': ['counseling', 'therapy', 'behavioral', 'psychotherapy'],
    'Supplements': ['vitamin', 'mineral', 'supplement', 'herb', 'nutraceutical', 'probiotic', 'omega-3', 'calcium', 'iron'],
    'Vaccines': ['vaccine', 'vaccination', 'immunization'],
    'Environmental Factors': ['exposure', 'pollution', 'radiation', 'chemical', 'asbestos', 'pesticide'],
    'Genetic Factors': ['brca1', 'brca2', 'genetic', 'mutation', 'hereditary'],
    'Hormonal Treatments': ['hormone', 'estrogen', 'progesterone', 'androgen', 'tamoxifen'],
    'Alternative Therapies': ['acupuncture', 'homeopathy', 'chiropractic', 'naturopathy', 'yoga', 'meditation'],
    'Dietary Interventions': ['diet', 'nutrition', 'caloric', 'ketogenic', 'plant-based', 'fiber'],
    'Physical Activity': ['exercise', 'workout', 'fitness', 'aerobic', 'anaerobic'],
    'Screening and Monitoring': ['screening', 'monitoring', 'mammogram', 'biopsy', 'self-exam'],
    'Public Health Measures': ['quarantine', 'isolation', 'social distancing', 'sanitization', 'lockdown'],
    'Personal Protective Equipment': ['mask', 'glove', 'face shield', 'gown']
}

    def classify_subject(subject_name):
        subject_name_lower = subject_name.lower()
        for category, keywords in categories.items():
            if any(keyword.lower() in subject_name_lower for keyword in keywords):
                return category
        return 'Other'

    df['Category'] = df['SUBJECT_NAME'].apply(classify_subject)

    df.to_csv(input_file, index=False)

def classify_diagnoses(input_file):
    df = pd.read_csv(input_file, encoding='latin1')
    
    categories = {
    "Early Diagnosis": ["early detection", "initial diagnosis", "screening", "preventive screening", "routine exams"],
    "Imaging": ["MRI", "CT scan", "X-ray", "ultrasound", "mammography", "breast scintigraphy", "digital imaging"],
    "Biopsy": ["biopsy", "tissue sample", "needle biopsy", "core biopsy", "fine needle aspiration"],
    "Blood Tests": ["blood test", "serum analysis", "blood work", "hematological tests", "blood profile"],
    "Genetic Tests": ["genetic test", "BRCA1", "BRCA2", "genetic screening", "genetic profiling", "susceptibility testing"],
    "Clinical Exam": ["clinical exam", "physical examination", "clinical assessment", "detailed physical exam", "palpation and inspection"],
    "Pathology Reports": ["pathology report", "histology report", "biopsy report", "tissue analysis", "histopathological report"],
    "Functional Tests": ["functional test", "performance test", "organ function assessment", "functional evaluation", "body function tests"],
}


    def classify_subject(subject_name):
        subject_name_lower = subject_name.lower()
        for category, keywords in categories.items():
            if any(keyword.lower() in subject_name_lower for keyword in keywords):
                return category
        return 'Other'

    df['Category'] = df['SUBJECT_NAME'].apply(classify_subject)

    df.to_csv(input_file, index=False)




def classify_others():
    files = {
        "causes": "/opt/airflow/dags/causes_filtered.csv",
        "treatments": "/opt/airflow/dags/treatments_filtered.csv",
        "preventions": "/opt/airflow/dags/preventions_filtered.csv",
        "diagnoses": "/opt/airflow/dags/diagnoses_filtered.csv"
    }

    models = {
        'Decision Tree': DecisionTreeClassifier(random_state=42),
        'Random Forest': RandomForestClassifier(random_state=42),
        'Naive Bayes': MultinomialNB(),
        'Logistic Regression': LogisticRegression(max_iter=1000)
    }

    for category, file in files.items():
        df = pd.read_csv(file, encoding='latin1')

        if 'Category' not in df.columns:
            raise ValueError(f"The 'Category' column is missing in the dataset: {file}")

        known_data = df[df['Category'] != 'Other']
        unknown_data = df[df['Category'] == 'Other']

        if unknown_data.empty:
            print(f"No 'Other' categories to classify in {file}. Skipping...")
            continue

        X_known = known_data.drop(columns=['Category'])
        y_known = known_data['Category']

        X_known = pd.get_dummies(X_known, drop_first=True)

        X_train, X_test, y_train, y_test = train_test_split(X_known, y_known, test_size=0.2, random_state=42)

        best_model = None
        best_score = 0
        best_report = ""

        for model_name, model in models.items():
            model.fit(X_train, y_train)

            y_pred = model.predict(X_test)

            report = classification_report(y_test, y_pred, output_dict=True, zero_division=0)

            accuracy = accuracy_score(y_test, y_pred)

            print(f"Classification Report for {category} with {model_name}:\n", report)

            if accuracy > best_score:
                best_score = accuracy
                best_model = model_name
                best_report = report

        if best_model is None:
            print(f"No suitable model found for {category}. Skipping...")
            continue

        X_unknown = unknown_data.drop(columns=['Category'])
        X_unknown = pd.get_dummies(X_unknown, drop_first=True)

        X_unknown = X_unknown.reindex(columns=X_known.columns, fill_value=0)

        models[best_model].fit(X_known, y_known)  

        df.loc[df['Category'] == 'Other', 'Category'] = models[best_model].predict(X_unknown)

        df.to_csv(file, index=False)


def save():

    db_connection_string = os.environ.get('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN')
    
    engine = create_engine(db_connection_string)
    csv_files = {
        "/opt/airflow/dags/causes_filtered.csv": "causes_table",
        "/opt/airflow/dags/treatments_filtered.csv": "treatments_table",
        "/opt/airflow/dags/preventions_filtered.csv": "preventions_table",
        "/opt/airflow/dags/diagnoses_filtered.csv": "diagnoses_table"
    }
    
  
    for csv_file, table_name in csv_files.items():
        df = pd.read_csv(csv_file, encoding='latin1')
        
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Table '{table_name}' successfully saved to PostgreSQL.")


with DAG(
    'breast_cancer_data_pipeline',
    default_args={'owner': 'airflow', 'start_date': datetime(2023, 10, 1)},
    schedule_interval='@daily',
    catchup=False
) as dag:
    load_data_task = PythonOperator(
        task_id='load_and_filter_data',
        python_callable=load_and_filter_data
    )

    clean_data_task = PythonOperator(
        task_id='clean_and_verify_data',
        python_callable=clean_and_verify_data,
        op_kwargs={'input_file': '/opt/airflow/dags/filtered_dataset3.csv'}
    )
    filter_data_task = PythonOperator(
        task_id='filter_data',
        python_callable=filter_data,
        op_kwargs={'input_file': '/opt/airflow/dags/cleaned_filtered_dataset3.csv'}
    )
    classify_causes_task = PythonOperator(
        task_id='classify_causes',
        python_callable=classify_causes,
        op_kwargs={'input_file': '/opt/airflow/dags/causes_filtered.csv'}
    )
    classify_treats_task = PythonOperator(
        task_id='classify_treats',
        python_callable=classify_treats,
        op_kwargs={'input_file': '/opt/airflow/dags/treatments_filtered.csv'}
    )
    classify_prevents_task = PythonOperator(
        task_id='classify_prevents',
        python_callable=classify_prevents,
        op_kwargs={'input_file': '/opt/airflow/dags/preventions_filtered.csv'}
    )
    classify_diagnoses_task = PythonOperator(
        task_id='classify_diagnoses',
        python_callable=classify_diagnoses,
        op_kwargs={'input_file': '/opt/airflow/dags/diagnoses_filtered.csv'}
    )
    classify_others_task  = PythonOperator(
        task_id='classify_others',
        python_callable=classify_others,
    )
    save_task  = PythonOperator(
        task_id='save',
        python_callable=save,
    )
    


    # Set up dependencies for the first ETL process
    load_data_task >> clean_data_task >> filter_data_task
    filter_data_task >> [
        classify_causes_task,
        classify_treats_task,
        classify_prevents_task,
        classify_diagnoses_task,
    ]  >> classify_others_task >> save_task 
