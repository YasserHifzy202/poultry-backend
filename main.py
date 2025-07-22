from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from io import BytesIO
import math

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

REQUIRED_OPERATIONAL_COLS = [
    'Flock', 'Date', 'Animal Mortality', 'Animals Culled', 'Table Eggs Prod',
    'Animal Feed Formula Name', 'Supplied Feed', 'Feed Received (Kg)',
    'Animal Feed Consumed', 'Water Consumption',
    'Animal Weight', 'Animal Uniformity', 'Animal CV Uniformity',
    'Female Feed Formula ID', 'Temperature Low', 'Ammonia Level',
    'Animal Feed Inventory', 'Female Feed Type ID',
    'Light_Duration (HU)', 'Light intensity %'
]

OPTIONAL_OPERATIONAL_COLS = [
    'Animal Weight', 'Animal Uniformity', 'Animal CV Uniformity'
]

REQUIRED_CARE_COLS = [
    'Vaccination', 'Creation User ID', 'Medication', 'Vacc Method',
    'Vacc Type', 'VaccinevDoze', 'Medication Batch', 'Concentration %',
    'Record Source Type', 'Medication Dose', 'Medication Exp Date',
    'Doctor Name', 'Doses Unit', 'Produced PS_Nest_HE', 'Vaccine Name'
]

def clean_nan_inf(record: dict) -> dict:
    for k, v in record.items():
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            record[k] = None
    return record

def is_operational_row(row):
    for col in REQUIRED_OPERATIONAL_COLS:
        if col in OPTIONAL_OPERATIONAL_COLS + ['Flock', 'Date']:
            continue
        val = row.get(col)
        if pd.notna(val):
            if isinstance(val, (int, float)):
                if float(val) != 0:
                    return True
            elif str(val).strip() not in ['', '0', 'nan', 'NaN']:
                return True
    return False

def check_operational_row(row):
    errors = []
    for col in REQUIRED_OPERATIONAL_COLS:
        if col in OPTIONAL_OPERATIONAL_COLS + ['Flock', 'Date']:
            continue
        val = row.get(col)
        if pd.isna(val) or str(val).strip() == '':
            errors.append(f'Missing {col}')
        elif isinstance(val, (int, float)) and val < 0:
            errors.append(f'Negative value in {col}')
    # تحقق من Flock وDate
    for col in ['Flock', 'Date']:
        val = row.get(col)
        if pd.isna(val) or str(val).strip() == '':
            errors.append(f'Missing {col}')
    return '; '.join(errors) if errors else None

def check_care_row(row):
    errors = []
    note = ''
    vacc = str(row.get('Vaccination') or '').strip()
    med = str(row.get('Medication') or '').strip()

    if vacc == '' and med == '':
        errors.append('Missing Vaccination and Medication')
    elif vacc != '' and med == '':
        note = 'Note: Only vaccination recorded, no medication data entered.'
    elif med != '' and vacc == '':
        note = 'Note: Only medication recorded, no vaccination data entered.'

    # فحص بقية الأعمدة كالمعتاد
    for col in REQUIRED_CARE_COLS:
        if col in ['Vaccination', 'Medication']:
            continue  # تم فحصهم أعلاه
        val = row.get(col)
        if pd.isna(val) or str(val).strip() == '':
            errors.append(f'Missing {col}')
    return '; '.join(errors) if errors else None, note

@app.post("/analyze")
async def analyze(file: UploadFile = File(...)):
    if not file.filename.endswith(('.xls', '.xlsx')):
        raise HTTPException(400, "Only Excel files allowed.")

    contents = await file.read()
    try:
        df = pd.read_excel(BytesIO(contents))
    except Exception as e:
        raise HTTPException(400, f"Failed to read Excel file: {e}")

    df.columns = df.columns.str.strip()

    for col in set(REQUIRED_OPERATIONAL_COLS + REQUIRED_CARE_COLS) - set(df.columns):
        df[col] = pd.NA

    # حوّل الأعمدة الرقمية إلى أرقام حقيقية
    for col in REQUIRED_OPERATIONAL_COLS:
        if col not in OPTIONAL_OPERATIONAL_COLS and col not in ['Flock', 'Date', 'Animal Feed Formula Name', 'Supplied Feed', 'Female Feed Formula ID', 'Female Feed Type ID']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # فصل البيانات
    operational_mask = df.apply(is_operational_row, axis=1)
    operational_df = df[operational_mask].copy()
    care_df = df[~operational_mask].copy()

    # فحص التكرار في البيانات التشغيلية (نفس القطيع + التاريخ)
    if not operational_df.empty:
        operational_df['Duplicate Error'] = operational_df.duplicated(subset=['Flock', 'Date'], keep=False)
    else:
        operational_df['Duplicate Error'] = False

    # فحص التكرار الذكي في بيانات الرعاية
    care_df = care_df.copy()
    care_df['care_key'] = care_df.apply(
        lambda r: (
            str(r.get('Flock')).strip().upper(),
            str(r.get('Date')).strip(),
            str(r.get('Vaccination')).strip().upper() if pd.notna(r.get('Vaccination')) else '',
            str(r.get('Vacc Method')).strip().upper() if pd.notna(r.get('Vacc Method')) else '',
            str(r.get('Vacc Type')).strip().upper() if pd.notna(r.get('Vacc Type')) else '',
            str(r.get('VaccinevDoze')).strip().upper() if pd.notna(r.get('VaccinevDoze')) else '',
            str(r.get('Medication')).strip().upper() if pd.notna(r.get('Medication')) else '',
            str(r.get('Medication Dose')).strip().upper() if pd.notna(r.get('Medication Dose')) else '',
            str(r.get('Medication Batch')).strip().upper() if pd.notna(r.get('Medication Batch')) else '',
            str(r.get('Medication Exp Date')).strip() if pd.notna(r.get('Medication Exp Date')) else '',
        ),
        axis=1
    )
    if not care_df.empty:
        care_df['Duplicate Error'] = care_df.duplicated(subset=['care_key'], keep=False)
    else:
        care_df['Duplicate Error'] = False

    # الفحص المنطقي
    operational_df['Error Details'] = operational_df.apply(check_operational_row, axis=1)
    operational_df['has_error'] = operational_df['Error Details'].notnull() & (operational_df['Error Details'] != "")
    operational_df.loc[operational_df['Duplicate Error'] == True, 'has_error'] = True
    operational_df.loc[operational_df['Duplicate Error'] == True, 'Error Details'] = \
        operational_df['Error Details'].astype(str) + '; Duplicate Flock/Date'

    # رعاية - ملاحظة/خطأ
    care_errors_and_notes = care_df.apply(check_care_row, axis=1, result_type='expand')
    care_df['Error Details'] = care_errors_and_notes[0]
    care_df['note'] = care_errors_and_notes[1]
    care_df['has_error'] = care_df['Error Details'].notnull() & (care_df['Error Details'] != "")
    care_df.loc[care_df['Duplicate Error'] == True, 'has_error'] = True
    care_df.loc[care_df['Duplicate Error'] == True, 'Error Details'] = \
        care_df['Error Details'].astype(str) + '; Duplicate Flock/Date/Vaccination/Medication'

    operational_records = [clean_nan_inf(r) for r in operational_df.to_dict(orient='records')]
    care_records = [clean_nan_inf(r) for r in care_df.to_dict(orient='records')]

    return {
        "operational_data": operational_records,
        "care_data": care_records,
    }
