# file: export_gsheet_to_csv.py

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials


def export_catalog_to_csv(credentials_file: str, sheet_url: str, tab_name: str, output_file: str):
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly"
    ]
    credentials = Credentials.from_service_account_file(
        credentials_file,
        scopes=scopes,
        subject="fairycatalogaccess@platinum-lead-466619-j9.iam.gserviceaccount.com"
    )

    try:
        client = gspread.authorize(credentials)
        spreadsheet = client.open_by_url(sheet_url)
        worksheet = spreadsheet.worksheet(tab_name)
        data = worksheet.get_all_records()

        df = pd.DataFrame(data)

        # Coerce all columns to string early and clean
        df = df.applymap(lambda x: str(x).replace('\n', ' ').replace('\r', ' ').strip() if pd.notna(x) else '')

        # Filter out rows with empty 'Wine-Searcher Name'
        df = df[df['Wine-Searcher Name'].astype(bool)]

        # Map and transform columns
        output_df = pd.DataFrame()
        output_df['SKU'] = df['ID']
        output_df['name'] = df['Wine-Searcher Name']
        output_df['description'] = df['Comments']
        output_df['vintage'] = df['Vintage'].replace('', 'NV')
        output_df['vintage'] = output_df['vintage'].fillna('NV')
        output_df['unit-size'] = df['Size'].apply(lambda x: '1.5' if x == 'Magnum 1.5l' else x)
        output_df['price'] = df['Price Bottle']
        output_df['url'] = df['Wine-Searcher Link'] if 'Wine-Searcher Link' in df.columns else ''
        output_df['min-order'] = 1
        output_df['tax'] = 'Inc'
        output_df['offer-type'] = 'R'
        output_df['delivery-time'] = '1-2 Weeks'
        output_df['stock-level'] = df['Inventory'].apply(lambda x: 32 if str(x) != '0' else 0)
        output_df['LWIN'] = df['LWIN']

        # Final strip on all output string columns
        for col in output_df.select_dtypes(include='object').columns:
            output_df[col] = output_df[col].str.strip()

        output_df.to_csv(output_file, sep='|', index=False)
        print(f"Exported '{tab_name}' to {output_file}")

    except APIError as e:
        if e.response.status_code == 403:
            print("Permission denied: Please make sure the Google Sheet is shared with the service account.")
        else:
            raise            

            
if __name__ == "__main__":
    export_catalog_to_csv(
        credentials_file="FairyCatalogAccess.json",
        sheet_url="https://docs.google.com/spreadsheets/d/1IyKeWZPhlEN5MKVEv8Yn_8gxsdgiEReuToaUZMcu5tg/edit?usp=sharing",
        tab_name="Catalog",
        output_file="catalog.csv"
    )
