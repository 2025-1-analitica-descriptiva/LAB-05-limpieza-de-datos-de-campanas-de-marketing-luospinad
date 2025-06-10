"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import glob
import pandas as pd
import os

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    file_path = 'files/input'
    dataframes = load_input(file_path)
    combinados_df = pd.concat(dataframes.values(), ignore_index=True)

    client_cols = ['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']
    campaign_cols = ['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome']
    economics_cols = ['client_id', 'cons_price_idx', 'euribor_three_months']

    client_df = combinados_df[client_cols].copy()
    campaign_df = combinados_df[campaign_cols].copy()
    economics_df = combinados_df[economics_cols].copy()


    # Formateo campos de client_df
    client_df.loc[:, 'job'] = client_df['job'].str.replace('.', '').str.replace('-','_')
    client_df.loc[:, 'education'] = client_df['education'].str.replace('.', '_')
    client_df.loc[:, 'education'] = client_df['education'].replace('unknown', pd.NA)
    # convertir yes a 1, lo otro a 0
    client_df.loc[:, 'credit_default'] = client_df['credit_default'].apply(lambda x: '1' if x == 'yes' else '0')
    client_df.loc[:, 'mortgage'] = client_df['mortgage'].apply(lambda x: '1' if x == 'yes' else '0')


    # Formateo campos campaign
    campaign_df.loc[:, 'previous_outcome'] = campaign_df['previous_outcome'].apply(lambda x: '1' if x == 'success' else '0')
    campaign_df.loc[:, 'campaign_outcome'] = campaign_df['campaign_outcome'].apply(lambda x: '1' if x == 'yes' else '0')
        
    combinados_df['day'] = combinados_df['day'].astype(str).str.zfill(2)
    combinados_df['fecha_str'] = combinados_df['day'] + ' ' + combinados_df['month'].str.lower() + ' 2022'
    campaign_df['last_contact_date'] = pd.to_datetime(combinados_df['fecha_str'], dayfirst=True).dt.strftime('%Y-%m-%d')

    campaign_df['last_contact_date'] = pd.to_datetime(
        combinados_df['fecha_str'],
        format='%d %b %Y'
    ).dt.strftime('%Y-%m-%d')

    
    output_path = 'files/output'
    os.makedirs(output_path, exist_ok=True)
    
    # Guardar en csv
    client_df.to_csv(f'{output_path}/client.csv', index=False)
    campaign_df.to_csv(f'{output_path}/campaign.csv', index=False)
    economics_df.to_csv(f'{output_path}/economics.csv', index=False)

    return

def load_input(input_directory):
    """Load text files in 'input_directory/'"""

    files = glob.glob(f"{input_directory}/*")
    dataframes = {
        os.path.basename(file): pd.read_csv(file, index_col=None)
        for file in files
    }

    return dataframes


if __name__ == "__main__":
    clean_campaign_data()
