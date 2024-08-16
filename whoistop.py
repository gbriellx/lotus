import requests
import re
import pandas as pd
import time
import csv
import logging
from urllib.parse import urlparse
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ATRASO_BASE = 60
ATRASO_PERSONALIZADO = 5
CONTADOR_SUCESSO = 0

def consulta_rdap(dominio):
    url = f"https://rdap.registro.br/domain/{dominio}"
    try:
        resposta = requests.get(url)
        resposta.raise_for_status()
        return resposta.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro na consulta RDAP para {dominio}: {str(e)}")
        return {"error": str(e)}

def consulta_cnpj(cnpj):
    url = f"https://receitaws.com.br/v1/cnpj/{cnpj}"
    try:
        resposta = requests.get(url)
        resposta.raise_for_status()
        return resposta.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro na consulta CNPJ {cnpj}: {str(e)}")
        return {"error": str(e)}

def extract_key_info(rdap_info):
    email_rdap = None
    cpf_cnpj = None
    name = None

    if 'error' in rdap_info:
        return {'email_rdap': 'Error', 'cpf_cnpj': 'Error', 'name': 'Error'}

    for entity in rdap_info.get('entities', []):
        if 'registrant' in entity.get('roles', []):
            vcard = entity.get('vcardArray', [[]])[1]
            name = vcard[1][3] if len(vcard) > 1 and len(vcard[1]) > 3 else 'Not available'
            for public_id in entity.get('publicIds', []):
                if public_id['type'] in ['cpf', 'cnpj']:
                    cpf_cnpj = public_id['identifier']
        
        for sub_entity in entity.get('entities', []):
            vcard = sub_entity.get('vcardArray', [[]])[1]
            for item in vcard:
                if item[0] == 'email':
                    email_rdap = item[3]
                    break

    return {
        'email_rdap': email_rdap if email_rdap else 'Not available',
        'cpf_cnpj': cpf_cnpj if cpf_cnpj else 'Not available',
        'name': name if name else 'Not available'
    }

def format_cnpj_info(info):
    qsa = info.get('qsa', [])
    socios = []
    for i, socio in enumerate(qsa[:2]):  # Pegamos até 2 sócios
        socios.append({
            f'socio_{i+1}_nome': socio.get('nome', 'Not available'),
            f'socio_{i+1}_cargo': socio.get('qual', 'Not available')
        })
    
    return {
        'nome': info.get('nome', 'Not available'),
        'logradouro': info.get('logradouro', 'Not available'),
        'numero': info.get('numero', 'Not available'),
        'bairro': info.get('bairro', 'Not available'),
        'municipio': info.get('municipio', 'Not available'),
        'uf': info.get('uf', 'Not available'),
        'telefone': info.get('telefone', 'Not available'),
        'email': info.get('email', 'Not available'),
        **{k: v for d in socios for k, v in d.items()}  # Adiciona informações dos sócios
    }

def sanitize_cnpj(cnpj):
    return re.sub(r'\D', '', cnpj)

def save_to_csv(dominio, rdap_info, cnpj_info, filename):
    global CONTADOR_SUCESSO
    try:
        df = pd.read_csv(filename)
    except FileNotFoundError:
        df = pd.DataFrame(columns=[
            'Domain', 'CNPJ', 'Company Name', 'Address', 'Phone',
            'RDAP Email', 'ReceitaWS Email', 'Socio 1 Nome', 'Socio 1 Cargo',
            'Socio 2 Nome', 'Socio 2 Cargo'
        ])

    new_row = {
        'Domain': dominio,
        'CNPJ': rdap_info.get('cpf_cnpj', 'Not available'),
        'Company Name': cnpj_info.get('nome', 'Not available'),
        'Address': f"{cnpj_info.get('logradouro', 'Not available')}, {cnpj_info.get('numero', 'Not available')}, {cnpj_info.get('bairro', 'Not available')}, {cnpj_info.get('municipio', 'Not available')}, {cnpj_info.get('uf', 'Not available')}",
        'Phone': cnpj_info.get('telefone', 'Not available'),
        'RDAP Email': rdap_info.get('email_rdap', 'Not available'),
        'ReceitaWS Email': cnpj_info.get('email', 'Not available'),
        'Socio 1 Nome': cnpj_info.get('socio_1_nome', 'Not available'),
        'Socio 1 Cargo': cnpj_info.get('socio_1_cargo', 'Not available'),
        'Socio 2 Nome': cnpj_info.get('socio_2_nome', 'Not available'),
        'Socio 2 Cargo': cnpj_info.get('socio_2_cargo', 'Not available')
    }

    df = df[~df['Domain'].isin([dominio])]  # Remove existing entry if any
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    df.to_csv(filename, index=False, encoding='utf-8')
    logging.info(f"Informações do domínio {dominio} salvas com sucesso.")
    CONTADOR_SUCESSO += 1
    return True

def clean_domains(domains):
    cleaned_domains = []
    for domain in domains:
        domain = domain.strip().lower()
        domain = re.sub(r'^https?://', '', domain)
        domain = domain.rstrip('/')
        domain = re.sub(r'^www\.', '', domain)
        parsed = urlparse('http://' + domain)
        domain_parts = parsed.netloc.split('.')
        if len(domain_parts) > 2:
            domain = '.'.join(domain_parts[-3:])
        else:
            domain = '.'.join(domain_parts)
        if domain.endswith('.br'):
            cleaned_domains.append(domain)
    return cleaned_domains

def main():
    global CONTADOR_SUCESSO
    input_filename = 'dominios.csv'
    output_filename = 'informacoes_empresa.csv'
    
    try:
        with open(input_filename, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)
            domains = [row[0] for row in reader if row]
    except Exception as e:
        logging.error(f"Erro ao ler o arquivo CSV: {e}")
        return
    
    cleaned_domains = clean_domains(domains)
    
    try:
        df_existente = pd.read_csv(output_filename)
        dominios_existentes = set(df_existente['Domain'])
    except FileNotFoundError:
        dominios_existentes = set()
    
    for dominio in cleaned_domains:
        if dominio in dominios_existentes:
            logging.info(f"O domínio {dominio} já está na planilha. Não será pesquisado novamente.")
            continue

        inicio_processamento = time.time()
        logging.info(f"Processando domínio: {dominio}")
        
        rdap_resultado = consulta_rdap(dominio)
        time.sleep(10)
        
        if isinstance(rdap_resultado, dict) and 'error' not in rdap_resultado:
            key_info = extract_key_info(rdap_resultado)
            logging.info(f"Domínio: {dominio} - Dados RDAP recuperados com sucesso")
            
            if key_info['cpf_cnpj']:
                cnpj_sanitizado = sanitize_cnpj(key_info['cpf_cnpj'])
                if len(cnpj_sanitizado) == 14:
                    receita_info = consulta_cnpj(cnpj_sanitizado)
                    time.sleep(10)
                    if isinstance(receita_info, dict) and 'error' not in receita_info:
                        formatted_info = format_cnpj_info(receita_info)
                        save_to_csv(dominio, key_info, formatted_info, output_filename)
                        logging.info(f"Domínio: {dominio} - Informações salvas com sucesso")
                    else:
                        logging.error(f"Domínio: {dominio} - Erro na consulta ReceitaWS: {receita_info.get('error', 'Erro desconhecido')}")
                else:
                    logging.warning(f"Domínio: {dominio} - O CPF/CNPJ não é um CNPJ válido.")
            else:
                logging.warning(f"Domínio: {dominio} - CNPJ não encontrado na consulta RDAP.")
        else:
            logging.error(f"Domínio: {dominio} - Erro na consulta RDAP: {rdap_resultado.get('error', 'Erro desconhecido')}")
        
        tempo_processamento = time.time() - inicio_processamento
        tempo_espera = max(0, ATRASO_BASE - tempo_processamento)
        time.sleep(tempo_espera)
        
        if CONTADOR_SUCESSO > 0 and CONTADOR_SUCESSO % 5 == 0:
            logging.info(f"Aplicando atraso personalizado de {ATRASO_PERSONALIZADO} minutos após {CONTADOR_SUCESSO} domínios processados com sucesso.")
            time.sleep(ATRASO_PERSONALIZADO * 60)
            
if __name__ == "__main__":
    main()
