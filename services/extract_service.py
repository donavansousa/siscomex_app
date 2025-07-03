from bs4 import BeautifulSoup
import os

directory_path = "read_data"

def get_files():
    try:
        filenames = os.listdir(directory_path)
        return filenames
    except FileNotFoundError:
        print(f"Error: Directory not found at {directory_path}")

def open_files(filenames):
    files = []
    for file in filenames:
        if os.path.join(directory_path, file):
            try:
                with open(os.path.join(directory_path, file), "r", encoding="utf-8") as f:
                    files.append(BeautifulSoup(f, "html.parser"))
            except IOError as e:
                print(f"Error opening or reading {file}: {e}")
    return files

def get_content_files():
    filenames = get_files()
    files = open_files(filenames)
    return files

def extract_text(label, content_files):
    for file in content_files:
        tag = file.find("dt", string=lambda text: text and label in text)
        if tag:
            return tag.find_next("dd").get_text(strip=True)
    return None

def extract_link(label, content_files):
    for file in content_files:
        tag = tag = file.find("dt", string=lambda text: text and label in text)
        if tag:
            value = tag.find_next("a").get_text(strip=True)
            if(value):
                return value
    return None

def load_json_file(num_bl):

    dados_carga = {
        "numero_ce": extract_link("No. CE-MERCANTE Master vinculado :"),
        "tipo_conhecimento": extract_text("Tipo de Conhecimento :"),
        "data_emissao_bl": extract_text("Data de Emissão do BL"),
        "razao_consignatario": extract_text("Razão Social/Nome"),
        "cnpj_consignatario": extract_text("CNPJ/CPF"),
        "porto_origem": extract_text("Porto de Origem"),
        "pais_procedencia": extract_text("País de Procedência"),
        "uf_destino": extract_text("UF de Destino Final"),
        "porto_destino": extract_text("Porto de Destino Final"),
        "descricao_mercadoria": extract_text("Descrição 1"),
        "peso_bruto_kg": extract_text("Peso Bruto da Carga"),
        "cubagem_m3": extract_text("Cubagem"),
        "valor_frete": extract_text("Valor do Frete Básico"),
        "modalidade_frete": extract_text("Modalidade de Frete"),
    }

    
