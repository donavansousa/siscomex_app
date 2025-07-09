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

def extract_num_bl(label, content_files):
    for file in content_files:
        tag = file.find("dt", string=lambda text: text and label in text)
        if tag:
            return tag.find_next("dd").get_text(strip=True)
    return None

def extract_ce_mercante(label, content_files):
    for file in content_files:
        tag = tag = file.find("dt", string=lambda text: text and label in text)
        if tag:
            value = tag.find_next("a").get_text(strip=True)
            if(value):
                return { 'file': file, 'value': value}
    return None

def extract_numero_manifesto(file):
        link = file.find("a", href=lambda h: h and "nrManifesto=" in h)
        if link:
            href = link.get("href", "")
            match = re.search(r"nrManifesto=(\d+)", href)
            if match:
                return match.group(1)
        return None

def extract_numero_despacho(file):
        link = file.find("a", href=re.compile(r"HelpConsultarDetalhesDI"))
        if link:
            match = re.search(r"'(\d+)'", link.get("href", ""))
            if match:
                return match.group(1)
        return None

def extract_situacao_mercadoria(file):
        h2 = file.find("h2", string=lambda t: t and "Mercadoria" in t)
        if h2:
            dl = h2.find_next("dl")
            if dl:
                situacao_dt = dl.find("dt", string=lambda t: t and "Situação" in t)
                if situacao_dt:
                    dd = situacao_dt.find_next("dd")
                    if dd:
                        return dd.get_text(strip=True)
        return None


def extract_portos_manifesto(file):
        h2 = file.find("h2", string=lambda t: t and "Manifesto/Conhecimento" in t)
        if h2:
            table = h2.find_next("table")
            if table:
                rows = table.find_all("tr")
                if len(rows) > 1:
                    cols = rows[1].find_all("td")
                    if len(cols) >= 3:
                        origem = re.sub(r"\s+", " ", cols[1].get_text()).strip()
                        destino = re.sub(r"\s+", " ", cols[2].get_text()).strip()
                        return {
                            "porto_carregamento": origem,
                            "porto_descarregamento": destino
                        }
        return None

def extract_section_fields(section_title, file):
        h2 = file.find("h2", string=lambda t: t and section_title in t)
        if h2:
            dl = h2.find_next("dl")
            if dl:
                resultado = {}
                for dt in dl.find_all("dt"):
                    label = dt.get_text(strip=True)
                    dd = dt.find_next("dd")
                    value = dd.get_text(strip=True) if dd else None
                    resultado[label] = value
                return resultado
        return None

import re

def extract_componentes_frete(file):
        h2 = file.find("h2", string=lambda t: t and "Frete e Despesas de Transporte" in t)
        if h2:
            dl = h2.find_next("dl")
            if dl:
                dt = dl.find("dt", string=lambda t: t and "Componentes do Frete" in t)
                if dt:
                    dd = dt.find_next("dd")
                    table = dd.find("table") if dd else None
                    if table:
                        componentes = []
                        rows = table.find_all("tr")[1:]  # ignora cabeçalho
                        for row in rows:
                            cols = row.find_all("td")
                            if len(cols) == 4:
                                componentes.append({
                                    "componente": re.sub(r"\s+", " ", cols[0].get_text()).strip(),
                                    "moeda": re.sub(r"\s+", " ", cols[1].get_text()).strip(),
                                    "valor": re.sub(r"\s+", " ", cols[2].get_text()).strip(),
                                    "recolhimento": re.sub(r"\s+", " ", cols[3].get_text()).strip()
                                })
                        return componentes
        return None

def load_json_file(content_file_selected):
    numero_manifesto = extract_numero_manifesto(content_file_selected)
    portos = extract_portos_manifesto(content_file_selected)
    manifesto_dl = extract_section_fields("Manifesto/Conhecimento", content_file_selected)
    embarcador = extract_section_fields("Embarcador", content_file_selected)
    consignatario = extract_section_fields("Consignatário", content_file_selected)
    notificado = extract_section_fields("Parte a ser Notificada", content_file_selected)
    transportador = extract_section_fields("Transportador ou representante:", content_file_selected)
    procedencia = extract_section_fields("Procedência e Destino da Carga", content_file_selected)
    mercadoria = extract_section_fields("Mercadoria", content_file_selected)
    frete = extract_section_fields("Frete e Despesas de Transporte", content_file_selected)

    dados_carga = {
        "numero_manifesto": numero_manifesto,
        "porto_carregamento": portos.get("porto_carregamento"),
        "porto_descarregamento": portos.get("porto_descarregamento"),

        "tipo_conhecimento": manifesto_dl.get("Tipo de Conhecimento :"),
        "bl_servico": manifesto_dl.get("BL de Serviço :"),
        "numero_ce_mercante": manifesto_dl.get("No. CE-MERCANTE Master vinculado :"),
        "data_emissao_bl": manifesto_dl.get("Data de Emissão do BL :"),
        "dados_complementares_embarcador": embarcador.get("Dados Complementares do Embarcador :"),

        "bl_a_ordem": consignatario.get("BL a Ordem :"),
        "cnpj_consignatario": consignatario.get("CNPJ/CPF :"),
        "razao_social_consignatario": consignatario.get("Razão Social/Nome :"),
        "dados_complementares_consignatario": consignatario.get("Dados Complementares do Consignatário :"),

        "cnpj_notificado": notificado.get("CNPJ/CPF :"),
        "razao_notificado": notificado.get("Razão Social/Nome :"),
        "dados_complementares_notificado": notificado.get("Dados Complementares :"),

        "cnpj_transportador": transportador.get("CNPJ :"),
        "razao_transportador": transportador.get("Razão Social :"),

        "numero_bl_original": procedencia.get("Número BL do Conhecimento de Embarque Original :"),
        "porto_origem": procedencia.get("Porto de Origem :"),
        "pais_procedencia": procedencia.get("País de Procedência :"),
        "uf_destino": procedencia.get("UF de Destino Final :"),
        "porto_destino": procedencia.get("Porto de Destino Final :"),
        "numero_documento_despacho": extract_numero_despacho(content_file_selected),

        "descricao_mercadoria_1": mercadoria.get("Descrição 1 :"),
        "descricao_mercadoria_2": mercadoria.get("Descrição 2 :"),
        "peso_bruto_kg": mercadoria.get("Peso Bruto da Carga (Kg) :"),
        "cubagem_m3": mercadoria.get("Cubagem (em m3) :"),
        "categoria_mercadoria": mercadoria.get("Categoria :"),
        "situacao_mercadoria": extract_situacao_mercadoria(content_file_selected),

        "recolhimento_frete": frete.get("Recolhimento de Frete :"),
        "modalidade_frete": frete.get("Modalidade de Frete :"),
        "moeda_frete": frete.get("Moeda do Frete :"),
        "valor_frete": frete.get("Valor do Frete Básico :"),
        "componentes_frete": extract_componentes_frete(content_file_selected),

    }

    return dados_carga

    
