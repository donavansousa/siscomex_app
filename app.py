from flask import Flask, jsonify
from services import extract_service, kafka_service, mongo_service
from datetime import datetime
import time

app = Flask(__name__)
    
@app.route('/numbl/<string:num_bl>', methods=['GET'])
def get_number_bl(num_bl):

    try:
        saved = mongo_service.get_by_num_bl(num_bl)
        max_attempts = 1
        if(saved):
            max_attempts = saved['max_attempts'] + 1

        content_files = extract_service.get_content_files()

        textNumbl = extract_service.extract_text("Número BL do Conhecimento de Embarque Original :", content_files)

        if textNumbl == num_bl:
            print('Leu BL no arquivos')
            
            textCEMercante = extract_service.extract_link("No. CE-MERCANTE Master vinculado :", content_files)

            if(not textCEMercante):
                print('Não leu CE Mercante nos arquivos')
                kafka_service.send_producer({'num_bl': num_bl, 'status': 'unprocessed', 'message': 'BL não contem CE Mercante ainda', 'date_request': datetime.now().isoformat()})  
                mongo_service.save({'num_bl': num_bl, 'status': 'unprocessed', 'message': 'BL não contem CE Mercante ainda', 
                                    'date_request': datetime.now().isoformat(), 'max_attempts': max_attempts})
                
                return jsonify({'message': 'Requisição foi recebida e salva com sucesso!'}), 200
        
            print('Leu CE Mercante nos arquivos')

            kafka_service.send_producer({'num_bl': num_bl, 'status': 'processed'})   

            
        else:
            print('Não leu BL nos arquivos')
            kafka_service.send_producer({'num_bl': num_bl, 'status': 'unprocessed', 'message': 'BL não foi encontrado ', 'date_request': datetime.now().isoformat()})    
            mongo_service.save({'num_bl': num_bl, 'status': 'unprocessed', 'message': 'BL não foi encontrado ', 
                                'date_request': datetime.now().isoformat(), 'max_attempts': max_attempts})
            
        return jsonify({'message': 'Requisição foi recebida e salva com sucesso!'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Inicializa o servidor
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
