from flask import Flask, jsonify
from services import extract_service, kafka_service, mongo_service
from datetime import datetime

app = Flask(__name__)
    
@app.route('/numbl/<string:num_bl>', methods=['GET'])
def get_number_bl(num_bl):

    try:
        saveds = list(mongo_service.get_record_by_num_bl(num_bl))
        max_attempts = 1
        
        if(saveds is not None and len(saveds) > 0):
            for saved in saveds:
                print('Verificou no banco e viu que ja houve uma tentiva de leitura sem sucesso')
                max_attempts = saved['max_attempts'] + 1
        
        content_files = extract_service.get_content_files()

            
        list_json = extract_service.extract_num_bl_and_ce_mercante("Número BL do Conhecimento de Embarque Original :", "No. CE-MERCANTE Master vinculado :", content_files, num_bl)
        
        if(list_json is not None and len(list_json) > 0 and not list_json[0]['not_num_bl']):
            print('Leu BL: '+num_bl+' nos arquivos')
            for json in list_json:
                if(not json['value']):
                    
                    kafka_service.send_producer({'num_bl': num_bl, 'status': 'unprocessed', 'message': 'BL não contem CE Mercante ainda', 'date_request': datetime.now().isoformat()})  
                    mongo_service.save_record({'num_bl': num_bl, 'status': 'unprocessed', 'message': 'BL não contem CE Mercante ainda', 
                                        'date_request': datetime.now().isoformat(), 'max_attempts': max_attempts})
                    
                    return jsonify({'message': 'Requisição foi recebida e salva com sucesso!'}), 200
            
                else:
                    print('Leu CE Mercante: '+json['value']+' nos arquivos')

                    print('Lendo dados nos arquivos')

                    data = extract_service.load_json_file(json['file'])

                    print('Dados lidos com sucesso')
                    
                    mongo_service.save_data(data)

                    kafka_service.send_producer({'num_bl': num_bl, 'status': 'processed'})   

                    print('----------------------------------------------')
            
        else:
            print('Não leu BL nos arquivos')
            kafka_service.send_producer({'num_bl': num_bl, 'status': 'unprocessed', 'message': 'BL não foi encontrado ', 'date_request': datetime.now().isoformat()})    
            mongo_service.save_record({'num_bl': num_bl, 'status': 'unprocessed', 'message': 'BL não foi encontrado ', 
                                'date_request': datetime.now().isoformat(), 'max_attempts': max_attempts})
            
        return jsonify({'message': 'Requisição foi recebida e salva com sucesso!'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Inicializa o servidor
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
