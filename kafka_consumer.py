import asyncio
import json
from kafka import KafkaConsumer
import httpx
from threading import Thread
from services import mongo_service
import time

semaphore = asyncio.Semaphore(5)
pending_tasks = set()

async def reprocess_get_number_bl(num_bl):
    async with semaphore:
        print(f'[⏳] Iniciando reprocessamento do BL {num_bl}')
        saved = mongo_service.get_by_num_bl(num_bl)

        if int(saved['max_attempts']) <= 10:
            print(f'Aguardando 2 horas para o reprocessamento do BL {num_bl}')
            await asyncio.sleep(7200)

            url = f"http://127.0.0.1:5000/numbl/{num_bl}"

            async with httpx.AsyncClient() as client:
                response = await client.get(url)

                if response.status_code != 200:
                    print(f"❌ Erro ao reprocessar o BL {num_bl}")
                else:
                    print(f"✅ Reprocessado o BL {num_bl}")
        else:
            print(f"⚠️ Número máximo de tentativas foi atingido no BL {num_bl}")

def on_task_done(task):
    try:
        task.result()
    except Exception as e:
        print(f'[ERRO] Tarefa lançou exceção: {e}')
    finally:
        pending_tasks.discard(task)

def kafka_consumer_thread(loop):
    consumer = KafkaConsumer(
        "queue",
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("[Kafka] Consumidor iniciado, aguardando mensagens...")
    time.sleep(2)
    for msg in consumer:
        print(f"[Kafka] Mensagem recebida: {json.dumps(msg.value, indent=2, ensure_ascii=False)}")
        if msg.value.get('status') == 'unprocessed':
            num_bl = msg.value.get('num_bl')
            if not num_bl:
                print("[ERRO] Campo 'num_bl' ausente na mensagem")
                continue

            # Agenda a coroutine no loop asyncio (thread-safe)
            task = asyncio.run_coroutine_threadsafe(reprocess_get_number_bl(num_bl), loop)
            pending_tasks.add(task)
            task.add_done_callback(on_task_done)
        else:
            print("[Kafka] Mensagem ignorada por status diferente de 'unprocessed'")

def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Inicia o consumidor Kafka em thread separada
    thread = Thread(target=kafka_consumer_thread, args=(loop,), daemon=True)
    thread.start()

    print("[Sistema] Loop de eventos iniciado.")
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        if pending_tasks:
            print(f"[Sistema] Aguardando {len(pending_tasks)} tarefas pendentes...")
            loop.run_until_complete(asyncio.gather(*pending_tasks))
    finally:
        loop.stop()
        loop.close()
        print("[Sistema] Finalizado.")

if __name__ == "__main__":
    main()