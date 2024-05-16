import uuid
import pickle
import requests
import asyncio
import multiprocessing
import json
import base64
import hashlib
import aiohttp
import codecs
from aiohttp.client import ClientTimeout
import random
from collections import defaultdict
import os
import shutil
import aiofiles
from getAudioFromVideo import convert_video_to_audio_ffmpeg

async def get_file(url, datos):
    cookies={'__adblocker':'true'}
    headers={"alt-svc":'h3=":443"; ma=86400, h3-29=":443"; ma=86400'
    ,'origin': 'https://portal-pautas.ine.mx/'
    ,'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
    ,'sec-ch-ua': '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"'
    ,"sec-fetch-site": "same-origin",
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'accept': '*/*',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'x-requested-with': 'XMLHttpRequest',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"'
    ,'referer': 'https://portal-pautas.ine.mx/'}
    respuesta=''
    timeout = ClientTimeout(total=160000)  # `0` value to disable timeout
    async with aiohttp.ClientSession(headers=headers,timeout=timeout,cookies=cookies) as session:
        async with session.post(url,data=datos) as resp:
            await resp.read()
            
    ##f = open(datos["f_p"],"w+")
    ##f.writelines(resultado)
    ##f.close()
async def getSpot(spot):
    cookies={'__adblocker':'true'}
    headers={"alt-svc":'h3=":443"; ma=86400, h3-29=":443"; ma=86400'
    ,'origin': 'https://portal-pautas.ine.mx/'
    ,'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
    ,'sec-ch-ua': '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"'
    ,"sec-fetch-site": "same-origin",
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'accept': '*/*',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'x-requested-with': 'XMLHttpRequest',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"'
    ,'referer': 'https://portal-pautas.ine.mx/'}
    timeout = ClientTimeout(total=200000)  # `0` value to disable timeout
    async with aiohttp.ClientSession(headers=headers,timeout=timeout,cookies=cookies) as session:
        async with session.get(spot["folio"]) as resp:
            if not os.path.exists(spot['directorio']):
                os.makedirs(spot["directorio"])
            nombre=spot["folio"][spot['folio'].rfind('/')+1:]
            async with aiofiles.open(f'{spot["directorio"]}/{nombre}', "wb") as f:
                chunk_size = 4096
                async for data in resp.content.iter_chunked(chunk_size):
                    await f.write(data)
    return 

def handlerGetSpot(workload):
    
    loop_async=asyncio.new_event_loop()
    loop_async.run_until_complete(asyncio.wait([loop_async.create_task(getSpot(x)) for x in workload]))
    loop_async.close()


def getWorkload(periodo,eleccion,index,actoresIgnorados):
    workload=[]
    catalogo=requests.get(f'https://portal-pautas.ine.mx/pautas5/portalPublico/promos_{eleccion}/{periodo}/tele_radio.json')
    catalogo=json.loads(catalogo.content)
    for actor in catalogo:
        tipo=str(list(actor.keys())[0])
        auxSplit=tipo.rfind('_')
        medio=tipo[auxSplit+1:]
        auxData=[]
        nombreActor=tipo[:auxSplit]
        if nombreActor in actoresIgnorados:
            continue
        for partido in actor[tipo]:

            for i in range(len(partido['version'])):
                if partido['folio'][i]['folio'] in index:
                    index['folio']=partido["folio"][i]["cautelar"]
                index[partido['folio'][i]['folio']]=partido["folio"][i]["cautelar"]
                partido["folio"][i]["directorio"]=f'{medio}/{partido["actor_politico"]}'
                partido["folio"][i]["partido"]=partido['actor_politico']
                partido["folio"][i]["nombre"]=partido["version"][i]
                workload.append(partido["folio"][i])
    return workload

def imprimeProgreso(fi):
    avance=actual['']+peticiones_concurrentes
    print (avance)
    actual['']+=peticiones_concurrentes

def convertirVideos():
    pool = multiprocessing.Pool(processes=15)
    base="tv"
    dires=0
    archi=0
    videos=[]
    if (not os.path.exists('tv')):
        return
    for x in os.listdir("tv"):
        directorio=f"{base}/{x}"
        for video in os.listdir(directorio) :
            if video.endswith('.mp4'):
                 videos.append(f'{directorio}/{video}')
    i=0
    while i<len(videos):
        limite=i+peticiones_concurrentes if i+peticiones_concurrentes<len(videos) else len(videos)
        pool.map_async(convert_video_to_audio_ffmpeg,[videos[i:limite]])
        print (f'{i} a {limite}')
        i+=peticiones_concurrentes
    pool.close()
    pool.join()
    shutil.rmtree('tv')

if __name__=='__main__':
    total=0
    actual={'':0}
    pool = multiprocessing.Pool(processes=6)
    peticiones_concurrentes=4
    worker_id=0
    workload=[]
    index={}
    if os.path.exists('index.pkl'):
        file=open('index.pkl','rb+')
        index=pickle.load(file)
        file.close()
    actoresIgnorados=[]
    workload = getWorkload('campania','federales',index=index,actoresIgnorados=actoresIgnorados)
    print (len(workload))
    with open('index.pkl','wb+') as file:
        pickle.dump(index,file)
    trans={}
    if os.path.exists('transLimpias.pkl'):
        file=open('transLimpias.pkl','rb+')
        trans=pickle.load(file)
        file.close()
    convertidas=[]
    if os.path.exists('data.json'):
        f = open('data.json')
        convertidas = json.load(f)
        f.close()
    for x in workload:
        if x["titulo"] in trans:
            x["transcripcion"]=trans[x["titulo"]]
            convertidas.append(x)
    with open("data.json", "w") as final:
        json.dump(convertidas, final) 
    aux_workload=[]
    i=0
    while i<len(workload):
        limite=i+peticiones_concurrentes if i+peticiones_concurrentes<len(workload) else len(workload)
        pool.map_async(handlerGetSpot,[workload[i:limite]],callback=imprimeProgreso)
        print (f'{i} a {limite}')
        i+=peticiones_concurrentes
    pool.close()
    pool.join()
    convertirVideos()


