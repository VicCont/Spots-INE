import multiprocessing
import os
import glob
import subprocess
import pathlib
import ffmpeg

def convert_video_to_audio_ffmpeg(video_file):
    """Converts video to audio directly using `ffmpeg` command
    with the help of subprocess module"""
    filename, ext = os.path.splitext(video_file)
    dir=filename[filename.find('/')+1:filename.rfind('/')]
    if not os.path.exists(f'audios/{dir}'):
                    os.makedirs(f'audios/{dir}')
    filename=filename[filename.rfind("/")+1:]
    (ffmpeg
     .input(video_file)
     .output(f"audios/{dir}/{filename}.mp3", loglevel="quiet")
     .run(overwrite_output=True)
     )


if __name__ == "__main__":
    pool = multiprocessing.Pool(processes=15)
    base="tv"
    dires=0
    archi=0
    workLoad=[]
    for x in os.listdir("tv"):
        directorio=f"{base}/{x}"
        for video in os.listdir(directorio) :
            if video.endswith('.mp4'):
                 workLoad.append(f'{directorio}/{video}')
    pool.map_async(convert_video_to_audio_ffmpeg,workLoad)
    pool.close()
    pool.join()
                