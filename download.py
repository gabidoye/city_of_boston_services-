import os
os.environ['KAGGLE_USERNAME'] = "gabidoye"
os.environ['KAGGLE_KEY'] = "191dc7d370b1678b9ebae6a6dc212380"
path = 'dataset'

from kaggle.api.kaggle_api_extended import Kaggle,KaggleApi
api = KaggleApi()
api.authenticate()
api.dataset_download_files('stefanoleone992/fifa-22-complete-player-dataset', path ,unzip=True)