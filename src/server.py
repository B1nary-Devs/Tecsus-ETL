from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
import os
from .main import main
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite todas as origens
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos os métodos
    allow_headers=["*"],  # Permite todos os cabeçalhos
)

@app.get("/")
async def read_item():
    return {'message': 'Estou no ar'}

# Certifique-se de que o diretório existe
os.makedirs('../data/raw', exist_ok=True)


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        file_location = f"../data/raw/{file.filename}"  # Caminho onde o arquivo será salvo
        # Assegura que a pasta existe
        os.makedirs(os.path.dirname(file_location), exist_ok=True)

        with open(file_location, "wb+") as file_object:
            file_object.write(await file.read())

        # Chamada da função main após o arquivo ser salvo
        folder_path = '../data/raw'
        main(folder_path)

        return JSONResponse(status_code=200, content={"message": "Arquivo recebido e salvo!"})
    except Exception as e:
        # Captura qualquer exceção que ocorra durante o processo de upload e salva do arquivo
        return JSONResponse(status_code=500, content={"message": "Falha ao processar o arquivo", "details": str(e)})
