import unittest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from src import main
import os


class TestUploadFile(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(main.app)

    def test_upload_file(self):
        test_file_content = b"Hello world"
        test_file_name = 'test.txt'
        files = {
            'file': (test_file_name, test_file_content)
        }

        # Usando patch para simular a função main e evitar efeitos colaterais
        with patch('sua_app.main', return_value=None) as mock_main:
            response = self.client.post("/upload", files=files)

            # Verificar se a função main foi chamada
            mock_main.assert_called_once()

            # Verificar a resposta
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json(), {"message": "Arquivo recebido e salvo!"})

            # Verificar se o arquivo foi salvo
            file_location = f"../data/raw/{test_file_name}"
            self.assertTrue(os.path.isfile(file_location))

            # Limpar: remover o arquivo após o teste
            os.remove(file_location)


if __name__ == '__main__':
    unittest.main()
