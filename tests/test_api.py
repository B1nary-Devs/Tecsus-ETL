import unittest
from unittest.mock import patch
from fastapi.testclient import TestClient
from src.server import app

class TestUploadFile(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    @patch("src.server.main")
    def test_upload_file(self, mock_main):
        # Simula um arquivo para upload
        file_content = b"file content"
        file_name = "testfile.txt"

        response = self.client.post(
            "/upload",
            files={"file": (file_name, file_content, "text/plain")}
        )

        # Verifica se o status da resposta é 200
        self.assertEqual(response.status_code, 200)
        # Verifica se a mensagem de sucesso está correta
        self.assertEqual(response.json(), {"message": "Arquivo recebido e salvo!"})
        # Verifica se a função main foi chamada uma vez
        mock_main.assert_called_once_with('../data/raw')

    @patch("builtins.open", side_effect=Exception("Test error"))
    def test_upload_file_failure(self, mock_open):
        # Simula um arquivo para upload
        file_content = b"file content"
        file_name = "testfile.txt"

        response = self.client.post(
            "/upload",
            files={"file": (file_name, file_content, "text/plain")}
        )

        # Verifica se o status da resposta é 500
        self.assertEqual(response.status_code, 500)
        # Verifica se a mensagem de erro está correta
        self.assertEqual(response.json()["message"], "Falha ao processar o arquivo")
        self.assertEqual(response.json()["details"], "Test error")

if __name__ == '__main__':
    unittest.main()