import unittest
from src.scripts.calculadora.calculadora import calculadora_adicao

class TestCalculadoraAdicao(unittest.TestCase):

    def test_calculadoraAdicao_SomaDeDoisNumerosPositivos_Expect_SomaCorreta(self):
        self.assertEqual(calculadora_adicao(1, 2), 3)

    def test_calculadoraAdicao_SomaDeDoisNumerosNegativos_Expect_SomaCorreta(self):
        self.assertEqual(calculadora_adicao(-1, -1), -2)

    def test_calculadoraAdicao_SomaDeUmNumeroPositivoEUmNegativo_Expect_SomaCorreta(self):
        self.assertEqual(calculadora_adicao(1, -1), 0)

    def test_calculadoraAdicao_SomaDeZeroEUmNumero_Expect_SomaCorreta(self):
        self.assertEqual(calculadora_adicao(0, 5), 5)

    def test_calculadoraAdicao_SomaDeNumeroEMaxInt_Expect_SomaCorreta(self):
        self.assertEqual(calculadora_adicao(1, 2 ** 31 - 1), 2 ** 31)

    def test_calculadoraAdicao_SomaDeNumeroEMinInt_Expect_SomaCorreta(self):
        self.assertEqual(calculadora_adicao(-1, -2 ** 31), -2 ** 31 - 1)


if __name__ == '__main__':
    unittest.main()
