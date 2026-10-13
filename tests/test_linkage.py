import unittest

from src.linkage import (
    STATUS_AMBIGUOUS,
    STATUS_FOUND,
    STATUS_NOT_FOUND,
    build_search_strategies,
    decide_match,
    rank_candidates,
    score_candidate,
)


class LinkageTest(unittest.TestCase):
    def setUp(self):
        self.query = {
            "municipio_normalizado": "290070",
            "logradouro_normalizado": "rodovia br 101",
            "numero_normalizado": "sn",
            "cep_normalizado": "48030260",
            "bairro_normalizado": "br101",
            "complemento_normalizado": "sn",
        }
        self.candidate = {
            "municipio_normalizado": "290070",
            "logradouro_normalizado": "rodovia br 101",
            "numero_normalizado": "sn",
            "cep_normalizado": "48030260",
            "localidade_normalizada": "br101",
            "complemento_normalizado": "sn",
            "setor_censitario": "290070205000001P",
        }

    def test_builds_exact_and_fuzzy_strategies(self):
        methods = [name for name, _ in build_search_strategies(self.query)]
        self.assertEqual(methods[0], "EXATO_CEP")
        self.assertIn("EXATO_LOGRADOURO_NUMERO", methods)
        self.assertIn("CEP_NUMERO", methods)
        self.assertIn("CEP_LOGRADOURO_APROXIMADO", methods)
        self.assertEqual(methods[-1], "APROXIMADO")

    def test_perfect_candidate_scores_one_hundred(self):
        score, evidence = score_candidate(self.query, self.candidate)
        self.assertEqual(score, 100.0)
        self.assertTrue(evidence["municipio_igual"])

    def test_different_municipality_scores_zero(self):
        candidate = dict(self.candidate, municipio_normalizado="999999")
        score, evidence = score_candidate(self.query, candidate)
        self.assertEqual(score, 0.0)
        self.assertFalse(evidence["municipio_igual"])

    def test_decision_found_ambiguous_and_not_found(self):
        best = {
            "id_endereco": "1",
            "score_vinculacao": 100.0,
            "elasticsearch_score": 2.0,
            "source": self.candidate,
        }
        second = {
            "id_endereco": "2",
            "score_vinculacao": 70.0,
            "elasticsearch_score": 1.0,
            "source": dict(self.candidate, setor_censitario="outro"),
        }
        decision = decide_match([best, second], 80.0, 8.0)
        self.assertEqual(decision["status"], STATUS_FOUND)
        self.assertEqual(decision["setor_censitario"], "290070205000001P")

        close = dict(second, score_vinculacao=95.0)
        decision = decide_match([best, close], 80.0, 8.0)
        self.assertEqual(decision["status"], STATUS_AMBIGUOUS)
        self.assertIsNone(decision["setor_censitario"])

        decision = decide_match([], 80.0, 8.0)
        self.assertEqual(decision["status"], STATUS_NOT_FOUND)

    def test_ranking_uses_linkage_score(self):
        weak = dict(self.candidate, numero_normalizado="999")
        hits = [
            {"_id": "weak", "_score": 9.0, "_source": weak},
            {"_id": "strong", "_score": 1.0, "_source": self.candidate},
        ]
        ranked = rank_candidates(self.query, hits)
        self.assertEqual(ranked[0]["id_endereco"], "strong")


if __name__ == "__main__":
    unittest.main()
