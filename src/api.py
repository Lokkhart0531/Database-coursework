from typing import Any, Dict, List

import requests


class APIManager:
    BASE_URL = "https://api.hh.ru"

    @staticmethod
    def get_companies(ids: List[int]) -> List[Dict[str, Any]]:
        """метод получает информацию о компаниях по их идентификаторам."""
        companies = []
        for company_id in ids:
            response = requests.get(f"{APIManager.BASE_URL}/employers/{company_id}")
            if response.status_code == 200:
                companies.append(response.json())
            else:
                print(
                    f"Ошибка при получении компании с ID {company_id}: {response.status_code}"
                )
        return companies

    @staticmethod
    def get_vacancies(company_id: int) -> List[Dict[str, Any]]:
        """метод получает список вакансий для заданной компании."""
        response = requests.get(
            f"{APIManager.BASE_URL}/vacancies?employer_id={company_id}"
        )
        if response.status_code == 200:
            return response.json().get("items", [])
        else:
            print(
                f"Ошибка при получении вакансий для компании с ID {company_id}: {response.status_code}"
            )
            return []
