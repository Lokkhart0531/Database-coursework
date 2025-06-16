import logging

from src.api import APIManager
from src.db_manager import DBManager

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Конфигурация базы данных
config = {
    "dbname": "cw-5",
    "user": "postgres",
    "password": "2431",
    "host": "localhost",
    "port": 5432,
}

company_ids = [
    15478,
    80,
    2324020,
    1740,
    9694561,
    1180205,
    1122462,
    8121,
    67611,
    123422
]
db_manager = DBManager(config)
db_manager.create_database()

try:
    db_manager.connect()

    # Создание таблиц
    db_manager.create_tables()
    vacancies_exist = db_manager.get_all_vacancies()

    if not vacancies_exist:
        print("В базе данных нет вакансий. Загружаем данные из API")

        found_companies = APIManager.get_companies(company_ids)

        if found_companies:
            for company in found_companies:
                employer_name = company.get("name")
                employer_id = db_manager.insert_employer(employer_name)

                if employer_id:
                    vacancies = APIManager.get_vacancies(company["id"])

                    for vacancy in vacancies:
                        vacancy_name = vacancy.get("name")
                        salary = vacancy.get("salary", {})
                        salary_min = salary.get("from") if salary else None
                        salary_max = salary.get("to") if salary else None

                        db_manager.insert_vacancy(
                            name=vacancy_name,
                            salary_min=salary_min,
                            salary_max=salary_max,
                            employer_id=employer_id,
                        )
    while True:
        print("1. Показать компании и количество вакансий")
        print("2. Показать среднюю зарплату")
        print("3. Показать вакансии по ключевому слову")
        print("4. Показать вакансии с зарплатой выше средней")
        print("5. Показать все вакансии")
        option = input("Выберите опцию (или 'exit' для выхода): ")

        if option == "1":
            companies = db_manager.get_companies_and_vacancies_count()
            for company in companies:
                print(f"Компания: {company[0]}, Количество вакансий: {company[1]}")

        elif option == "2":
            avg_salary = db_manager.get_avg_salary()
            print(f"Средняя зарплата: {avg_salary}")

        elif option == "3":
            keyword = input("Введите ключевое слово: ")
            vacancies = db_manager.get_vacancies_with_keyword(keyword)
            if vacancies:
                for vacancy in vacancies:
                    title = vacancy[1]
                    salary_min = vacancy[2]
                    salary_max = vacancy[3]
                    print(
                        f"Вакансия: {title}, Минимальная зарплата: {salary_min}, Максимальная зарплата: {salary_max}"
                    )
            else:
                print("Вакансии не найдены.")

        elif option == "4":
            high_salary_vacancies = db_manager.get_vacancies_with_higher_salary()
            if high_salary_vacancies:
                for vacancy in high_salary_vacancies:
                    title = vacancy[1]
                    salary_min = vacancy[2]
                    salary_max = vacancy[3]
                    print(
                        f"Вакансия: {title}, Минимальная зарплата: {salary_min}, Максимальная зарплата: {salary_max}"
                    )
            else:
                print("Вакансии с высокой зарплатой не найдены.")

        elif option == "5":
            all_vacancies = db_manager.get_all_vacancies()
            for vacancy in all_vacancies:
                print(vacancy)

        elif option.lower() == "exit":
            break

        else:
            print("Неверный выбор. Пожалуйста, попробуйте снова.")

finally:
    # Закрытие соединения с базой данных
    db_manager.close()
