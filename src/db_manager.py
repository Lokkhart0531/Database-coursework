import logging
from typing import Any, Optional

import psycopg2
from psycopg2 import sql


class DBManager:
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.cursor = None

    def connect(self):
        """Создает соединение с базой данных и курсор."""
        try:
            self.connection = psycopg2.connect(
                dbname=self.config["dbname"],
                user=self.config["user"],
                password=self.config["password"],
                host=self.config["host"],
                port=self.config["port"],
            )
            self.cursor = self.connection.cursor()
            logging.info("Соединение с базой данных установлено.")
        except Exception as e:
            logging.error(f"Ошибка при подключении к базе данных: {e}")
            raise

    def create_database(self):
        """Создает базу данных, если она не существует."""
        try:
            conn = psycopg2.connect(
                dbname="postgres",
                user=self.config["user"],
                password=self.config["password"],
                host=self.config["host"],
                port=self.config["port"],
            )
            conn.autocommit = True
            cursor = conn.cursor()

            cursor.execute(
                "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",
                (self.config["dbname"],),
            )
            exists = cursor.fetchone()

            if not exists:
                cursor.execute(
                    sql.SQL("CREATE DATABASE {}").format(
                        sql.Identifier(self.config["dbname"])
                    )
                )
                logging.info(f"База данных '{self.config['dbname']}' успешно создана.")
            else:
                logging.info(f"База данных '{self.config['dbname']}' уже существует.")

        except Exception as e:
            logging.error(f"Ошибка при создании базы данных: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def create_tables(self) -> None:
        """Создает таблицы employers и vacancies в базе данных, если они не существуют."""
        create_employers_table = """
        CREATE TABLE IF NOT EXISTS employers (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            vacancies_count INTEGER DEFAULT 0
        );
        """

        create_vacancies_table = """
        CREATE TABLE IF NOT EXISTS vacancies (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            salary_min INTEGER,
            salary_max INTEGER,
            employer_id INTEGER REFERENCES employers(id)
        );
        """

        if self.cursor is not None:
            try:
                self.cursor.execute(create_employers_table)
                self.cursor.execute(create_vacancies_table)
                self.connection.commit()
                logging.info("Таблицы успешно созданы.")
            except Exception as e:
                logging.error(f"Ошибка при создании таблиц: {e}")
                self.connection.rollback()
        else:
            logging.error(
                "Курсор не инициализирован. Проверьте соединение с базой данных."
            )

    def close(self):
        """Закрывает соединение с базой данных."""
        if self.cursor is not None:
            self.cursor.close()

        if self.connection is not None:
            self.connection.close()

    def insert_employer(self, name: str) -> Optional[int]:
        """Метод вставляет нового работодателя в таблицу employers."""
        if not self.cursor or not self.connection:
            logging.error("Курсор или соединение не инициализированы.")
            return None

        try:
            self.cursor.execute(
                "INSERT INTO employers (name) VALUES (%s) RETURNING id;", (name,)
            )
            employer_id = self.cursor.fetchone()[0]
            self.connection.commit()
            logging.info(f"Работодатель '{name}' успешно добавлен с ID {employer_id}.")
            return employer_id
        except Exception as e:
            logging.error(f"Ошибка при вставке работодателя: {e}")
            self.connection.rollback()

    def insert_vacancy(
        self,
        name: str,
        salary_min: Optional[int],
        salary_max: Optional[int],
        employer_id: int,
    ) -> None:
        """Метод вставляет новую вакансию в таблицу vacancies."""

        if not self.cursor or not self.connection:
            logging.error("Курсор или соединение не инициализированы.")
            return

        try:
            self.cursor.execute(
                "INSERT INTO vacancies (name, salary_min, salary_max, employer_id) VALUES (%s, %s, %s, %s);",
                (name, salary_min, salary_max, employer_id),
            )
            self.connection.commit()
            logging.info(
                f"Вакансия '{name}' успешно добавлена для работодателя ID {employer_id}."
            )
        except Exception as e:
            logging.error(f"Ошибка при вставке вакансии: {e}")
            self.connection.rollback()

    def get_companies_and_vacancies_count(self):
        """Метод получает список компаний и количество их вакансий."""

        query = """
        SELECT e.name, COUNT(v.id) AS vacancies_count 
        FROM employers e 
        LEFT JOIN vacancies v ON e.id = v.employer_id 
        GROUP BY e.id;
        """

        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            logging.error(f"Ошибка при получении компаний и количества вакансий: {e}")

    def get_all_vacancies(self):
        """Метод получает все вакансии из базы данных."""

        query = """
         SELECT e.name AS company_name, v.name AS vacancy_title, v.salary_min AS min_salary,
               v.salary_max AS max_salary 
               FROM vacancies v 
               JOIN employers e ON v.employer_id = e.id;
               """

        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            logging.error(f"Ошибка при получении всех вакансий: {e}")

    def get_avg_salary(self) -> Optional[float]:
        """Метод получает среднюю зарплату по всем вакансиям."""

        query = "SELECT AVG((salary_min + salary_max) / 2.0) FROM vacancies;"
        try:
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            logging.error(f"Ошибка при получении средней зарплаты: {e}")

    def get_vacancies_with_higher_salary(self):
        """Метод получает все вакансии с зарплатой выше средней"""

        avg_salary = self.get_avg_salary()
        if avg_salary is None:
            return []

        query = """
           SELECT * FROM vacancies WHERE (salary_min + salary_max) / 2.0 > %s;
           """
        try:
            self.cursor.execute(query, (avg_salary,))
            return self.cursor.fetchall()
        except Exception as e:
            logging.error(
                f"Ошибка при получении вакансий с зарплатой выше средней: {e}"
            )

    def get_vacancies_with_keyword(self, keyword: str) -> Any:
        """Метод получает все вакансии по ключевому слову"""

        query = "SELECT * FROM vacancies WHERE name ILIKE %s;"
        try:
            self.cursor.execute(query, ("%" + keyword + "%",))
            return self.cursor.fetchall()
        except Exception as e:
            logging.error(
                f"Ошибка при получении вакансий по ключевому слову '{keyword}': {e}"
            )
